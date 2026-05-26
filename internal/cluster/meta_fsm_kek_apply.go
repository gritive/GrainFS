package cluster

import (
	"bytes"
	"crypto/subtle"
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// ErrFSMKEKFatal indicates a KEK lifecycle Apply failure that is local to this
// node (disk I/O, content-match mismatch, in-memory mutation failure). Peers
// may have succeeded — continuing the apply loop would silently fork. The
// apply loop MUST halt on this sentinel.
var ErrFSMKEKFatal = errors.New("kek apply fatal: node-local failure during cluster-replicated mutation")

// fatalKEKApply wraps err with ErrFSMKEKFatal so callers can use
// errors.Is(err, ErrFSMKEKFatal) to classify the error as fatal.
// Returns nil when err is nil.
func fatalKEKApply(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrFSMKEKFatal, err)
}

// SetKEKStore wires the cluster-wide KEK store into the MetaFSM. Must be
// called before the apply loop replays MetaCmdTypeKEKRotate. nil disables
// rotation Apply (the command returns a non-fatal error so followers do not
// drop the log entry permanently).
func (f *MetaFSM) SetKEKStore(s *encrypt.KEKStore) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keystore = s
}

// KEKStore returns the registered KEK store, or nil if none is wired.
func (f *MetaFSM) KEKStore() *encrypt.KEKStore {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.keystore
}

// SetClusterID copies the 16-byte cluster identity into the FSM. Panics on
// wrong length — this is wired once at boot and a bug, not user input.
func (f *MetaFSM) SetClusterID(id []byte) {
	if len(id) != 16 {
		panic(fmt.Sprintf("MetaFSM.SetClusterID: id must be 16 bytes, got %d", len(id)))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	copy(f.clusterID[:], id)
}

// ClusterID returns a copy of the configured cluster identity. All-zero
// before SetClusterID is invoked.
func (f *MetaFSM) ClusterID() [16]byte {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.clusterID
}

// SetKEKDir configures the on-disk directory where KEK files live. Empty
// disables on-disk persistence (useful in unit tests). Set once at boot
// alongside SetKEKStore.
func (f *MetaFSM) SetKEKDir(dir string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kekDir = dir
}

// applyKEKRotate is the deterministic FSM apply of MetaCmdTypeKEKRotate.
// All re-seals happen on the leader; followers verify and install the bytes
// verbatim. The function maintains snapshot-atomic install: activeKEKVersion,
// the rewrapped DEK map (via dekKeeper.InstallKEKRotation), and the keystore
// active marker mutate together inside a single f.mu lock window so a
// concurrent snapshot writer cannot observe a torn state.
//
// Idempotent replay: when the same log entry is reapplied (snapshot tail
// replay after restart), Apply verifies that every piece of state — K_new
// on disk, FSM wrap[] entries, DEKKeeper internal state, request status,
// keystore active marker — matches the payload, and repairs missing pieces.
// Mismatches halt gracefully rather than silently mutate.
func (f *MetaFSM) applyKEKRotate(applyIndex uint64, payload []byte) error {
	cmd, err := DecodeMetaKEKRotateCmd(payload)
	if err != nil {
		return fmt.Errorf("KEKRotate: decode: %w", err)
	}

	// 0. confirm token (cheap, deterministic, independent of state)
	if cmd.Confirm != "rotate-now" {
		return fmt.Errorf("KEKRotate: bad confirm token")
	}

	if f.keystore == nil {
		return fmt.Errorf("KEKRotate: keystore not wired")
	}
	if f.dekKeeper == nil {
		return fmt.Errorf("KEKRotate: dek keeper not wired")
	}

	// 1. Idempotent replay branch. A snapshot-tail re-Apply finds activeKEKVersion
	//    already at cmd.NewVersion. Gate on NewVersion > 0 so a fresh FSM with
	//    activeKEKVersion=0 doesn't accidentally take this branch for a cmd that
	//    proposes NewVersion=0 (illegal — proposals must advance the active
	//    version by exactly 1, see step 2). Hold f.mu for the whole sequence so
	//    a snapshot writer cannot observe an in-progress repair (Pass-5 H2).
	f.mu.Lock()
	currentActive := f.activeKEKVersion
	if cmd.NewVersion > 0 && cmd.NewVersion == currentActive && f.keystore.HasVersion(cmd.NewVersion) {
		defer f.mu.Unlock()
		return f.replayKEKRotate(applyIndex, cmd)
	}
	f.mu.Unlock()

	// 2. Ordinary version mismatch.
	if cmd.NewVersion != currentActive+1 {
		return fmt.Errorf("KEKRotate: new_version=%d, expected %d (active=%d)", cmd.NewVersion, currentActive+1, currentActive)
	}

	// 3. wrap_set_hash check — deterministic no-op on mismatch.
	currentHash := f.canonicalCurrentWrapSetHash()
	if !bytes.Equal(currentHash[:], cmd.WrapSetHash) {
		f.RecordRotationRequestStatus(cmd.RequestID, RotationStatusStaleNoOp, applyIndex)
		return nil
	}

	// 4. Unwrap K_new under the active KEK with the rotation AAD.
	activeKEK, err := f.keystore.Get(currentActive)
	if err != nil {
		return fmt.Errorf("KEKRotate: get active KEK %d: %w", currentActive, err)
	}
	defer zeroKEK(activeKEK)
	aad := encrypt.BuildAAD(encrypt.DomainKEKRotate, f.clusterID[:], encrypt.FieldUint32(cmd.NewVersion))
	newKEK, err := encrypt.AESGCMOpenWithAAD(activeKEK, cmd.WrappedNewKEK, aad)
	if err != nil {
		return fmt.Errorf("KEKRotate: unwrap K_new: %w", err)
	}
	defer zeroKEK(newKEK)
	if len(newKEK) != encrypt.KEKSize {
		return fmt.Errorf("KEKRotate: K_new wrong length %d", len(newKEK))
	}

	// 5. Verify the rewrapped DEK set: equal gen set, sorted ascending unique,
	//    every payload entry unseals under newKEK to the same plaintext as the
	//    corresponding current wrap under activeKEK. DEK wraps use NIL AAD
	//    (matches Phase A DEKKeeper.Rotate/Rewrap).
	if err := f.verifyRewrappedDEKsAgainstWrapSet(cmd.RewrappedDEKs, activeKEK, newKEK); err != nil {
		return fmt.Errorf("KEKRotate: rewrapped_deks: %w", err)
	}

	// 6. Idempotent disk write of K_new. AddAndPersist refuses to overwrite a
	//    pre-existing file, so we explicitly content-match on duplicate.
	if f.kekDir != "" {
		if err := f.keystore.AddAndPersist(f.kekDir, cmd.NewVersion, newKEK); err != nil {
			if errors.Is(err, encrypt.ErrKEKVersionDuplicate) {
				existing, getErr := f.keystore.Get(cmd.NewVersion)
				if getErr != nil {
					return fatalKEKApply(fmt.Errorf("KEKRotate: refetch K_new after duplicate: %v", getErr))
				}
				if subtle.ConstantTimeCompare(existing, newKEK) != 1 {
					zeroKEK(existing)
					return fatalKEKApply(fmt.Errorf("KEKRotate: K_new disk content mismatch — graceful halt"))
				}
				zeroKEK(existing)
			} else {
				return fatalKEKApply(fmt.Errorf("KEKRotate: persist K_new: %v", err))
			}
		}
	} else {
		// No on-disk persistence configured (test wiring). Install in memory
		// only. Duplicate Add is tolerated when the bytes match.
		if err := f.keystore.Add(cmd.NewVersion, newKEK); err != nil {
			if errors.Is(err, encrypt.ErrKEKVersionDuplicate) {
				existing, getErr := f.keystore.Get(cmd.NewVersion)
				if getErr != nil {
					return fatalKEKApply(fmt.Errorf("KEKRotate: refetch K_new after duplicate: %v", getErr))
				}
				if subtle.ConstantTimeCompare(existing, newKEK) != 1 {
					zeroKEK(existing)
					return fatalKEKApply(fmt.Errorf("KEKRotate: K_new in-memory content mismatch — graceful halt"))
				}
				zeroKEK(existing)
			} else {
				return fatalKEKApply(fmt.Errorf("KEKRotate: store K_new: %v", err))
			}
		}
	}

	// 7. Snapshot-atomic install. f.mu held for the entire mutation window so
	//    a snapshot writer reading f.activeKEKVersion cannot observe it
	//    advanced while DEKKeeper wraps still belong to the prior version (or
	//    vice versa). f.activeKEKVersion + dekKeeper.kek + dekKeeper.wrap
	//    mutate together under f.mu. Note: KEKStore.Add (step 6) had a
	//    side-effect of auto-advancing keystore.active to NewVersion already
	//    — that is benign because (a) the DKVS snapshot trailer reads
	//    f.activeKEKVersion, not keystore.active, and (b) the apply goroutine
	//    is single-writer, so no concurrent Apply observes the brief skew
	//    between keystore.active and f.activeKEKVersion. The explicit
	//    SetActiveVersion below is thus idempotent on first Apply and
	//    load-bearing only on the replay-repair path (step (e)).
	rewrappedMap := buildRewrappedMap(cmd.RewrappedDEKs)
	f.mu.Lock()
	f.activeKEKVersion = cmd.NewVersion
	if err := f.dekKeeper.InstallKEKRotation(newKEK, rewrappedMap); err != nil {
		f.mu.Unlock()
		return fatalKEKApply(fmt.Errorf("KEKRotate: DEKKeeper install: %v", err))
	}
	if err := f.keystore.SetActiveVersion(cmd.NewVersion); err != nil {
		f.mu.Unlock()
		return fatalKEKApply(fmt.Errorf("KEKRotate: SetActiveVersion: %v", err))
	}
	f.recordRotationRequestStatusLocked(cmd.RequestID, RotationStatusApplied, applyIndex)
	f.mu.Unlock()

	// 8. Audit — payload fields only (no node-local time.Now). Task 10 will
	//    wire a real audit sink; the deterministic structure is fixed here.
	f.auditAppendKEKRotate(cmd, applyIndex)
	return nil
}

// replayKEKRotate runs the idempotent-replay verification with f.mu already
// held by the caller. Returns nil on success (full state consistent with
// payload), or a wrapped ErrFSMKEKFatal on any mismatch we cannot repair.
func (f *MetaFSM) replayKEKRotate(applyIndex uint64, cmd KEKRotateCmd) error {
	// (a) K_new on disk byte-identical to the payload-unwrapped K_new. We
	// re-derive K_new from cmd.WrappedNewKEK to confirm the snapshot bytes
	// match what this log entry would have produced.
	existing, err := f.keystore.Get(cmd.NewVersion)
	if err != nil {
		return fatalKEKApply(fmt.Errorf("KEKRotate: replay get K_new: %v", err))
	}
	defer zeroKEK(existing)
	prevKEK, err := f.keystore.Get(cmd.NewVersion - 1)
	if err != nil {
		return fatalKEKApply(fmt.Errorf("KEKRotate: replay get K_prev: %v", err))
	}
	defer zeroKEK(prevKEK)
	aad := encrypt.BuildAAD(encrypt.DomainKEKRotate, f.clusterID[:], encrypt.FieldUint32(cmd.NewVersion))
	replayKEK, err := encrypt.AESGCMOpenWithAAD(prevKEK, cmd.WrappedNewKEK, aad)
	if err != nil {
		return fatalKEKApply(fmt.Errorf("KEKRotate: replay unwrap K_new: %v", err))
	}
	defer zeroKEK(replayKEK)
	if subtle.ConstantTimeCompare(existing, replayKEK) != 1 {
		return fatalKEKApply(fmt.Errorf("KEKRotate: replay K_new disk content mismatch"))
	}

	// (b) FSM wrap[] must equal cmd.RewrappedDEKs byte-for-byte. Acquires
	// DEKKeeper.mu under f.mu (lock order: f.mu → keeper.mu).
	if err := f.verifyWrapMapEqualsPayload(cmd.RewrappedDEKs); err != nil {
		return fatalKEKApply(fmt.Errorf("KEKRotate: replay wrap[] mismatch: %v", err))
	}

	// (c) DEKKeeper idempotent install. InstallKEKRotation overwrites k.kek
	// and k.wrap atomically — since (a) and (b) just proved both halves
	// already match the payload, the call is a no-op.
	rewrappedMap := buildRewrappedMap(cmd.RewrappedDEKs)
	if err := f.dekKeeper.InstallKEKRotation(replayKEK, rewrappedMap); err != nil {
		return fatalKEKApply(fmt.Errorf("KEKRotate: replay DEKKeeper install: %v", err))
	}

	// (d) Request status repair. The FIFO ring may have evicted the original
	// Applied entry; record it again so subsequent lookups succeed.
	if s, found := f.lookupRotationRequestStatusLocked(cmd.RequestID); !found || s != RotationStatusApplied {
		f.recordRotationRequestStatusLocked(cmd.RequestID, RotationStatusApplied, applyIndex)
	}

	// (e) KEKStore active marker repair. A crash between step 7 and the
	// SetActiveVersion call in the original Apply leaves the marker stale;
	// idempotent repair brings it back in line.
	if f.keystore.ActiveVersion() != cmd.NewVersion {
		if err := f.keystore.SetActiveVersion(cmd.NewVersion); err != nil {
			return fatalKEKApply(fmt.Errorf("KEKRotate: replay SetActiveVersion repair: %v", err))
		}
	}

	// No audit double-append on replay — original Apply already wrote the
	// audit line; the request status records the deterministic decision.
	return nil
}

// canonicalCurrentWrapSetHash returns the deterministic SHA-256 hash of the
// current DEKKeeper wrap[] entries (sorted by gen). The caller may or may not
// hold f.mu; this method acquires keeper.mu only via DEKKeeper.VersionsAndActive.
func (f *MetaFSM) canonicalCurrentWrapSetHash() [32]byte {
	wraps, _ := f.dekKeeper.VersionsAndActive()
	entries := make([]encrypt.WrapSetEntry, 0, len(wraps))
	for g, w := range wraps {
		entries = append(entries, encrypt.WrapSetEntry{Gen: g, Wrap: w})
	}
	return encrypt.CanonicalWrapSetHash(entries)
}

// verifyRewrappedDEKsAgainstWrapSet enforces that every payload entry matches
// the FSM's current wrap[] in plaintext under the supplied KEKs. DEK wraps
// use NIL AAD to match Phase A DEKKeeper.Rotate / Rewrap (no AAD on the
// fsm-internal wrap envelope; storage-layer AAD lives at the value layer).
func (f *MetaFSM) verifyRewrappedDEKsAgainstWrapSet(payload []RewrappedDEKEntry, activeKEK, newKEK []byte) error {
	// Sort ascending, reject duplicates.
	for i := 1; i < len(payload); i++ {
		if payload[i].Gen <= payload[i-1].Gen {
			return fmt.Errorf("payload not sorted ascending unique by gen at index %d (gens %d,%d)", i, payload[i-1].Gen, payload[i].Gen)
		}
	}

	currentWraps, _ := f.dekKeeper.VersionsAndActive()
	if len(payload) != len(currentWraps) {
		return fmt.Errorf("payload gen count %d != FSM wrap count %d", len(payload), len(currentWraps))
	}

	for _, e := range payload {
		currentWrap, ok := currentWraps[e.Gen]
		if !ok {
			return fmt.Errorf("payload gen %d not in FSM wrap[]", e.Gen)
		}
		oldPlain, err := encrypt.AESGCMOpen(activeKEK, currentWrap)
		if err != nil {
			return fmt.Errorf("payload gen %d: unseal current wrap with active KEK: %w", e.Gen, err)
		}
		newPlain, err := encrypt.AESGCMOpen(newKEK, e.Wrapped)
		if err != nil {
			zeroKEK(oldPlain)
			return fmt.Errorf("payload gen %d: unseal rewrapped with new KEK: %w", e.Gen, err)
		}
		eq := subtle.ConstantTimeCompare(oldPlain, newPlain) == 1
		zeroKEK(oldPlain)
		zeroKEK(newPlain)
		if !eq {
			return fmt.Errorf("payload gen %d: rewrapped plaintext mismatch", e.Gen)
		}
	}
	return nil
}

// verifyWrapMapEqualsPayload returns nil iff DEKKeeper's current wrap[] is
// byte-identical to the payload's RewrappedDEKs. Used by the idempotent
// replay branch (where the install has already happened in the original
// Apply, so the keeper's wraps should already match the payload bytes).
//
// Lock convention: caller MUST hold f.mu. This calls keeper.VersionsAndActive
// which acquires keeper.mu — order preserved (f.mu → keeper.mu, never reverse).
func (f *MetaFSM) verifyWrapMapEqualsPayload(payload []RewrappedDEKEntry) error {
	for i := 1; i < len(payload); i++ {
		if payload[i].Gen <= payload[i-1].Gen {
			return fmt.Errorf("payload not sorted ascending unique by gen at index %d (gens %d,%d)", i, payload[i-1].Gen, payload[i].Gen)
		}
	}
	currentWraps, _ := f.dekKeeper.VersionsAndActive()
	if len(payload) != len(currentWraps) {
		return fmt.Errorf("payload gen count %d != DEKKeeper wrap count %d", len(payload), len(currentWraps))
	}
	for _, e := range payload {
		current, ok := currentWraps[e.Gen]
		if !ok {
			return fmt.Errorf("payload gen %d not in DEKKeeper wrap[]", e.Gen)
		}
		if !bytes.Equal(current, e.Wrapped) {
			return fmt.Errorf("payload gen %d wrap content differs from DEKKeeper wrap[]", e.Gen)
		}
	}
	return nil
}

// buildRewrappedMap copies the payload slice into a fresh map suitable for
// DEKKeeper.InstallKEKRotation. The keeper takes its own defensive copy on
// install, but this layer also defensive-copies so the caller is free to
// mutate the payload slice (or it being a flatbuffer view, free its memory)
// after install returns.
func buildRewrappedMap(payload []RewrappedDEKEntry) map[uint32][]byte {
	out := make(map[uint32][]byte, len(payload))
	for _, e := range payload {
		out[e.Gen] = append([]byte(nil), e.Wrapped...)
	}
	return out
}

// auditAppendKEKRotate is a placeholder that Task 10 will wire to the real
// audit sink. Kept deterministic (payload fields only) so the structure
// can be exercised by tests today.
func (f *MetaFSM) auditAppendKEKRotate(cmd KEKRotateCmd, applyIndex uint64) {
	// Intentionally empty — Task 10 will populate.
	_ = cmd
	_ = applyIndex
}

// zeroKEK overwrites a KEK-sized byte slice with zeros. Defense against
// memory forensics on transient unwrapped KEK material.
func zeroKEK(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
