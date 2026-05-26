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
		return fatalKEKApply(fmt.Errorf("KEKRotate: get active KEK %d: %v", currentActive, err))
	}
	defer zeroKEK(activeKEK)
	aad := encrypt.BuildAAD(encrypt.DomainKEKRotate, f.clusterID[:], encrypt.FieldUint32(cmd.NewVersion))
	newKEK, err := encrypt.AESGCMOpenWithAAD(activeKEK, cmd.WrappedNewKEK, aad)
	if err != nil {
		return fatalKEKApply(fmt.Errorf("KEKRotate: unwrap K_new: %v", err))
	}
	defer zeroKEK(newKEK)
	if len(newKEK) != encrypt.KEKSize {
		return fatalKEKApply(fmt.Errorf("KEKRotate: K_new wrong length %d", len(newKEK)))
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

// applyKEKRetire is the deterministic FSM apply of MetaCmdTypeKEKRetire.
// It marks the given KEK version as Retiring in both the kek_status table and
// the KEKStore. The key bytes are NOT removed — that is MetaKEKPruneCmd's job.
//
// Idempotent replay: if the version is already Retiring or Pruned the command
// is a no-op (return nil). This handles snapshot-tail re-application cleanly.
func (f *MetaFSM) applyKEKRetire(applyIndex uint64, payload []byte) error {
	cmd, err := DecodeMetaKEKRetireCmd(payload)
	if err != nil {
		return fmt.Errorf("KEKRetire: decode: %w", err)
	}

	// 0. confirm token — cheap, deterministic, independent of state.
	expectedConfirm := fmt.Sprintf("delete-permanently-%d", cmd.Version)
	if cmd.Confirm != expectedConfirm {
		return fmt.Errorf("KEKRetire: confirm token mismatch (got %q, want %q)", cmd.Confirm, expectedConfirm)
	}

	// 1. Idempotent replay: already Retiring or Pruned → no-op.
	if _, status, _, ok := f.LookupKEKStatus(cmd.Version); ok {
		if status == KEKLifecycleRetiring || status == KEKLifecyclePruned {
			return nil
		}
	}

	f.mu.Lock()
	currentActive := f.activeKEKVersion
	f.mu.Unlock()

	// 2. Cannot retire the active version.
	if cmd.Version >= currentActive {
		return fmt.Errorf("KEKRetire: version %d must be < active %d", cmd.Version, currentActive)
	}

	// 3. Missing version is a node-local divergence — fatal.
	if !f.keystore.HasVersion(cmd.Version) {
		return fatalKEKApply(fmt.Errorf("KEKRetire: version %d not in keystore (local divergence)", cmd.Version))
	}

	// 4. cluster_state_at_propose mismatch → stale no-op (leader retries).
	if cmd.ClusterStateAtPropose.ActiveKEKVersion != currentActive {
		f.RecordRotationRequestStatus(cmd.RequestID, RotationStatusStaleNoOp, applyIndex)
		return nil
	}

	// 5. Mark in-memory retiring state.
	if err := f.keystore.Retire(cmd.Version); err != nil {
		return fatalKEKApply(fmt.Errorf("KEKRetire: in-memory mark: %w", err))
	}

	// 6. Update FSM kek_status and request status. SetKEKStatus acquires f.mu
	//    internally, so call it outside any f.mu lock window.
	f.SetKEKStatus(cmd.Version, KEKLifecycleRetiring, applyIndex)
	f.RecordRotationRequestStatus(cmd.RequestID, RotationStatusApplied, applyIndex)

	// 7. Audit — payload fields only (no node-local time.Now). Task 10 wires the real sink.
	f.auditAppendKEKRetire(cmd, applyIndex)
	return nil
}

// auditAppendKEKRetire is a placeholder that Task 10 will wire to the real
// audit sink.
func (f *MetaFSM) auditAppendKEKRetire(cmd KEKRetireCmd, applyIndex uint64) {
	// Intentionally empty — Task 10 will populate.
	_ = cmd
	_ = applyIndex
}

// applyKEKPrune is the deterministic FSM apply of MetaCmdTypeKEKPrune. It
// permanently removes a Retiring KEK version after every voter has attested
// lease_count == 0. The voter set is LEADER-STAMPED (cmd.VoterIDs) — Apply
// uses ONLY the stamped list and NEVER reads live raft configuration
// (followers lagging on a config-change entry would otherwise see a different
// voter set than the leader at Apply time, breaking determinism — Pass-12 C1).
//
// Idempotent replay branches:
//
//	(a) status==Pruned && !HasVersion → no-op no-error
//	(b) status==Retiring && !HasVersion → partial-apply recovery: the original
//	    Apply succeeded RemoveAndUnlink but crashed before SetKEKStatus(Pruned).
//	    Re-validate attestation (so an unrelated corrupt entry cannot be
//	    silently turned into Pruned by replay) and finalize the status.
//
// Voter coverage path runs validatePruneAttestation — a pure helper with no
// keystore or FSM-lock dependencies, reused by both the ordinary path and the
// partial-apply recovery branch.
func (f *MetaFSM) applyKEKPrune(applyIndex uint64, payload []byte) error {
	cmd, err := DecodeMetaKEKPruneCmd(payload)
	if err != nil {
		return fmt.Errorf("KEKPrune: decode: %w", err)
	}

	// 0. Confirm token — cheap, deterministic, independent of state.
	expectedConfirm := fmt.Sprintf("delete-permanently-%d", cmd.Version)
	if cmd.Confirm != expectedConfirm {
		return fmt.Errorf("KEKPrune: confirm token mismatch (got %q, want %q)", cmd.Confirm, expectedConfirm)
	}

	if f.keystore == nil {
		return fmt.Errorf("KEKPrune: keystore not wired")
	}

	// 1. Idempotent replay branches. Look up status before any mutation so
	//    replay paths short-circuit cleanly.
	_, status, retireIdx, ok := f.LookupKEKStatus(cmd.Version)
	hasVersion := f.keystore.HasVersion(cmd.Version)

	// (a) Already pruned + key not on disk → full replay; no-op.
	if ok && status == KEKLifecyclePruned && !hasVersion {
		return nil
	}
	// (b) Retiring + key missing on disk → partial-apply recovery. The first
	//     Apply already unlinked the disk file but crashed before
	//     SetKEKStatus(Pruned). Validate attestation (defense against a
	//     corrupt entry silently flipping us to Pruned) then finalize.
	if ok && status == KEKLifecycleRetiring && !hasVersion {
		if err := validatePruneAttestation(cmd, retireIdx); err != nil {
			return fmt.Errorf("KEKPrune: partial-apply recovery: %w", err)
		}
		f.SetKEKStatus(cmd.Version, KEKLifecyclePruned, applyIndex)
		f.RecordRotationRequestStatus(cmd.RequestID, RotationStatusApplied, applyIndex)
		f.auditAppendKEKPrune(cmd, applyIndex)
		return nil
	}

	// 2. Ordinary path: require Retiring state AND key still on disk.
	if !ok || status != KEKLifecycleRetiring {
		return fmt.Errorf("KEKPrune: version %d not in retiring state (status=%d, present=%v)", cmd.Version, status, ok)
	}
	if !hasVersion {
		// Defensive: covered by branch (b). If we reach here, lifecycle state
		// is inconsistent (Retiring without the key bytes) — but the (b)
		// branch already would have handled it. Treat as ordinary error.
		return fmt.Errorf("KEKPrune: version %d retiring but missing from keystore", cmd.Version)
	}

	// 3a. voter_config_hash gate FIRST (Pass-3 H3). Length is already enforced
	//     by Decode (must be 32). Compare canonical encoding of stamped
	//     voter_ids against the stamped hash. Mismatch → record StaleNoOp and
	//     return nil so the leader retries (this is operator-visible drift,
	//     not a hard rejection — see Task 9 expectation #7).
	wantHash := encrypt.CanonicalVoterSetHash(cmd.VoterIDs)
	if !bytes.Equal(wantHash[:], cmd.VoterConfigHash) {
		f.RecordRotationRequestStatus(cmd.RequestID, RotationStatusStaleNoOp, applyIndex)
		return nil
	}

	// 3b. Full attestation validation.
	if err := validatePruneAttestation(cmd, retireIdx); err != nil {
		return fmt.Errorf("KEKPrune: %w", err)
	}

	// 4. Disk-first state mutation. RemoveAndUnlink performs os.Remove +
	//    fsync(parent) THEN drops the in-memory entry. On disk failure, the
	//    in-memory state is preserved so a later replay can retry — see
	//    branch (b) above.
	if err := f.keystore.RemoveAndUnlink(f.kekDir, cmd.Version); err != nil {
		return fatalKEKApply(fmt.Errorf("KEKPrune: remove and unlink: %v", err))
	}

	// 5. Finalize status + record request outcome.
	f.SetKEKStatus(cmd.Version, KEKLifecyclePruned, applyIndex)
	f.RecordRotationRequestStatus(cmd.RequestID, RotationStatusApplied, applyIndex)

	// 6. Audit — payload fields only (no node-local time.Now). Task 10 wires the real sink.
	f.auditAppendKEKPrune(cmd, applyIndex)
	return nil
}

// validatePruneAttestation is a pure helper that verifies the attestation
// portion of a KEKPruneCmd against the stamped voter_ids list and the retire
// commit index. NO keystore or FSM-lock dependencies — reused by both the
// ordinary Apply path and the partial-apply recovery branch.
//
// Checks (Pass-1 C4, Pass-5 C2, Pass-6 C1):
//   - voter_ids non-empty
//   - voter_ids sorted ascending unique (Decode already enforces; double-check)
//   - voter_config_hash matches CanonicalVoterSetHash(voter_ids)
//   - attestation count == voter count
//   - no duplicate node_id in attestations
//   - every attestation node_id is in voter_ids
//   - every voter_id has an attestation
//   - every observed_at_index >= retire commit index
//   - every lease_count == 0
func validatePruneAttestation(cmd KEKPruneCmd, retireIdx uint64) error {
	if len(cmd.VoterIDs) == 0 {
		return fmt.Errorf("voter_ids must be non-empty")
	}
	for i := 1; i < len(cmd.VoterIDs); i++ {
		if cmd.VoterIDs[i] <= cmd.VoterIDs[i-1] {
			return fmt.Errorf("voter_ids not sorted ascending unique at index %d", i)
		}
	}
	wantHash := encrypt.CanonicalVoterSetHash(cmd.VoterIDs)
	if !bytes.Equal(wantHash[:], cmd.VoterConfigHash) {
		return fmt.Errorf("voter_config_hash does not match canonical encoding of voter_ids")
	}

	expectedVoters := make(map[string]struct{}, len(cmd.VoterIDs))
	for _, id := range cmd.VoterIDs {
		expectedVoters[id] = struct{}{}
	}
	if len(cmd.LeaseAttestation) != len(expectedVoters) {
		return fmt.Errorf("attestation count %d != voter count %d", len(cmd.LeaseAttestation), len(expectedVoters))
	}
	seen := make(map[string]struct{}, len(cmd.LeaseAttestation))
	for _, att := range cmd.LeaseAttestation {
		if _, dup := seen[att.NodeID]; dup {
			return fmt.Errorf("duplicate attestation for node %s", att.NodeID)
		}
		seen[att.NodeID] = struct{}{}
		if _, isVoter := expectedVoters[att.NodeID]; !isVoter {
			return fmt.Errorf("attestation from non-voter %s", att.NodeID)
		}
		if att.ObservedAtIndex < retireIdx {
			return fmt.Errorf("attestation observed_at_index %d < retire commit index %d for node %s", att.ObservedAtIndex, retireIdx, att.NodeID)
		}
		if att.LeaseCount != 0 {
			return fmt.Errorf("node %s lease_count=%d > 0", att.NodeID, att.LeaseCount)
		}
	}
	for nodeID := range expectedVoters {
		if _, hit := seen[nodeID]; !hit {
			return fmt.Errorf("missing attestation from voter %s", nodeID)
		}
	}
	return nil
}

// auditAppendKEKPrune is a placeholder that Task 10 will wire to the real
// audit sink.
func (f *MetaFSM) auditAppendKEKPrune(cmd KEKPruneCmd, applyIndex uint64) {
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
