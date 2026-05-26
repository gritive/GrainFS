package cluster

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
)

// SetEncryptor wires the cluster-wide encryptor used to gate cluster-config
// patches carrying wrapped secrets. Must be called before the raft log starts
// replaying. nil means cluster-config patches with a wrapped secret will be
// rejected at apply.
func (f *MetaFSM) SetEncryptor(e *encrypt.Encryptor) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.encryptor = e
}

// Encryptor returns the registered encryptor, or nil if it has not been wired.
func (f *MetaFSM) Encryptor() *encrypt.Encryptor { return f.encryptor }

// SetDEKKeeper wires the DEK keeper into the MetaFSM. Must be called before
// the apply loop starts — either pre-Start during bootMetaRaftWiring, or via
// MetaRaft.Start's preApplyLoop callback (which runs AFTER Restore but BEFORE
// the apply goroutine launches; that is how §7 T57 swaps in a keeper rebuilt
// from the DKVS snapshot trailer). Calling SetDEKKeeper concurrently with the
// apply loop races DEKRotate / DEKVersionPrune / JWTSigningKeyRotate.
//
// nil means DEKRotate/DEKVersionPrune are safe no-ops (not configured yet).
func (f *MetaFSM) SetDEKKeeper(k *encrypt.DEKKeeper) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dekKeeper = k
}

// SetJWTKeySet replaces the in-process JWT KeySet used by the FSM apply path
// to reflect newly installed/demoted keys into memory. Must be called before
// the raft log starts replaying. Passing nil resets to the internal default.
func (f *MetaFSM) SetJWTKeySet(ks *iamjwt.KeySet) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ks == nil {
		f.jwtKeys = iamjwt.NewKeySet()
	} else {
		f.jwtKeys = ks
	}
}

// JWTKeySet returns the KeySet currently wired into the FSM.
// It is always non-nil (NewMetaFSM seeds a default KeySet).
// Callers should not modify the returned value directly; use SetJWTKeySet.
func (f *MetaFSM) JWTKeySet() *iamjwt.KeySet {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.jwtKeys
}

// dekRefCount returns the number of ObjectIndexEntry records that reference
// the given DEK generation. Returns 0 if the generation has no entries.
func (f *MetaFSM) dekRefCount(gen uint32) uint64 {
	return f.dekRefCounts[gen]
}

// incDEKRef increments the ref count for the given DEK generation.
// Must be called with f.mu held.
func (f *MetaFSM) incDEKRef(gen uint32) {
	f.dekRefCounts[gen]++
}

// decDEKRef decrements the ref count for the given DEK generation.
// Clamps at zero to guard against double-decrement on buggy replay.
// Must be called with f.mu held.
func (f *MetaFSM) decDEKRef(gen uint32) {
	if f.dekRefCounts[gen] > 0 {
		f.dekRefCounts[gen]--
		if f.dekRefCounts[gen] == 0 {
			delete(f.dekRefCounts, gen)
		}
	}
}

// PendingDEKVersions returns the DEK versions decoded during the last Restore
// call, along with the active generation. The runtime calls this after Restore
// to construct a DEKKeeper via encrypt.LoadFromFSM(kek, versions).
// Returns nil, 0 if no DKVS trailer was present in the snapshot.
func (f *MetaFSM) PendingDEKVersions() (map[uint32][]byte, uint32) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.pendingDEKVersions, f.pendingDEKActive
}

// SnapshotCapturedKEKVersion returns the active_kek_version that was recorded
// in the DKVS trailer of the most recent Restore call. This is the KEK version
// the wrapped DEKs were sealed under at snapshot time — which may differ from
// ActiveKEKVersion() if KEK rotation log entries have since been replayed.
// Returns 0 if no DKVS trailer was present (Phase A / pre-rotation snapshots).
// Task 4c: rebuildDEKKeeperFromRestore uses this to select the correct KEK
// for LoadFromFSM instead of store.ActiveKEK() (which is the current rotation state).
func (f *MetaFSM) SnapshotCapturedKEKVersion() uint32 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.pendingActiveKEKVersion
}

// ActiveKEKVersion returns the cluster-wide active KEK version that wrap[gen]
// entries are sealed under. Phase A always returns 0 (no rotation yet);
// Phase B will mutate this via MetaCmdTypeKEKRotate Apply.
func (f *MetaFSM) ActiveKEKVersion() uint32 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.activeKEKVersion
}

// SetActiveKEKVersion overwrites the in-memory active KEK version. Intended
// for future use by MetaCmdTypeKEKRotate Apply (Phase B) and for test setup.
// Not exposed via any RPC.
//
// Persistence note: the value is written only when the DKVS snapshot trailer
// is emitted (i.e. DEKKeeper wired with ≥1 version). For a freshly booted FSM
// with no DEKs yet, Snapshot/Restore round-trips will silently default the
// value back to 0 — which is the correct Phase A semantics.
func (f *MetaFSM) SetActiveKEKVersion(v uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.activeKEKVersion = v
}

// Rotation returns the rotation sub-FSM. State is decoupled from the rest of
// MetaFSM and has its own RWMutex; callers can read snapshots concurrently.
func (f *MetaFSM) Rotation() *RotationFSM { return f.rotation }

// SetOnRotationApplied wires a side-effect callback fired after each rotation
// command commits. Called from the FSM apply goroutine; the callback runs disk
// I/O and transport identity swaps. Set before MetaRaft.Start().
func (f *MetaFSM) SetOnRotationApplied(fn func(RotationState)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onRotationApplied = fn
}

// SetRotationSteady seeds the rotation FSM with the active SPKI on startup.
// Called by meta_raft initialization once the local PSK has been resolved.
func (f *MetaFSM) SetRotationSteady(activeSPKI [32]byte) {
	f.rotation.SetSteady(activeSPKI)
}

func (f *MetaFSM) applyDEKRotate() error {
	if f.dekKeeper == nil {
		return nil
	}
	return f.dekKeeper.Rotate()
}

func (f *MetaFSM) applyDEKVersionPrune(data []byte) error {
	if f.dekKeeper == nil {
		return nil
	}
	gen, err := decodeMetaDEKVersionPruneCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: DEKVersionPrune: %w", err)
	}
	safe := f.dekRefCount(gen) == 0
	return f.dekKeeper.Prune(gen, safe)
}

func (f *MetaFSM) applyJWTSigningKeyRotate(data []byte) error {
	if f.dekKeeper == nil {
		return fmt.Errorf("meta_fsm: JWTSigningKeyRotate: DEK keeper not wired")
	}
	kid, wrapped, dekGen, demotedAtUnix, err := decodeMetaJWTSigningKeyRotateCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: JWTSigningKeyRotate: decode: %w", err)
	}
	demotedAt := time.Unix(demotedAtUnix, 0)
	// Demote old current in the persistent store
	f.jwtKeyStore.Demote(demotedAt)
	// Install new current in the persistent store
	seed := iamjwt.KeySeed{Kid: kid, WrappedSecret: wrapped, DekGen: dekGen, Role: "current"}
	f.jwtKeyStore.Put(seed)
	// Reflect into the local in-process KeySet
	f.jwtKeys.DemoteCurrentToPrevious(demotedAt)
	if err := f.jwtKeys.InstallCurrent(seed, f.dekKeeper); err != nil {
		return fmt.Errorf("meta_fsm: JWTSigningKeyRotate: install jwt key locally: %w", err)
	}
	return nil
}

func (f *MetaFSM) applyJWTSigningKeyPrune(data []byte) error {
	pruneAtUnix, err := decodeMetaJWTSigningKeyPruneCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: JWTSigningKeyPrune: decode: %w", err)
	}
	pruneAt := time.Unix(pruneAtUnix, 0)
	if !f.jwtKeyStore.PrunePrevSafe(pruneAt) {
		return iamjwt.ErrPrunePrev
	}
	f.jwtKeyStore.RemovePrev()
	_ = f.jwtKeys.Prune(true)
	return nil
}

// applyRotateKeyBegin commits phase 1 → 2 transition. The rotation FSM
// validates capabilities, idempotency, and phase preconditions; on success
// the side-effect callback is invoked with the new state so the worker can
// load keys.d/next.key, verify SPKI, and swap the transport accept set.
func (f *MetaFSM) applyRotateKeyBegin(data []byte) error {
	c, err := decodeMetaRotateKeyBeginCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: RotateKeyBegin: %w", err)
	}
	if err := f.rotation.Apply(c); err != nil {
		// FSM rejected (capability missing, conflicting rotation in progress).
		// Log but do not crash apply loop — followers must converge with leader.
		log.Warn().Err(err).Msg("meta_fsm: RotateKeyBegin rejected by rotation FSM")
		return nil
	}
	f.fireRotationApplied()
	return nil
}

func (f *MetaFSM) applyRotateKeySwitch(data []byte) error {
	c, err := decodeMetaRotateKeySwitchCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: RotateKeySwitch: %w", err)
	}
	if err := f.rotation.Apply(c); err != nil {
		log.Warn().Err(err).Msg("meta_fsm: RotateKeySwitch rejected by rotation FSM")
		return nil
	}
	f.fireRotationApplied()
	return nil
}

func (f *MetaFSM) applyRotateKeyDrop(data []byte) error {
	c, err := decodeMetaRotateKeyDropCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: RotateKeyDrop: %w", err)
	}
	if err := f.rotation.Apply(c); err != nil {
		log.Warn().Err(err).Msg("meta_fsm: RotateKeyDrop rejected by rotation FSM")
		return nil
	}
	f.fireRotationApplied()
	return nil
}

func (f *MetaFSM) applyRotateKeyAbort(data []byte) error {
	c, err := decodeMetaRotateKeyAbortCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: RotateKeyAbort: %w", err)
	}
	if err := f.rotation.Apply(c); err != nil {
		log.Warn().Err(err).Msg("meta_fsm: RotateKeyAbort rejected by rotation FSM")
		return nil
	}
	f.fireRotationApplied()
	return nil
}

// fireRotationApplied snapshots state and invokes the callback outside any
// FSM lock. The callback (RotationWorker.OnPhaseChange) does disk I/O and
// transport mutation — must not run under MetaFSM.mu.
func (f *MetaFSM) fireRotationApplied() {
	f.mu.RLock()
	cb := f.onRotationApplied
	f.mu.RUnlock()
	if cb == nil {
		return
	}
	cb(f.rotation.State())
}
