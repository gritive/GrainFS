// Package cluster: leader-side KEK rotation orchestrator (Task 5).
//
// ProposeKEKRotate runs the full pre-validation pipeline on the leader, then
// submits a MetaKEKRotateCmd through meta-Raft. By the time the cmd reaches
// follower FSMs every byte (K_new wrap, rewrapped DEK set, wrap_set_hash) is
// fixed — followers verify-and-install verbatim, no node-side reseal.
//
// Pre-validation order (each check rejects with a precise error message):
//
//  1. confirm token == "rotate-now"
//  2. epochCtx non-nil (i.e. this node is leader; Task 6 will wire real lifecycle)
//  3. mu.TryLock — only one rotation in flight at a time (kek_lifecycle_in_flight)
//  4. ActiveKEKVersion != math.MaxUint32 (v2 keyspace migration guard)
//  5. live DEK gen count <= MaxLiveDEKGens (256 KiB payload budget)
//  6. peerProbe.ProbeAllKEKDiskSpace — every voter reports >= MinKeystoreFreeBytes
//  7. crypto build: K_new (32B random) → wrap under K_active (AAD) → re-seal every
//     DEK (NIL AAD per Phase A) → wrap_set_hash over CURRENT FSM wraps
//  8. total encoded payload <= MaxKEKRotateCmdBytes
//  9. dry-run validate (pure function over the FSM's current view)
//
// 10. raft.Propose — blocks until applied on this leader
// 11. LookupRotationRequestStatus → translate Applied/StaleNoOp/Rejected
package cluster

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// Pre-validation budget constants. The 256 KiB cap on the encoded MetaCmd
// keeps a single rotation under the raft-log per-entry budget; with 32-byte
// wrapped KEK (~60B GCM-sealed) and ~80B per DEK entry, 1024 live gens still
// fits comfortably below 100 KiB — the explicit cap gives operators time to
// run a DEK prune before bumping these defaults.
const (
	MaxLiveDEKGens       = 1024
	MaxKEKRotateCmdBytes = 256 * 1024
	MinKeystoreFreeBytes = 64 * 1024
)

// ErrKEKRotateAnotherInFlight is returned by ProposeKEKRotate when a previous
// rotation has not finished. Sentinel so callers (admin UDS, future CLI) can
// distinguish "retry later" from "operator must fix something".
var ErrKEKRotateAnotherInFlight = errors.New("KEKRotate: another rotation in flight")

// PeerKEKProbe is the interface the leader uses to query every voter's
// keystore-directory free bytes (rotate path) and per-version in-flight lease
// count (prune path).
//
// ProbeAllKEKDiskSpace MUST include the local (leader) node in the returned
// slice — disk pressure on the proposer is just as fatal as on a follower.
//
// ProbeKEKLeaseSnapshot accepts an explicit voter list (leader-stamped at
// propose time) so the prune path's voter coverage matches the voter set the
// FSM Apply will later validate against. It MUST attempt every voter and
// return exactly one LeaseAttestationSample per voter on success; missing or
// failed voters surface as an error.
type PeerKEKProbe interface {
	ProbeAllKEKDiskSpace(ctx context.Context) ([]KEKDiskSpaceResp, error)
	ProbeKEKLeaseSnapshot(ctx context.Context, voters []string, version uint32) ([]LeaseAttestationSample, error)
}

// LeaseAttestationSample is the leader-side view of a single voter's
// KEKLeaseSnapshotResp — flattened to the fields the FSM cmd actually carries.
type LeaseAttestationSample struct {
	NodeID          string
	ObservedAtIndex uint64
	LeaseCount      uint64
}

// RaftConfigReader exposes the effective raft configuration (the committed
// voter list) to the leader at propose time. The returned voter IDs MUST
// already be sorted ascending unique. configIndex is the raft log index at
// which the current configuration was committed; the leader uses it to
// detect mid-flight membership changes (race-detect: re-read after probe,
// abort if it advanced).
//
// FSM Apply NEVER consults this — voter set is leader-stamped into the
// MetaKEKPruneCmd payload (Pass-12 C1). A follower lagging on a config-change
// entry would otherwise see a different live voter set than the leader at
// Apply time → nondeterminism.
type RaftConfigReader interface {
	EffectiveConfiguration() (voterIDs []string, configIndex uint64)
}

// ErrKEKPruneNotLeader is returned by ProposeKEKPrune when the local node is
// not the raft leader.
var ErrKEKPruneNotLeader = errors.New("KEKPrune: not leader")

// KEKRaftSubmitter is the subset of MetaRaft the leader orchestrator depends on.
// Production wires *MetaRaft directly. Tests inject a fake whose Propose runs
// the FSM Apply path synchronously (matching MetaRaft's wait-applied semantics).
type KEKRaftSubmitter interface {
	Propose(ctx context.Context, cmdType MetaCmdType, payload []byte) error
}

// leadershipEpoch carries a context that is cancelled when this node loses
// raft leadership. Task 6 will wire Acquire/LoseLeadership; for Task 5 the
// test fixture calls SetEpochCtx directly to simulate leadership state.
type leadershipEpoch struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// KEKRotationLeader orchestrates the leader-side validate-and-submit pipeline
// for MetaKEKRotateCmd. Construct via NewKEKRotationLeader; wire leadership
// state with AcquireLeadership / LoseLeadership (or the legacy SetEpochCtx /
// ClearEpoch aliases). Single-flight is enforced via mu.
type KEKRotationLeader struct {
	fsm              *MetaFSM
	raft             KEKRaftSubmitter
	peerProbe        PeerKEKProbe
	raftConfigReader RaftConfigReader

	// mu serialises all KEK lifecycle operations: Rotate (now), and Retire /
	// Prune (implemented in later tasks). Conceptually "kek_lifecycle_in_flight":
	// only one lifecycle operation runs at a time. Acquired non-blocking via
	// TryLock — concurrent calls fail-fast rather than queuing. Auto-released by
	// the in-flight goroutine's deferred Unlock when the epoch ctx is cancelled
	// on leadership loss; LoseLeadership must NOT explicitly Unlock (race).
	mu sync.Mutex

	// epochCtx.Load() returns nil when this node is NOT leader. The ctx is
	// cancelled by LoseLeadership; ProposeKEKRotate uses it as the parent of its
	// bounded 60s timeout so an in-flight rotation aborts cleanly on step-down.
	epochCtx atomic.Pointer[leadershipEpoch]

	// rngKEK returns 32 random bytes for K_new. crypto/rand by default;
	// tests inject a deterministic source.
	rngKEK func() ([]byte, error)

	// wallClock stamps audit fields. time.Now by default; tests inject a
	// deterministic clock.
	wallClock func() time.Time

	// onMutexAcquired is a test-only hook called immediately after mu.TryLock
	// succeeds. nil in production. Used to signal test goroutines that the mutex
	// is held so they can call LoseLeadership without a sleep race.
	onMutexAcquired func()
}

// KEKRotationLeaderConfig wires production dependencies. fsm + raft + peerProbe
// are required; rngKEK and wallClock default to crypto/rand and time.Now when
// nil.
type KEKRotationLeaderConfig struct {
	FSM       *MetaFSM
	Raft      KEKRaftSubmitter
	PeerProbe PeerKEKProbe
	// RaftConfigReader is consulted by ProposeKEKPrune at propose time to
	// snapshot the voter set + raft config_index. Optional for code paths
	// that only call ProposeKEKRotate (it is unused by rotate); ProposeKEKPrune
	// returns an error if it is nil.
	RaftConfigReader RaftConfigReader
	RNGKEK           func() ([]byte, error)
	WallClock        func() time.Time
}

// NewKEKRotationLeader constructs a leader. Panics on nil required fields —
// these are wired once at boot and a missing dep is a bug, not user input.
func NewKEKRotationLeader(cfg KEKRotationLeaderConfig) *KEKRotationLeader {
	if cfg.FSM == nil {
		panic("NewKEKRotationLeader: FSM is required")
	}
	if cfg.Raft == nil {
		panic("NewKEKRotationLeader: Raft is required")
	}
	if cfg.PeerProbe == nil {
		panic("NewKEKRotationLeader: PeerProbe is required")
	}
	rng := cfg.RNGKEK
	if rng == nil {
		rng = defaultRNGKEK
	}
	clk := cfg.WallClock
	if clk == nil {
		clk = time.Now
	}
	return &KEKRotationLeader{
		fsm:              cfg.FSM,
		raft:             cfg.Raft,
		peerProbe:        cfg.PeerProbe,
		raftConfigReader: cfg.RaftConfigReader,
		rngKEK:           rng,
		wallClock:        clk,
	}
}

// defaultRNGKEK reads encrypt.KEKSize bytes from crypto/rand.
func defaultRNGKEK() ([]byte, error) {
	buf := make([]byte, encrypt.KEKSize)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return nil, fmt.Errorf("rand: %w", err)
	}
	return buf, nil
}

// AcquireLeadership is called when this node wins raft leadership. It installs
// a fresh cancellable epoch context so ProposeKEKRotate can proceed, and
// cancels any stale prior epoch. The mutex is NOT held on entry (a fresh leader
// starts unlocked — enforced by test TestKEKRotationLeader_NewLeaderStartsUnlocked).
func (l *KEKRotationLeader) AcquireLeadership() {
	l.SetEpochCtx(context.Background())
}

// LoseLeadership is called when this node steps down. It cancels the epoch
// context, which propagates into any in-flight ProposeKEKRotate's ctx, causing
// the propose to return ErrNotLeader. The goroutine's deferred mu.Unlock then
// releases the mutex — LoseLeadership must NOT explicitly unlock (race).
func (l *KEKRotationLeader) LoseLeadership() {
	l.ClearEpoch()
}

// SetEpochCtx publishes a leadership-epoch context derived from parent.
// AcquireLeadership is the preferred production entry-point (passes
// context.Background()); SetEpochCtx exists for tests that need a
// controllable parent. A previous epoch is cancelled before the new one is
// stored.
func (l *KEKRotationLeader) SetEpochCtx(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	ep := &leadershipEpoch{ctx: ctx, cancel: cancel}
	if old := l.epochCtx.Swap(ep); old != nil {
		old.cancel()
	}
}

// ClearEpoch signals loss-of-leadership: the stored epoch (if any) is cancelled
// and removed. Any in-flight ProposeKEKRotate sees its ctx done and unwinds.
// LoseLeadership is the preferred production entry-point; ClearEpoch exists as
// an alias for backwards-compatible test helpers.
func (l *KEKRotationLeader) ClearEpoch() {
	if old := l.epochCtx.Swap(nil); old != nil {
		old.cancel()
	}
}

// ProposeKEKRotate runs the full leader-side pipeline. Returns nil on
// "FSM Applied", ErrKEKRotateAnotherInFlight on concurrent attempt, and a
// wrapped error describing which guard rejected otherwise.
func (l *KEKRotationLeader) ProposeKEKRotate(confirm, actor string) error {
	// 1. Confirm token. Cheap, no state needed; reject early.
	if confirm != "rotate-now" {
		return errors.New("KEKRotate: bad confirm token (must be \"rotate-now\")")
	}

	// 2. Epoch guard. Task 6 wires real raft leader-state into SetEpochCtx;
	//    for Task 5 the test fixture sets it directly. nil → not leader.
	ep := l.epochCtx.Load()
	if ep == nil {
		return errors.New("KEKRotate: not leader")
	}

	// 3. Bounded timeout off the epoch ctx so leader step-down cancels us.
	ctx, cancel := context.WithTimeout(ep.ctx, 60*time.Second)
	defer cancel()

	// 4. Single-flight. Non-blocking TryLock — concurrent calls fail-fast
	//    rather than queueing.
	if !l.mu.TryLock() {
		return ErrKEKRotateAnotherInFlight
	}
	defer l.mu.Unlock()
	if l.onMutexAcquired != nil {
		l.onMutexAcquired()
	}

	// 5. uint32 overflow guard. Active == math.MaxUint32 cannot advance
	//    without a v2 keyspace migration (cmd schema is uint32).
	active := l.fsm.ActiveKEKVersion()
	if active == math.MaxUint32 {
		return errors.New("KEKRotate: active_kek_version at max uint32 — v2 keyspace migration required")
	}
	newVersion := active + 1

	// 6. Live DEK gen count guard. Re-wrap payload size scales linearly with
	//    the number of live DEK generations; cap at 1024 so a single rotation
	//    cmd stays below MaxKEKRotateCmdBytes.
	currentWraps, _ := l.fsm.dekKeeper.VersionsAndActive()
	if liveGens := len(currentWraps); liveGens > MaxLiveDEKGens {
		return fmt.Errorf("KEKRotate: live DEK gens %d > MaxLiveDEKGens %d — run DEK prune first", liveGens, MaxLiveDEKGens)
	}

	// 7. Cluster-wide disk-space probe. The leader rejects if ANY voter
	//    (including itself) reports less than MinKeystoreFreeBytes free in
	//    its keystore directory. Catches OOD before a follower halts apply
	//    mid-rotation and forks state.
	probes, err := l.peerProbe.ProbeAllKEKDiskSpace(ctx)
	if err != nil {
		return fmt.Errorf("KEKRotate: probe disk space: %w", err)
	}
	for _, p := range probes {
		if p.FreeBytes < MinKeystoreFreeBytes {
			return fmt.Errorf("KEKRotate: node %s keystore free=%dB < %dB",
				p.NodeID, p.FreeBytes, MinKeystoreFreeBytes)
		}
	}

	// 8. Generate K_new (32B). zeroKEK on return — even on error paths.
	plainKnew, err := l.rngKEK()
	if err != nil {
		return fmt.Errorf("KEKRotate: rand K_new: %w", err)
	}
	defer zeroKEK(plainKnew)
	if len(plainKnew) != encrypt.KEKSize {
		return fmt.Errorf("KEKRotate: rngKEK returned %d bytes, want %d", len(plainKnew), encrypt.KEKSize)
	}

	// 9. Wrap K_new under K_active with the rotation AAD (DomainKEKRotate,
	//    clusterID, NewVersion). FSM apply verifies the same AAD.
	store := l.fsm.KEKStore()
	if store == nil {
		return errors.New("KEKRotate: keystore not wired")
	}
	activeKEK, err := store.Get(active)
	if err != nil {
		return fmt.Errorf("KEKRotate: get active KEK %d: %w", active, err)
	}
	defer zeroKEK(activeKEK)

	clusterID := l.fsm.ClusterID()
	aad := encrypt.BuildAAD(encrypt.DomainKEKRotate, clusterID[:], encrypt.FieldUint32(newVersion))
	wrappedNewKEK, err := encrypt.AESGCMSealWithAAD(activeKEK, plainKnew, aad)
	if err != nil {
		return fmt.Errorf("KEKRotate: wrap K_new: %w", err)
	}

	// 10. Re-seal every live DEK under K_new (NIL AAD, matching Phase A).
	//     Iterate the wrap map from step 6 — the apply path will verify the
	//     payload against the current wrap set via wrap_set_hash, so any
	//     concurrent DEK rotation between here and the FSM apply collapses
	//     to a StaleNoOp.
	rewrapped, err := reSealAllDEKs(currentWraps, activeKEK, plainKnew)
	if err != nil {
		return fmt.Errorf("KEKRotate: re-seal DEKs: %w", err)
	}

	// 11. Compute wrap_set_hash over the CURRENT FSM wraps (same canonical
	//     form the FSM uses).
	wrapSetHashArr := canonicalWrapSetHashOver(currentWraps)
	wrapSetHash := wrapSetHashArr[:]

	// 12. Stamp audit fields. UUID v7 is k-sortable so audit lines retain
	//     a natural time order even when raft index is not yet committed.
	uuidV7, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("KEKRotate: request id: %w", err)
	}
	var requestID [16]byte
	copy(requestID[:], uuidV7[:])

	cmd := KEKRotateCmd{
		PayloadVersion:       currentKEKRotatePayloadVersion,
		NewVersion:           newVersion,
		WrappedNewKEK:        wrappedNewKEK,
		WrapSetHash:          wrapSetHash,
		RewrappedDEKs:        rewrapped,
		Confirm:              confirm,
		Actor:                actor,
		RequestID:            requestID,
		RequestedAtUnixNanos: l.wallClock().UnixNano(),
		ClusterStateAtPropose: ClusterStateAtPropose{
			ActiveKEKVersion: active,
			RetainedKEKCount: uint32(len(store.Versions())),
			LiveDEKGenCount:  uint32(len(currentWraps)),
		},
	}

	// 13. Encode + total payload size guard.
	payload, err := EncodeMetaKEKRotateCmd(cmd)
	if err != nil {
		return fmt.Errorf("KEKRotate: encode: %w", err)
	}
	if len(payload) > MaxKEKRotateCmdBytes {
		return fmt.Errorf("KEKRotate: encoded payload %dB > %dB cap — run DEK prune first",
			len(payload), MaxKEKRotateCmdBytes)
	}

	// 14. Dry-run validate: re-runs the same shape checks the FSM Apply uses
	//     (sorted gens, count match, K_new AAD-unwrap, every payload entry
	//     plaintext-equal to its current wrap). Catches encode/rewrap bugs
	//     BEFORE they reach raft — a bad payload that made it through propose
	//     would fatal-halt every node's apply loop (ErrFSMKEKFatal).
	if err := dryRunValidateKEKRotate(l.fsm, cmd, activeKEK, plainKnew); err != nil {
		return fmt.Errorf("KEKRotate: dry-run: %w", err)
	}

	// 15. Submit via raft. MetaRaft.Propose blocks until applied locally —
	//     no separate readback wait needed.
	if err := l.raft.Propose(ctx, MetaCmdTypeKEKRotate, payload); err != nil {
		// If the epoch was cleared while we were in-flight, surface a
		// leader-loss error rather than a raw context cancellation so callers
		// can distinguish "step-down, retry on new leader" from "raft error".
		if l.epochCtx.Load() == nil {
			return errors.New("KEKRotate: not leader (lost leadership mid-rotation)")
		}
		return fmt.Errorf("KEKRotate: submit: %w", err)
	}

	// 16. Translate FSM-recorded request status into a leader-side error.
	//     LookupRotationRequestStatus is a plain map read; no timeout needed.
	status, found := l.fsm.LookupRotationRequestStatus(requestID)
	if !found {
		// Either the ring evicted the entry already (very rare for an Apply
		// that just returned) or the cmd took an early-return path (e.g.
		// keystore not wired). Treat as success — Propose already returned
		// nil — but surface for callers that care.
		return nil
	}
	switch status {
	case RotationStatusApplied:
		return nil
	case RotationStatusStaleNoOp:
		return errors.New("KEKRotate: stale wrap_set (concurrent DEK rotation); retry")
	case RotationStatusRejected:
		return errors.New("KEKRotate: rejected by FSM")
	default:
		return fmt.Errorf("KEKRotate: unexpected FSM status %d", status)
	}
}

// reSealAllDEKs unseals every (gen, wrap) under activeKEK with NIL AAD and
// re-seals under newKEK with NIL AAD. The result is sorted ascending by gen
// — the FSM verifier rejects unsorted/duplicate payloads. Plaintext DEK
// material is zeroed before return.
func reSealAllDEKs(currentWraps map[uint32][]byte, activeKEK, newKEK []byte) ([]RewrappedDEKEntry, error) {
	if len(currentWraps) == 0 {
		// Empty DEK set is legal (e.g. very fresh cluster); FSM will accept
		// a payload with zero rewrapped entries and just rotate the KEK.
		return nil, nil
	}
	out := make([]RewrappedDEKEntry, 0, len(currentWraps))
	for gen, wrap := range currentWraps {
		plain, err := encrypt.AESGCMOpen(activeKEK, wrap)
		if err != nil {
			return nil, fmt.Errorf("unseal gen %d under active KEK: %w", gen, err)
		}
		rewrapped, err := encrypt.AESGCMSeal(newKEK, plain)
		zeroKEK(plain)
		if err != nil {
			return nil, fmt.Errorf("re-seal gen %d under new KEK: %w", gen, err)
		}
		out = append(out, RewrappedDEKEntry{Gen: gen, Wrapped: rewrapped})
	}
	// Sort ascending by gen — FSM verify path requires it (see
	// verifyRewrappedDEKsAgainstWrapSet in meta_fsm_kek_apply.go).
	sortRewrappedAscending(out)
	return out, nil
}

// sortRewrappedAscending sorts entries in place by Gen ascending. Insertion
// sort — N is bounded by MaxLiveDEKGens (1024) and the typical case is < 10,
// so the simpler algorithm wins on cache and code-size.
func sortRewrappedAscending(entries []RewrappedDEKEntry) {
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && entries[j-1].Gen > entries[j].Gen; j-- {
			entries[j-1], entries[j] = entries[j], entries[j-1]
		}
	}
}

// canonicalWrapSetHashOver computes the same canonical hash the FSM uses
// (sorted-by-gen SHA-256 over WrapSetEntry list) but takes a wrap map directly
// — avoids forcing the FSM accessor through a lock for the leader path.
func canonicalWrapSetHashOver(wraps map[uint32][]byte) [32]byte {
	entries := make([]encrypt.WrapSetEntry, 0, len(wraps))
	for g, w := range wraps {
		entries = append(entries, encrypt.WrapSetEntry{Gen: g, Wrap: w})
	}
	return encrypt.CanonicalWrapSetHash(entries)
}

// dryRunValidateKEKRotate runs the FSM-apply-style verification checks
// against the in-memory cmd BEFORE raft propose. Does NOT mutate FSM state.
//
// Checks (must match meta_fsm_kek_apply.go's applyKEKRotate verifier):
//
//   - new_version == active + 1
//   - wrap_set_hash == canonical hash of the FSM's current wrap[]
//   - K_new AAD-unwraps cleanly under K_active and has KEK-sized plaintext
//     (and equals the plainKnew we just sealed — defends against in-place
//     payload corruption between seal and propose)
//   - payload sorted ascending unique by gen
//   - len(payload) == len(current wraps)
//   - every payload entry NIL-AAD-unseals under K_new to the same plaintext
//     as the current wrap NIL-AAD-unseals under K_active
//
// activeKEK / plainKnew are passed in (rather than re-fetched) so we avoid
// re-derefencing keystore and re-running AAD-open in the happy path.
func dryRunValidateKEKRotate(fsm *MetaFSM, cmd KEKRotateCmd, activeKEK, plainKnew []byte) error {
	// (a) Version-advance check.
	active := fsm.ActiveKEKVersion()
	if cmd.NewVersion != active+1 {
		return fmt.Errorf("new_version=%d, want %d (active=%d)", cmd.NewVersion, active+1, active)
	}

	// (b) wrap_set_hash check.
	currentHash := fsm.canonicalCurrentWrapSetHash()
	if len(cmd.WrapSetHash) != 32 {
		return fmt.Errorf("wrap_set_hash length %d, want 32", len(cmd.WrapSetHash))
	}
	for i := 0; i < 32; i++ {
		if currentHash[i] != cmd.WrapSetHash[i] {
			return fmt.Errorf("wrap_set_hash mismatch")
		}
	}

	// (c) K_new AAD-unwrap check + plaintext equality with the leader's
	//     freshly-generated K_new. If a buggy seal path produced a payload
	//     that decrypts to different bytes, every follower would fatal-halt;
	//     catch it here.
	clusterID := fsm.ClusterID()
	aad := encrypt.BuildAAD(encrypt.DomainKEKRotate, clusterID[:], encrypt.FieldUint32(cmd.NewVersion))
	unwrapped, err := encrypt.AESGCMOpenWithAAD(activeKEK, cmd.WrappedNewKEK, aad)
	if err != nil {
		return fmt.Errorf("AAD-unwrap K_new: %w", err)
	}
	defer zeroKEK(unwrapped)
	if len(unwrapped) != encrypt.KEKSize {
		return fmt.Errorf("K_new wrong length %d, want %d", len(unwrapped), encrypt.KEKSize)
	}
	if len(unwrapped) != len(plainKnew) {
		return fmt.Errorf("K_new length mismatch with leader-generated plain")
	}
	for i := range unwrapped {
		if unwrapped[i] != plainKnew[i] {
			return fmt.Errorf("K_new payload-vs-leader plaintext mismatch")
		}
	}

	// (d) Rewrapped DEK set checks — reuse the FSM verifier so the leader
	//     dry-run and the apply path are bit-for-bit equivalent.
	if err := fsm.verifyRewrappedDEKsAgainstWrapSet(cmd.RewrappedDEKs, activeKEK, unwrapped); err != nil {
		return fmt.Errorf("rewrapped_deks: %w", err)
	}
	return nil
}

// Compile-time assertion that the underlying clusterpb cmd-type byte matches
// the alias we export here. Guards against schema drift breaking the propose
// path silently.
var _ clusterpb.MetaCmdType = MetaCmdTypeKEKRotate

// ProposeKEKPrune runs the leader-side prune pipeline:
//
//  1. Epoch guard + bounded 60s timeout.
//  2. Single-flight via the same kek_lifecycle_in_flight mutex used by rotate.
//  3. Pre-check: target version must be Retiring in FSM kek_status.
//  4. Snapshot raft effective configuration (sorted voter IDs + config_index).
//  5. Probe every voter for an in-flight lease snapshot.
//  6. Race-detect: re-read config_index after the probe — abort if it advanced
//     (a newly added voter would otherwise be missing from the attestation).
//  7. Verify every attestation has lease_count == 0.
//  8. Build MetaKEKPruneCmd with leader-stamped voter_ids + voter_config_hash.
//  9. Submit via raft and translate the FSM-recorded request status.
//
// Returns nil on FSM Applied. Returns ErrKEKRotateAnotherInFlight on concurrent
// attempt (same mutex as ProposeKEKRotate). Other failures surface a wrapped
// error describing which gate rejected.
func (l *KEKRotationLeader) ProposeKEKPrune(version uint32, actor string) error {
	// 1. Epoch guard.
	ep := l.epochCtx.Load()
	if ep == nil {
		return ErrKEKPruneNotLeader
	}

	// 2. Bounded timeout off the epoch ctx so leader step-down cancels us.
	ctx, cancel := context.WithTimeout(ep.ctx, 60*time.Second)
	defer cancel()

	// 3. Single-flight: share the rotate mutex.
	if !l.mu.TryLock() {
		return ErrKEKRotateAnotherInFlight
	}
	defer l.mu.Unlock()
	if l.onMutexAcquired != nil {
		l.onMutexAcquired()
	}

	if l.raftConfigReader == nil {
		return errors.New("KEKPrune: raft config reader not wired")
	}

	// 4. Pre-check: target version must be Retiring in FSM kek_status.
	_, status, _, ok := l.fsm.LookupKEKStatus(version)
	if !ok || status != KEKLifecycleRetiring {
		return fmt.Errorf("KEKPrune: version %d must be in retiring state (status=%d, present=%v)", version, status, ok)
	}

	// 5. Snapshot raft effective configuration (voter IDs sorted ascending +
	//    config_index). Voter IDs MUST be sorted ascending unique per
	//    RaftConfigReader contract. Defensive sort here in case a future
	//    implementor returns unsorted.
	voters, configIndex := l.raftConfigReader.EffectiveConfiguration()
	if len(voters) == 0 {
		return errors.New("KEKPrune: empty voter set in raft configuration")
	}
	sortedVoters := append([]string(nil), voters...)
	sort.Strings(sortedVoters)
	for i := 1; i < len(sortedVoters); i++ {
		if sortedVoters[i] == sortedVoters[i-1] {
			return fmt.Errorf("KEKPrune: duplicate voter %q in raft configuration", sortedVoters[i])
		}
	}
	voterHash := encrypt.CanonicalVoterSetHash(sortedVoters)

	// 6. Probe every voter for an in-flight lease snapshot. Probe MUST attempt
	//    each voter (including local node) and return exactly one sample per
	//    voter on success.
	samples, err := l.peerProbe.ProbeKEKLeaseSnapshot(ctx, sortedVoters, version)
	if err != nil {
		return fmt.Errorf("KEKPrune: probe lease snapshot: %w", err)
	}
	if len(samples) != len(sortedVoters) {
		return fmt.Errorf("KEKPrune: only %d/%d voters responded — retry when cluster healthy", len(samples), len(sortedVoters))
	}

	// 7. Race-detect: did membership change between read and probe completion?
	_, configIndexAfter := l.raftConfigReader.EffectiveConfiguration()
	if configIndexAfter != configIndex {
		return fmt.Errorf("KEKPrune: raft membership changed during probe (config_index %d → %d) — retry", configIndex, configIndexAfter)
	}

	// 8. Verify every attestation has lease_count == 0. (FSM Apply will also
	//    re-check this — defense in depth.) Build the attestation list in
	//    voter sort order so the encoded payload is canonical.
	attestationByNode := make(map[string]LeaseAttestationSample, len(samples))
	for _, s := range samples {
		if _, dup := attestationByNode[s.NodeID]; dup {
			return fmt.Errorf("KEKPrune: duplicate probe response for node %s", s.NodeID)
		}
		attestationByNode[s.NodeID] = s
		if s.LeaseCount != 0 {
			return fmt.Errorf("KEKPrune: node %s lease_count=%d > 0 — version still in use", s.NodeID, s.LeaseCount)
		}
	}
	attestations := make([]LeaseAttestationEntry, 0, len(sortedVoters))
	for _, v := range sortedVoters {
		s, hit := attestationByNode[v]
		if !hit {
			return fmt.Errorf("KEKPrune: missing probe response from voter %s", v)
		}
		attestations = append(attestations, LeaseAttestationEntry(s))
	}

	// 9. Build cmd.
	uuidV7, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("KEKPrune: request id: %w", err)
	}
	var requestID [16]byte
	copy(requestID[:], uuidV7[:])

	cmd := KEKPruneCmd{
		PayloadVersion:       currentKEKPrunePayloadVersion,
		Version:              version,
		Confirm:              fmt.Sprintf("delete-permanently-%d", version),
		LeaseAttestation:     attestations,
		VoterIDs:             sortedVoters,
		VoterConfigIndex:     configIndex,
		VoterConfigHash:      voterHash[:],
		Actor:                actor,
		RequestID:            requestID,
		RequestedAtUnixNanos: l.wallClock().UnixNano(),
		ClusterStateAtPropose: ClusterStateAtPropose{
			ActiveKEKVersion: l.fsm.ActiveKEKVersion(),
			RetainedKEKCount: uint32(len(l.fsm.KEKStore().Versions())),
			LiveDEKGenCount:  uint32(l.liveDEKGenCount()),
		},
	}

	payload, err := EncodeMetaKEKPruneCmd(cmd)
	if err != nil {
		return fmt.Errorf("KEKPrune: encode: %w", err)
	}

	// 10. Submit via raft. MetaRaft.Propose blocks until applied locally —
	//     no separate readback wait needed (same shape as ProposeKEKRotate).
	if err := l.raft.Propose(ctx, MetaCmdTypeKEKPrune, payload); err != nil {
		if l.epochCtx.Load() == nil {
			return errors.New("KEKPrune: not leader (lost leadership mid-prune)")
		}
		return fmt.Errorf("KEKPrune: submit: %w", err)
	}

	// 11. Translate FSM-recorded request status.
	st, found := l.fsm.LookupRotationRequestStatus(requestID)
	if !found {
		return nil
	}
	switch st {
	case RotationStatusApplied:
		return nil
	case RotationStatusStaleNoOp:
		return errors.New("KEKPrune: stale voter set (voter_config_hash drift); retry")
	case RotationStatusRejected:
		return errors.New("KEKPrune: rejected by FSM")
	default:
		return fmt.Errorf("KEKPrune: unexpected FSM status %d", st)
	}
}

// ProposeKEKRetire runs the leader-side retire pipeline:
//
//  1. Confirm token == "delete-permanently-<version>".
//  2. Epoch guard + bounded 60s timeout.
//  3. Single-flight via the kek_lifecycle_in_flight mutex.
//  4. Pre-check: target version is loaded, not active, and not already
//     Retiring/Pruned (idempotent replay is the FSM's job; the propose path
//     rejects upstream so the operator sees a clear error).
//  5. Build MetaKEKRetireCmd (no peer probe, no DEK reseal, no payload cap).
//  6. Submit via raft and translate the FSM-recorded request status.
//
// Returns nil on FSM Applied. Returns ErrKEKRotateAnotherInFlight on concurrent
// attempt. Other failures surface a wrapped error.
func (l *KEKRotationLeader) ProposeKEKRetire(version uint32, confirm, actor string) error {
	expected := fmt.Sprintf("delete-permanently-%d", version)
	if confirm != expected {
		return fmt.Errorf("KEKRetire: bad confirm token (got %q, want %q)", confirm, expected)
	}

	ep := l.epochCtx.Load()
	if ep == nil {
		return errors.New("KEKRetire: not leader")
	}

	ctx, cancel := context.WithTimeout(ep.ctx, 60*time.Second)
	defer cancel()

	if !l.mu.TryLock() {
		return ErrKEKRotateAnotherInFlight
	}
	defer l.mu.Unlock()
	if l.onMutexAcquired != nil {
		l.onMutexAcquired()
	}

	active := l.fsm.ActiveKEKVersion()
	if version >= active {
		return fmt.Errorf("KEKRetire: version %d must be < active %d", version, active)
	}

	store := l.fsm.KEKStore()
	if store == nil {
		return errors.New("KEKRetire: keystore not wired")
	}
	if !store.HasVersion(version) {
		return fmt.Errorf("KEKRetire: version %d not in keystore", version)
	}
	if _, status, _, ok := l.fsm.LookupKEKStatus(version); ok {
		if status == KEKLifecycleRetiring {
			return fmt.Errorf("KEKRetire: version %d already retiring", version)
		}
		if status == KEKLifecyclePruned {
			return fmt.Errorf("KEKRetire: version %d already pruned", version)
		}
	}

	uuidV7, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("KEKRetire: request id: %w", err)
	}
	var requestID [16]byte
	copy(requestID[:], uuidV7[:])

	currentWraps, _ := l.fsm.dekKeeper.VersionsAndActive()
	cmd := KEKRetireCmd{
		PayloadVersion:       currentKEKRetirePayloadVersion,
		Version:              version,
		Confirm:              confirm,
		Actor:                actor,
		RequestID:            requestID,
		RequestedAtUnixNanos: l.wallClock().UnixNano(),
		ClusterStateAtPropose: ClusterStateAtPropose{
			ActiveKEKVersion: active,
			RetainedKEKCount: uint32(len(store.Versions())),
			LiveDEKGenCount:  uint32(len(currentWraps)),
		},
	}

	payload, err := EncodeMetaKEKRetireCmd(cmd)
	if err != nil {
		return fmt.Errorf("KEKRetire: encode: %w", err)
	}

	if err := l.raft.Propose(ctx, MetaCmdTypeKEKRetire, payload); err != nil {
		if l.epochCtx.Load() == nil {
			return errors.New("KEKRetire: not leader (lost leadership mid-retire)")
		}
		return fmt.Errorf("KEKRetire: submit: %w", err)
	}

	st, found := l.fsm.LookupRotationRequestStatus(requestID)
	if !found {
		return nil
	}
	switch st {
	case RotationStatusApplied:
		return nil
	case RotationStatusStaleNoOp:
		return errors.New("KEKRetire: stale cluster state (active KEK drifted); retry")
	case RotationStatusRejected:
		return errors.New("KEKRetire: rejected by FSM")
	default:
		return fmt.Errorf("KEKRetire: unexpected FSM status %d", st)
	}
}

// liveDEKGenCount returns the number of live DEK generations or 0 if the
// keeper is not wired. Helper to keep the audit struct compact.
func (l *KEKRotationLeader) liveDEKGenCount() int {
	if l.fsm == nil || l.fsm.dekKeeper == nil {
		return 0
	}
	wraps, _ := l.fsm.dekKeeper.VersionsAndActive()
	return len(wraps)
}
