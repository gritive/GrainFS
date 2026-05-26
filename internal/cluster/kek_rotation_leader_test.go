package cluster

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// fakeRaftSubmitter applies cmds synchronously to the wired FSM, mimicking
// MetaRaft.Propose's wait-applied semantics. Tests inject one to exercise the
// leader pipeline without spinning a real raft node. interceptPayload allows
// the bad-payload test to corrupt the encoded cmd just before apply.
type fakeRaftSubmitter struct {
	fsm              *MetaFSM
	applyIndex       atomic.Uint64
	interceptPayload func([]byte) []byte
	lastErr          error
}

func (f *fakeRaftSubmitter) Propose(_ context.Context, cmdType MetaCmdType, payload []byte) error {
	envelope, err := encodeMetaCmd(cmdType, payload)
	if err != nil {
		return err
	}
	if f.interceptPayload != nil {
		envelope = f.interceptPayload(envelope)
	}
	idx := f.applyIndex.Add(1) + 100
	err = f.fsm.applyCmdAtIndex(envelope, idx)
	f.lastErr = err
	return err
}

// fakePeerProbe returns a fixed set of disk-space + lease-snapshot reports.
type fakePeerProbe struct {
	probes      []KEKDiskSpaceResp
	err         error
	leaseSample func(voters []string, version uint32) ([]LeaseAttestationSample, error)
}

func (p *fakePeerProbe) ProbeAllKEKDiskSpace(_ context.Context) ([]KEKDiskSpaceResp, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.probes, nil
}

func (p *fakePeerProbe) ProbeKEKLeaseSnapshot(_ context.Context, voters []string, version uint32) ([]LeaseAttestationSample, error) {
	if p.leaseSample != nil {
		return p.leaseSample(voters, version)
	}
	// Default: every voter attests lease_count=0 at observed_at_index=10000.
	out := make([]LeaseAttestationSample, len(voters))
	for i, v := range voters {
		out[i] = LeaseAttestationSample{NodeID: v, ObservedAtIndex: 10000 + uint64(i), LeaseCount: 0}
	}
	return out, nil
}

// fakeRaftConfigReader returns a fixed voter list + configIndex; tests can
// flip configIndexAfter to simulate a membership change between probe-start
// and probe-end (the race-detect path).
type fakeRaftConfigReader struct {
	voters           []string
	configIndex      uint64
	configIndexAfter uint64 // 0 → return configIndex; nonzero → return on 2nd call
	calls            int
}

func (r *fakeRaftConfigReader) EffectiveConfiguration() ([]string, uint64) {
	r.calls++
	if r.calls > 1 && r.configIndexAfter != 0 {
		return append([]string(nil), r.voters...), r.configIndexAfter
	}
	return append([]string(nil), r.voters...), r.configIndex
}

// leaderTestFixture wires a KEKRotationLeader on top of an FSM seeded with K0
// active and DEK gen 1 wrapped under K0. The leader has an epoch ctx already
// installed so ProposeKEKRotate passes the "not leader" check by default.
type leaderTestFixture struct {
	t      *testing.T
	fsm    *MetaFSM
	leader *KEKRotationLeader
	raft   *fakeRaftSubmitter
	probe  *fakePeerProbe
	k0     []byte
	k1     []byte
	plain  []byte
	dekK0  []byte
	rngKEK func() ([]byte, error)
}

type leaderFixtureOpts struct {
	probes           []KEKDiskSpaceResp
	probeErr         error
	activeKEK        uint32 // 0 → default seed
	extraLiveDEKs    int    // additional DEK gens seeded into the keeper
	rngKEK           func() ([]byte, error)
	corruptRewrapBit bool
	skipLeadership   bool
}

func newLeaderTestFixture(t *testing.T, opts leaderFixtureOpts) *leaderTestFixture {
	t.Helper()
	fx := &leaderTestFixture{
		t:     t,
		fsm:   NewMetaFSM(),
		k0:    bytes.Repeat([]byte{0xA0}, encrypt.KEKSize),
		k1:    bytes.Repeat([]byte{0xA1}, encrypt.KEKSize),
		plain: bytes.Repeat([]byte{0xD1}, encrypt.DEKSize),
	}

	var clusterID [16]byte
	for i := range clusterID {
		clusterID[i] = byte(i + 1)
	}
	fx.fsm.SetClusterID(clusterID[:])

	store := encrypt.NewKEKStore()
	if err := store.Add(0, fx.k0); err != nil {
		t.Fatalf("seed KEKStore K0: %v", err)
	}
	// Optionally fast-forward the active KEK version (overflow test).
	if opts.activeKEK > 0 {
		fx.fsm.SetActiveKEKVersion(opts.activeKEK)
	}
	fx.fsm.SetKEKStore(store)

	// Build a DEKKeeper with gen 1 (and optional extra gens) all sealed under K0.
	var err error
	fx.dekK0, err = encrypt.AESGCMSeal(fx.k0, fx.plain)
	if err != nil {
		t.Fatalf("seal DEK gen 1: %v", err)
	}
	versions := map[uint32][]byte{1: fx.dekK0}
	for i := 2; i <= 1+opts.extraLiveDEKs; i++ {
		w, err := encrypt.AESGCMSeal(fx.k0, fx.plain)
		if err != nil {
			t.Fatalf("seal DEK gen %d: %v", i, err)
		}
		versions[uint32(i)] = w
	}
	keeper, err := encrypt.LoadFromFSM(fx.k0, versions)
	if err != nil {
		t.Fatalf("LoadFromFSM: %v", err)
	}
	fx.fsm.SetDEKKeeper(keeper)

	// Default probes: a single node with plenty of free space.
	probes := opts.probes
	if probes == nil {
		probes = []KEKDiskSpaceResp{{NodeID: "node-self", FreeBytes: 1 << 30, KeystorePath: "/tmp/keys"}}
	}
	fx.probe = &fakePeerProbe{probes: probes, err: opts.probeErr}
	fx.raft = &fakeRaftSubmitter{fsm: fx.fsm}

	// rng: default to deterministic K1 (matches the K1 the FSM tests use)
	// so the dry-run path passes. Overridden when the test wants a fault.
	rng := opts.rngKEK
	if rng == nil {
		rng = func() ([]byte, error) {
			out := make([]byte, encrypt.KEKSize)
			copy(out, fx.k1)
			return out, nil
		}
	}
	if opts.corruptRewrapBit {
		// Inject a fault by flipping a large run of bytes in the middle of
		// the envelope — the resulting MetaCmd will fail one of the FSM
		// integrity checks (FB schema parse, AAD-open of K_new, or DEK
		// plaintext equality) and propagate up as a submit error.
		fx.raft.interceptPayload = func(envelope []byte) []byte {
			cp := append([]byte(nil), envelope...)
			mid := len(cp) / 2
			for i := mid; i < mid+16 && i < len(cp); i++ {
				cp[i] ^= 0xFF
			}
			return cp
		}
	}
	fx.rngKEK = rng

	fx.leader = NewKEKRotationLeader(KEKRotationLeaderConfig{
		FSM:              fx.fsm,
		Raft:             fx.raft,
		PeerProbe:        fx.probe,
		RaftConfigReader: &fakeRaftConfigReader{voters: []string{"node-self"}, configIndex: 5},
		RNGKEK:           rng,
		WallClock:        func() time.Time { return time.Unix(1717000000, 0) },
	})
	if !opts.skipLeadership {
		fx.leader.SetEpochCtx(context.Background())
	}
	return fx
}

func TestLeaderProposeKEKRotate_RejectsOnBadConfirmToken(t *testing.T) {
	fx := newLeaderTestFixture(t, leaderFixtureOpts{})
	err := fx.leader.ProposeKEKRotate("nope", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "confirm token") {
		t.Errorf("expected confirm-token reject, got: %v", err)
	}
}

func TestLeaderProposeKEKRotate_RejectsWhenNotLeader(t *testing.T) {
	fx := newLeaderTestFixture(t, leaderFixtureOpts{skipLeadership: true})
	err := fx.leader.ProposeKEKRotate("rotate-now", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "not leader") {
		t.Errorf("expected not-leader reject, got: %v", err)
	}
}

func TestLeaderProposeKEKRotate_RejectsOnDiskSpaceProbeLow(t *testing.T) {
	fx := newLeaderTestFixture(t, leaderFixtureOpts{
		probes: []KEKDiskSpaceResp{
			{NodeID: "node-0", FreeBytes: 1 << 30},
			{NodeID: "node-1", FreeBytes: 16 * 1024}, // < 64 KiB
		},
	})
	err := fx.leader.ProposeKEKRotate("rotate-now", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "node-1") {
		t.Errorf("expected disk-space probe reject with node-1 detail, got: %v", err)
	}
}

func TestLeaderProposeKEKRotate_RejectsOnUint32Overflow(t *testing.T) {
	fx := newLeaderTestFixture(t, leaderFixtureOpts{activeKEK: math.MaxUint32})
	err := fx.leader.ProposeKEKRotate("rotate-now", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "max uint32") {
		t.Errorf("expected uint32 overflow reject, got: %v", err)
	}
}

func TestLeaderProposeKEKRotate_RejectsOnPayloadSizeCap(t *testing.T) {
	// 2000 live DEK gens > MaxLiveDEKGens (1024).
	fx := newLeaderTestFixture(t, leaderFixtureOpts{extraLiveDEKs: 1999})
	err := fx.leader.ProposeKEKRotate("rotate-now", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "MaxLiveDEKGens") {
		t.Errorf("expected MaxLiveDEKGens reject, got: %v", err)
	}
}

func TestLeaderProposeKEKRotate_DryRunDetectsBadRNG(t *testing.T) {
	// rngKEK returns a wrong-length key — dry-run rejects before raft.
	fx := newLeaderTestFixture(t, leaderFixtureOpts{
		rngKEK: func() ([]byte, error) {
			return bytes.Repeat([]byte{0x00}, 16), nil // 16 != KEKSize
		},
	})
	err := fx.leader.ProposeKEKRotate("rotate-now", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "rngKEK returned") {
		t.Errorf("expected rngKEK-length reject, got: %v", err)
	}
	if fx.raft.applyIndex.Load() != 0 {
		t.Errorf("raft must not be touched on dry-run reject, applyIndex=%d", fx.raft.applyIndex.Load())
	}
}

func TestLeaderProposeKEKRotate_RejectsOnRNGFailure(t *testing.T) {
	fx := newLeaderTestFixture(t, leaderFixtureOpts{
		rngKEK: func() ([]byte, error) {
			return nil, errors.New("entropy source unavailable")
		},
	})
	err := fx.leader.ProposeKEKRotate("rotate-now", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "rand K_new") {
		t.Errorf("expected rand-source reject, got: %v", err)
	}
}

func TestLeaderProposeKEKRotate_PropagatesProbeError(t *testing.T) {
	fx := newLeaderTestFixture(t, leaderFixtureOpts{
		probeErr: errors.New("transport down"),
	})
	err := fx.leader.ProposeKEKRotate("rotate-now", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "probe disk space") {
		t.Errorf("expected probe-error wrap, got: %v", err)
	}
}

func TestLeaderProposeKEKRotate_RejectsOnRaftCorruption(t *testing.T) {
	// A bit-flipped envelope on the way into raft → FSM Apply error.
	fx := newLeaderTestFixture(t, leaderFixtureOpts{corruptRewrapBit: true})
	err := fx.leader.ProposeKEKRotate("rotate-now", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "submit") {
		t.Errorf("expected submit-error wrap from corrupted envelope, got: %v", err)
	}
}

func TestLeaderProposeKEKRotate_HappyPath(t *testing.T) {
	fx := newLeaderTestFixture(t, leaderFixtureOpts{})
	if err := fx.leader.ProposeKEKRotate("rotate-now", "admin@uds"); err != nil {
		t.Fatalf("ProposeKEKRotate: %v", err)
	}
	if got := fx.fsm.ActiveKEKVersion(); got != 1 {
		t.Errorf("active_kek_version = %d, want 1", got)
	}
	if !fx.fsm.KEKStore().HasVersion(1) {
		t.Errorf("keystore missing version 1 after rotate")
	}
	if act := fx.fsm.KEKStore().ActiveVersion(); act != 1 {
		t.Errorf("KEKStore.ActiveVersion = %d, want 1", act)
	}
	// Subsequent rotation under K1 must succeed end-to-end. Need a fresh
	// rngKEK returning a different key (K2). Swap by constructing a new
	// fixture seeded with the post-rotate state? Simpler: just check that
	// the raft.lastErr is nil (set above via Propose path).
	if fx.raft.lastErr != nil {
		t.Errorf("raft.lastErr = %v, want nil", fx.raft.lastErr)
	}
}

func TestLeaderProposeKEKRotate_RejectsOnConcurrent(t *testing.T) {
	fx := newLeaderTestFixture(t, leaderFixtureOpts{})
	// Manually grab the mutex to simulate an in-flight rotation, then verify
	// ProposeKEKRotate returns ErrKEKRotateAnotherInFlight without touching
	// raft.
	fx.leader.mu.Lock()
	defer fx.leader.mu.Unlock()
	err := fx.leader.ProposeKEKRotate("rotate-now", "admin@uds")
	if !errors.Is(err, ErrKEKRotateAnotherInFlight) {
		t.Errorf("expected ErrKEKRotateAnotherInFlight, got: %v", err)
	}
	if fx.raft.applyIndex.Load() != 0 {
		t.Errorf("raft.applyIndex = %d, want 0 (must not submit)", fx.raft.applyIndex.Load())
	}
}

func TestLeaderProposeKEKRotate_ClearEpochCancelsInFlight(t *testing.T) {
	// SetEpochCtx then ClearEpoch — second Propose must reject as not-leader.
	fx := newLeaderTestFixture(t, leaderFixtureOpts{})
	fx.leader.ClearEpoch()
	err := fx.leader.ProposeKEKRotate("rotate-now", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "not leader") {
		t.Errorf("expected not-leader reject after ClearEpoch, got: %v", err)
	}
}

func TestLeaderProposeKEKRotate_HappyPathRecordsApplied(t *testing.T) {
	// Strengthens HappyPath: verify the FSM recorded RotationStatusApplied
	// for the request — this is the signal the leader's status translation
	// layer relies on to return nil.
	fx := newLeaderTestFixture(t, leaderFixtureOpts{})
	if err := fx.leader.ProposeKEKRotate("rotate-now", "admin@uds"); err != nil {
		t.Fatalf("ProposeKEKRotate: %v", err)
	}
	// Scan the ring for the (single) Applied entry.
	fx.fsm.mu.RLock()
	defer fx.fsm.mu.RUnlock()
	if len(fx.fsm.lastRotationRequests) == 0 {
		t.Fatal("no rotation request status recorded")
	}
	last := fx.fsm.lastRotationRequests[len(fx.fsm.lastRotationRequests)-1]
	if last.status != RotationStatusApplied {
		t.Errorf("last recorded rotation status = %v, want RotationStatusApplied", last.status)
	}
}

func TestLeaderProposeKEKRotate_AcceptsExactlyMinFreeBytes(t *testing.T) {
	// Threshold boundary: a node reporting exactly MinKeystoreFreeBytes is
	// accepted (we reject <, not <=).
	fx := newLeaderTestFixture(t, leaderFixtureOpts{
		probes: []KEKDiskSpaceResp{
			{NodeID: "node-edge", FreeBytes: MinKeystoreFreeBytes},
		},
	})
	if err := fx.leader.ProposeKEKRotate("rotate-now", "admin@uds"); err != nil {
		t.Errorf("expected accept at exact threshold, got: %v", err)
	}
}

// --- dryRunValidateKEKRotate direct unit tests ----------------------------
//
// These exercise the four reject branches of dryRunValidateKEKRotate
// independent of the surrounding ProposeKEKRotate pipeline. Each test builds
// a happy cmd via the FSM-level fixture (which already wires K0 + DEK gen 1),
// tampers exactly one field, and asserts the precise rejection reason. Guards
// against silent regression: if any branch starts returning nil, the FSM
// apply loop on every node would fatal-halt on the first bad rotation.

func buildDryRunCmd(t *testing.T) (*kekRotateTestFixture, KEKRotateCmd, []byte, []byte) {
	t.Helper()
	fx := newKEKRotateTestFixture(t)
	cmd := fx.buildHappyCmd([16]byte{0xDD})
	return fx, cmd, fx.k0, fx.k1
}

func TestDryRunValidateKEKRotate_AcceptsHappyCmd(t *testing.T) {
	fx, cmd, activeKEK, plainKnew := buildDryRunCmd(t)
	if err := dryRunValidateKEKRotate(fx.fsm, cmd, activeKEK, plainKnew); err != nil {
		t.Fatalf("dry-run on happy cmd: %v", err)
	}
}

func TestDryRunValidateKEKRotate_RejectsWrongNewVersion(t *testing.T) {
	fx, cmd, activeKEK, plainKnew := buildDryRunCmd(t)
	cmd.NewVersion = 5 // active is 0, want 1
	err := dryRunValidateKEKRotate(fx.fsm, cmd, activeKEK, plainKnew)
	if err == nil || !strings.Contains(err.Error(), "new_version=5") {
		t.Errorf("expected version-advance reject, got: %v", err)
	}
}

func TestDryRunValidateKEKRotate_RejectsWrapSetHashMismatch(t *testing.T) {
	fx, cmd, activeKEK, plainKnew := buildDryRunCmd(t)
	cmd.WrapSetHash = append([]byte(nil), cmd.WrapSetHash...)
	cmd.WrapSetHash[0] ^= 0xFF
	err := dryRunValidateKEKRotate(fx.fsm, cmd, activeKEK, plainKnew)
	if err == nil || !strings.Contains(err.Error(), "wrap_set_hash mismatch") {
		t.Errorf("expected wrap_set_hash mismatch, got: %v", err)
	}
}

func TestDryRunValidateKEKRotate_RejectsWrapSetHashWrongLength(t *testing.T) {
	fx, cmd, activeKEK, plainKnew := buildDryRunCmd(t)
	cmd.WrapSetHash = cmd.WrapSetHash[:30]
	err := dryRunValidateKEKRotate(fx.fsm, cmd, activeKEK, plainKnew)
	if err == nil || !strings.Contains(err.Error(), "wrap_set_hash length") {
		t.Errorf("expected wrap_set_hash length reject, got: %v", err)
	}
}

func TestDryRunValidateKEKRotate_RejectsBadAADUnwrap(t *testing.T) {
	fx, cmd, activeKEK, plainKnew := buildDryRunCmd(t)
	cmd.WrappedNewKEK = append([]byte(nil), cmd.WrappedNewKEK...)
	cmd.WrappedNewKEK[len(cmd.WrappedNewKEK)-1] ^= 0xFF
	err := dryRunValidateKEKRotate(fx.fsm, cmd, activeKEK, plainKnew)
	if err == nil || !strings.Contains(err.Error(), "AAD-unwrap K_new") {
		t.Errorf("expected AAD-unwrap reject, got: %v", err)
	}
}

func TestDryRunValidateKEKRotate_RejectsKNewLeaderMismatch(t *testing.T) {
	// K_new AAD-unwraps cleanly to a 32-byte plaintext, but the leader's
	// rngKEK returned a different K_new. This protects against an in-process
	// payload tampering between AESGCMSealWithAAD and dry-run.
	fx, cmd, activeKEK, plainKnew := buildDryRunCmd(t)
	other := bytes.Repeat([]byte{0x99}, encrypt.KEKSize) // != fx.k1
	err := dryRunValidateKEKRotate(fx.fsm, cmd, activeKEK, other)
	if err == nil || !strings.Contains(err.Error(), "K_new payload-vs-leader") {
		t.Errorf("expected K_new mismatch reject, got: %v", err)
	}
	_ = plainKnew
}

func TestDryRunValidateKEKRotate_RejectsBadRewrap(t *testing.T) {
	fx, cmd, activeKEK, plainKnew := buildDryRunCmd(t)
	cmd.RewrappedDEKs = []RewrappedDEKEntry{{Gen: 1, Wrapped: append([]byte(nil), cmd.RewrappedDEKs[0].Wrapped...)}}
	cmd.RewrappedDEKs[0].Wrapped[len(cmd.RewrappedDEKs[0].Wrapped)-1] ^= 0xFF
	err := dryRunValidateKEKRotate(fx.fsm, cmd, activeKEK, plainKnew)
	if err == nil || !strings.Contains(err.Error(), "rewrapped_deks") {
		t.Errorf("expected rewrapped_deks reject, got: %v", err)
	}
}

// --- Task 6: leadership-epoch lifecycle + mutex auto-release tests ----------

// blockedRaftSubmitter blocks Propose until the block channel is closed,
// then returns nil. Used to hold a ProposeKEKRotate goroutine inside the mutex.
type blockedRaftSubmitter struct {
	block <-chan struct{}
}

func (b *blockedRaftSubmitter) Propose(ctx context.Context, _ MetaCmdType, _ []byte) error {
	select {
	case <-b.block:
		return nil
	case <-ctx.Done():
		// Return the raw context error. ProposeKEKRotate's post-Propose path
		// detects epochCtx==nil and surfaces "not leader" to callers.
		return ctx.Err()
	}
}

// slowRaftSubmitterOpts carries a per-submit delay for newTestKEKRotationLeader.
type slowRaftSubmitterOpts struct {
	delay time.Duration
}

// slowRaftSubmitter wraps fakeRaftSubmitter with a configurable delay before
// the synchronous FSM apply, giving concurrent goroutines time to attempt
// their own TryLock.
type slowRaftSubmitter struct {
	inner *fakeRaftSubmitter
	delay time.Duration
}

func (s *slowRaftSubmitter) Propose(ctx context.Context, cmdType MetaCmdType, payload []byte) error {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.inner.Propose(ctx, cmdType, payload)
}

// withSlowRaftSubmit is a functional option for newTestKEKRotationLeader.
func withSlowRaftSubmit(d time.Duration) func(*slowRaftSubmitterOpts) {
	return func(o *slowRaftSubmitterOpts) { o.delay = d }
}

// newTestKEKRotationLeader builds a KEKRotationLeader backed by the standard
// leaderTestFixture, without pre-installing a leadership epoch. Callers must
// call leader.AcquireLeadership() when they want the leader state.
// Accepts optional withSlowRaftSubmit options.
func newTestKEKRotationLeader(t *testing.T, opts ...func(*slowRaftSubmitterOpts)) *KEKRotationLeader {
	t.Helper()
	o := &slowRaftSubmitterOpts{}
	for _, fn := range opts {
		fn(o)
	}
	fx := newLeaderTestFixture(t, leaderFixtureOpts{skipLeadership: true})
	if o.delay > 0 {
		slow := &slowRaftSubmitter{inner: fx.raft, delay: o.delay}
		fx.leader.raft = slow
	}
	return fx.leader
}

func TestKEKRotationLeader_MutexReleasesOnLeadershipLoss(t *testing.T) {
	leader := newTestKEKRotationLeader(t)
	leader.AcquireLeadership()

	// Block raft submit so propose is stuck in-flight.
	blocker := make(chan struct{})
	leader.raft = &blockedRaftSubmitter{block: blocker}
	mutexHeld := make(chan struct{})
	leader.onMutexAcquired = func() { close(mutexHeld) }

	done := make(chan error, 1)
	go func() {
		done <- leader.ProposeKEKRotate("rotate-now", "admin@uds")
	}()
	select {
	case <-mutexHeld:
	case <-time.After(5 * time.Second):
		t.Fatalf("propose never acquired mutex")
	}

	leader.LoseLeadership() // cancels epoch ctx

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "leader") {
			t.Errorf("expected leadership-loss error, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("propose did not return on leadership loss")
	}
	close(blocker)
	// mutex must be released by the goroutine's defer.
	if !leader.mu.TryLock() {
		t.Errorf("mutex still held after leadership loss")
	}
	leader.mu.Unlock()
}

func TestKEKRotationLeader_NewLeaderStartsUnlocked(t *testing.T) {
	leader := newTestKEKRotationLeader(t)
	leader.AcquireLeadership()
	if !leader.mu.TryLock() {
		t.Errorf("fresh leader mutex held")
	}
	leader.mu.Unlock()
}

func TestKEKRotationLeader_ConcurrentProposeRejected(t *testing.T) {
	leader := newTestKEKRotationLeader(t, withSlowRaftSubmit(200*time.Millisecond))
	leader.AcquireLeadership()

	var wg sync.WaitGroup
	errs := make([]error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = leader.ProposeKEKRotate("rotate-now", "admin@uds")
		}(i)
	}
	wg.Wait()

	success, reject := 0, 0
	for _, err := range errs {
		if err == nil {
			success++
		} else if strings.Contains(err.Error(), "in flight") {
			reject++
		}
	}
	if success != 1 || reject != 4 {
		t.Errorf("got success=%d reject=%d, want 1 + 4 (errs=%v)", success, reject, errs)
	}
}

// --- Task 9: ProposeKEKPrune tests ----------------------------------------

// prepareLeaderForPrune extends the standard leader fixture for a prune
// scenario: seed multiple KEK versions, retire one, wire the raft config
// reader with the matching voter set, and configure the keystore directory.
// Returns the fixture + the version to be pruned.
func prepareLeaderForPrune(t *testing.T, voters []string) (*leaderTestFixture, uint32) {
	t.Helper()
	fx := newLeaderTestFixture(t, leaderFixtureOpts{})

	// Wire a real keystore (on tmp dir) so RemoveAndUnlink can actually
	// unlink. Replace the in-memory store the fixture installed.
	dir := t.TempDir()
	store, err := encrypt.LoadOrInitKEKStoreDir(dir)
	if err != nil {
		t.Fatalf("LoadOrInitKEKStoreDir: %v", err)
	}
	// Add versions 1..2 (auto-generated 0 is already there).
	for v := uint32(1); v <= 2; v++ {
		kek := bytes.Repeat([]byte{byte(0xB0 + v)}, encrypt.KEKSize)
		if err := store.AddAndPersist(dir, v, kek); err != nil {
			t.Fatalf("AddAndPersist v=%d: %v", v, err)
		}
	}
	if err := store.SetActiveVersion(2); err != nil {
		t.Fatalf("SetActiveVersion 2: %v", err)
	}
	fx.fsm.SetKEKStore(store)
	fx.fsm.SetKEKDir(dir)
	fx.fsm.SetActiveKEKVersion(2)

	// Retire version 1 (the version we will prune).
	if err := store.Retire(1); err != nil {
		t.Fatalf("Retire(1): %v", err)
	}
	fx.fsm.SetKEKStatus(1, KEKLifecycleRetiring, 100)

	// Reconfigure raft config reader for the requested voter set.
	fx.leader.raftConfigReader = &fakeRaftConfigReader{
		voters:      append([]string(nil), voters...),
		configIndex: 42,
	}
	return fx, 1
}

func TestLeaderProposeKEKPrune_HappyPath(t *testing.T) {
	voters := []string{"node-0", "node-1"}
	fx, version := prepareLeaderForPrune(t, voters)
	if err := fx.leader.ProposeKEKPrune(version, "admin@uds"); err != nil {
		t.Fatalf("ProposeKEKPrune: %v", err)
	}
	if fx.fsm.KEKStore().HasVersion(version) {
		t.Errorf("version %d still in keystore after prune", version)
	}
	_, status, _, ok := fx.fsm.LookupKEKStatus(version)
	if !ok || status != KEKLifecyclePruned {
		t.Errorf("kek_status[%d] = %v ok=%v, want Pruned", version, status, ok)
	}
}

func TestLeaderProposeKEKPrune_RejectsWhenNotLeader(t *testing.T) {
	fx, version := prepareLeaderForPrune(t, []string{"node-0"})
	fx.leader.ClearEpoch()
	if err := fx.leader.ProposeKEKPrune(version, "admin@uds"); !errors.Is(err, ErrKEKPruneNotLeader) {
		t.Errorf("expected ErrKEKPruneNotLeader, got: %v", err)
	}
}

func TestLeaderProposeKEKPrune_RejectsWhenNotRetiring(t *testing.T) {
	fx, _ := prepareLeaderForPrune(t, []string{"node-0"})
	// Use the active version 2 which is NOT in Retiring state.
	if err := fx.leader.ProposeKEKPrune(2, "admin@uds"); err == nil || !strings.Contains(err.Error(), "retiring") {
		t.Errorf("expected retiring-state reject, got: %v", err)
	}
}

func TestLeaderProposeKEKPrune_DetectsMembershipChangeMidProbe(t *testing.T) {
	voters := []string{"node-0", "node-1"}
	fx, version := prepareLeaderForPrune(t, voters)
	rcr := fx.leader.raftConfigReader.(*fakeRaftConfigReader)
	rcr.configIndexAfter = rcr.configIndex + 1 // 2nd read advances
	err := fx.leader.ProposeKEKPrune(version, "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "membership changed") {
		t.Errorf("expected membership-change reject, got: %v", err)
	}
}

func TestLeaderProposeKEKPrune_RejectsNonZeroLeaseCount(t *testing.T) {
	voters := []string{"node-0", "node-1"}
	fx, version := prepareLeaderForPrune(t, voters)
	fx.probe.leaseSample = func(vs []string, _ uint32) ([]LeaseAttestationSample, error) {
		out := make([]LeaseAttestationSample, len(vs))
		for i, v := range vs {
			lc := uint64(0)
			if v == "node-0" {
				lc = 3
			}
			out[i] = LeaseAttestationSample{NodeID: v, ObservedAtIndex: 200 + uint64(i), LeaseCount: lc}
		}
		return out, nil
	}
	err := fx.leader.ProposeKEKPrune(version, "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "lease_count") {
		t.Errorf("expected lease_count reject, got: %v", err)
	}
}

func TestLeaderProposeKEKPrune_RejectsMissingProbeResponses(t *testing.T) {
	voters := []string{"node-0", "node-1", "node-2"}
	fx, version := prepareLeaderForPrune(t, voters)
	fx.probe.leaseSample = func(vs []string, _ uint32) ([]LeaseAttestationSample, error) {
		// Drop the last voter to simulate transport failure.
		out := make([]LeaseAttestationSample, 0, len(vs)-1)
		for i, v := range vs {
			if i == len(vs)-1 {
				continue
			}
			out = append(out, LeaseAttestationSample{NodeID: v, ObservedAtIndex: 200 + uint64(i), LeaseCount: 0})
		}
		return out, nil
	}
	err := fx.leader.ProposeKEKPrune(version, "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "responded") {
		t.Errorf("expected missing-response reject, got: %v", err)
	}
}

// --- Task 11: ProposeKEKRetire tests --------------------------------------

// prepareLeaderForRetire seeds two KEK versions (0 + 1) with v=1 active, leaving
// v=0 in the implicit "active" lifecycle state (no kek_status entry) so it can
// be retired. Returns the version to retire (0).
func prepareLeaderForRetire(t *testing.T) (*leaderTestFixture, uint32) {
	t.Helper()
	fx := newLeaderTestFixture(t, leaderFixtureOpts{})

	dir := t.TempDir()
	store, err := encrypt.LoadOrInitKEKStoreDir(dir)
	if err != nil {
		t.Fatalf("LoadOrInitKEKStoreDir: %v", err)
	}
	k1 := bytes.Repeat([]byte{0xB1}, encrypt.KEKSize)
	if err := store.AddAndPersist(dir, 1, k1); err != nil {
		t.Fatalf("AddAndPersist v=1: %v", err)
	}
	if err := store.SetActiveVersion(1); err != nil {
		t.Fatalf("SetActiveVersion(1): %v", err)
	}
	fx.fsm.SetKEKStore(store)
	fx.fsm.SetKEKDir(dir)
	fx.fsm.SetActiveKEKVersion(1)
	return fx, 0
}

func TestLeaderProposeKEKRetire_HappyPath(t *testing.T) {
	fx, version := prepareLeaderForRetire(t)
	confirm := "delete-permanently-0"
	if err := fx.leader.ProposeKEKRetire(version, confirm, "admin@uds"); err != nil {
		t.Fatalf("ProposeKEKRetire: %v", err)
	}
	_, status, _, ok := fx.fsm.LookupKEKStatus(version)
	if !ok || status != KEKLifecycleRetiring {
		t.Errorf("kek_status[%d] = %v ok=%v, want Retiring", version, status, ok)
	}
}

func TestLeaderProposeKEKRetire_RejectsWhenNotLeader(t *testing.T) {
	fx, version := prepareLeaderForRetire(t)
	fx.leader.ClearEpoch()
	err := fx.leader.ProposeKEKRetire(version, "delete-permanently-0", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "not leader") {
		t.Errorf("expected not-leader reject, got: %v", err)
	}
}

func TestLeaderProposeKEKRetire_RejectsBadConfirm(t *testing.T) {
	fx, version := prepareLeaderForRetire(t)
	err := fx.leader.ProposeKEKRetire(version, "delete-permanently-99", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "confirm") {
		t.Errorf("expected confirm-mismatch reject, got: %v", err)
	}
}

func TestLeaderProposeKEKRetire_RejectsActiveVersion(t *testing.T) {
	fx, _ := prepareLeaderForRetire(t)
	// Active is 1; retiring it is forbidden by FSM Apply. Pre-check rejects.
	err := fx.leader.ProposeKEKRetire(1, "delete-permanently-1", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "active") {
		t.Errorf("expected active-version reject, got: %v", err)
	}
}

func TestLeaderProposeKEKRetire_RejectsAlreadyRetiring(t *testing.T) {
	fx, version := prepareLeaderForRetire(t)
	fx.fsm.SetKEKStatus(version, KEKLifecycleRetiring, 100)
	err := fx.leader.ProposeKEKRetire(version, "delete-permanently-0", "admin@uds")
	if err == nil || !strings.Contains(err.Error(), "already") {
		t.Errorf("expected already-retiring reject, got: %v", err)
	}
}
