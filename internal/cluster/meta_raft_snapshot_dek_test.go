package cluster

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

// snapDEKTestClusterID is the deterministic 16-byte clusterID used by the
// live-snapshot DEK install fixtures. DEK wraps are AAD-bound to
// (clusterID, gen, kekVer), so the same clusterID must be staged on both the
// source FSM (that produces the snapshot) and the restore target.
func snapDEKTestClusterID() []byte {
	id := make([]byte, 16)
	for i := range id {
		id[i] = byte(i + 1)
	}
	return id
}

// keeperWithGens returns a DEKKeeper holding freshly generated DEK
// generations 0..n sealed under kek (KEK version 0), active gen = n.
func keeperWithGens(t *testing.T, kek, clusterID []byte, n uint32) *encrypt.DEKKeeper {
	t.Helper()
	keeper, err := encrypt.NewEmptyDEKKeeper(kek, clusterID)
	require.NoError(t, err, "NewEmptyDEKKeeper")
	for gen := uint32(0); gen <= n; gen++ {
		w, kv, err := keeper.GenerateWrappedDEK(gen)
		require.NoError(t, err, "GenerateWrappedDEK gen=%d", gen)
		require.NoError(t, keeper.InstallReplicatedDEK(gen, w, kv), "InstallReplicatedDEK gen=%d", gen)
	}
	return keeper
}

// snapshotFromKeeper produces a genuine MetaFSM snapshot (via Snapshot()) whose
// DKVS trailer carries the given keeper's wrapped DEKs. The returned bytes are
// what a follower would receive over an InstallSnapshot RPC. Using a single
// keeper for both the snapshot and any sealed payload guarantees the gen-N DEK
// material matches (GenerateWrappedDEK produces random per-gen material, so two
// independent keepers would NOT share gen-N keys).
func snapshotFromKeeper(t *testing.T, keeper *encrypt.DEKKeeper, kek, clusterID []byte) []byte {
	t.Helper()

	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(0, kek), "KEKStore.Add(0)")

	src := NewMetaFSM()
	src.SetClusterID(clusterID)
	src.SetKEKStore(store)
	src.SetDEKKeeper(keeper)

	snap, err := src.Snapshot()
	require.NoError(t, err, "Snapshot")
	return snap
}

// newRandKEK returns a fresh random KEK of the correct size.
func newRandKEK(t *testing.T) []byte {
	t.Helper()
	kek := make([]byte, encrypt.KEKSize)
	_, err := rand.Read(kek)
	require.NoError(t, err)
	return kek
}

// staleKeeperFromSnap builds a "stale" follower keeper holding gens 0..upTo
// using the EXACT wrapped bytes the snapshot keeper holds for those gens. This
// mirrors the cluster invariant that every node installs the SAME replicated DEK
// bytes per gen (DEKReplicatedRotate is deterministic), so the snapshot's gen-0
// install onto this follower is an idempotent same-bytes no-op rather than a
// non-deterministic-bytes collision. active = upTo.
func staleKeeperFromSnap(t *testing.T, snapKeeper *encrypt.DEKKeeper, kek, clusterID []byte, upTo uint32) *encrypt.DEKKeeper {
	t.Helper()
	versions, _ := snapKeeper.VersionsAndActive()
	keeper, err := encrypt.NewEmptyDEKKeeper(kek, clusterID)
	require.NoError(t, err, "NewEmptyDEKKeeper")
	for gen := uint32(0); gen <= upTo; gen++ {
		w, ok := versions[gen]
		require.True(t, ok, "snapshot keeper must hold gen %d", gen)
		require.NoError(t, keeper.InstallReplicatedDEK(gen, w, 0), "InstallReplicatedDEK gen=%d", gen)
	}
	return keeper
}

// TestApplySnapshotEntry_InPlaceInstallPreservesIdentity is the key regression
// guard against the WRONG (object-replacement) design. A follower applies a live
// snapshot carrying DEK gen N (> the follower's current gen) plus an IAM
// credential AND object data both sealed under gen N. The live keeper and a
// data-plane adapter share the SAME keeper object (mirroring ShardService.segEnc
// captured once at boot). After applySnapshotEntry:
//
//	(a) f.dekKeeper POINTER is unchanged — the object-replacement design fails here;
//	(b) the data-plane adapter (sharing that object) decrypts gen-N object data;
//	(c) an IAM unseal under gen N succeeds.
//
// On master (no install) and on the first impl (new keeper object) at least one
// of these fails.
func TestApplySnapshotEntry_InPlaceInstallPreservesIdentity(t *testing.T) {
	t.Parallel()

	const targetGen = uint32(2)
	clusterID := snapDEKTestClusterID()
	kek := newRandKEK(t)

	// One keeper holding gens 0..N seals BOTH payloads and produces the snapshot,
	// so an in-place install of its wrapped bytes recovers the exact gen-N DEK.
	snapKeeper := keeperWithGens(t, kek, clusterID, targetGen)
	snapEnc := storage.NewDEKKeeperAdapter(snapKeeper, clusterID)

	const saID = "AKIASNAPDEK000000000"
	const accessKey = "AKIASNAPDEK000000000"
	const secret = "top-secret-credential-material"
	credCT, credGen, err := iam.WrapSecret(snapEnc, saID, accessKey, secret)
	require.NoError(t, err)
	require.Equal(t, targetGen, credGen, "credential must be sealed under active gen N")

	objPlain := []byte("object-payload-sealed-under-gen-N")
	objFields := []encrypt.AADField{encrypt.FieldString("bucket/object")}
	objCT, objGen, err := snapEnc.Seal(encrypt.DomainShard, objFields, objPlain)
	require.NoError(t, err)
	require.Equal(t, targetGen, objGen, "object data must be sealed under active gen N")

	snapBytes := snapshotFromKeeper(t, snapKeeper, kek, clusterID)

	// Stale live keeper: only gen 0 (same bytes as the snapshot's gen 0, per the
	// cluster determinism invariant). The DATA-PLANE adapter and the IAM applier
	// both wrap THIS object — exactly the shared-object posture the in-place
	// design must preserve.
	staleKeeper := staleKeeperFromSnap(t, snapKeeper, kek, clusterID, 0)

	fsm := NewMetaFSM()
	fsm.SetClusterID(clusterID)
	fsm.SetDEKKeeper(staleKeeper)
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(0, kek))
	fsm.SetKEKStore(store)

	// The data-plane adapter is fabricated in-test over the shared keeper. In
	// production ShardService.WithShardDEKKeeper captures f.dekKeeper once at boot
	// (exercised elsewhere); these tests prove the shared-keeper invariant — that
	// an in-place install reaches any adapter sharing the keeper object — not the
	// boot wiring itself.
	dataPlane := storage.NewDEKKeeperAdapter(staleKeeper, clusterID) // captured ONCE, never re-wired
	applier := iam.NewApplier(iam.NewStore(), storage.NewDEKKeeperAdapter(staleKeeper, clusterID))

	keeperBefore := fsm.DEKKeeper()

	m := &MetaRaft{fsm: fsm}
	halt := m.applySnapshotEntry(raft.SnapshotMeta{Index: 42}, snapBytes, 42)
	require.False(t, halt, "applySnapshotEntry must not halt on a valid snapshot")

	// (a) Object identity preserved — the WRONG design swaps in a new keeper here.
	require.Same(t, keeperBefore, fsm.DEKKeeper(),
		"in-place install must preserve f.dekKeeper object identity (object-replacement design fails)")

	// (b) Data plane: the adapter that was captured at boot decrypts gen-N data
	// with NO re-wiring, because it shares the in-place-mutated keeper.
	gotObj, err := dataPlane.Open(encrypt.DomainShard, objFields, objGen, objCT)
	require.NoError(t, err, "data-plane adapter must decrypt gen-N object data after in-place install")
	require.Equal(t, objPlain, gotObj)

	// (c) IAM unseal under gen N succeeds through the same shared keeper.
	gotSecret, err := iam.UnwrapSecret(applier.Encryptor(), saID, accessKey, credGen, credCT)
	require.NoError(t, err, "IAM unseal under gen N must succeed after in-place install")
	require.Equal(t, secret, gotSecret)
}

// TestApplySnapshotEntry_LaterRotationReachesDataPlane proves the in-place
// install keeps the SHARED keeper object tracking future rotations: after the
// snapshot install at gen N, a DEKReplicatedRotate to gen N+1 must be visible to
// the data-plane adapter captured at boot (the bug the first impl reintroduced —
// a swapped keeper leaves ShardService frozen).
func TestApplySnapshotEntry_LaterRotationReachesDataPlane(t *testing.T) {
	t.Parallel()

	const targetGen = uint32(1)
	clusterID := snapDEKTestClusterID()
	kek := newRandKEK(t)

	snapKeeper := keeperWithGens(t, kek, clusterID, targetGen)
	snapBytes := snapshotFromKeeper(t, snapKeeper, kek, clusterID)

	staleKeeper := staleKeeperFromSnap(t, snapKeeper, kek, clusterID, 0)

	fsm := NewMetaFSM()
	fsm.SetClusterID(clusterID)
	fsm.SetDEKKeeper(staleKeeper)
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(0, kek))
	fsm.SetKEKStore(store)

	dataPlane := storage.NewDEKKeeperAdapter(staleKeeper, clusterID) // captured ONCE

	m := &MetaRaft{fsm: fsm}
	require.False(t, m.applySnapshotEntry(raft.SnapshotMeta{Index: 5}, snapBytes, 5))
	require.Same(t, staleKeeper, fsm.DEKKeeper(), "identity preserved after install")

	// Live rotation to gen N+1. The leader generates the wrapped DEK; every node
	// installs the SAME bytes. Build a genuine DEKReplicatedRotate command and
	// apply it through the FSM's applier.
	const nextGen = targetGen + 1
	wrapped, kekVer, err := staleKeeper.GenerateWrappedDEK(nextGen)
	require.NoError(t, err)
	cmd, err := EncodeDEKReplicatedRotateCmd(DEKReplicatedRotateCmd{
		Gen:               nextGen,
		WrappedDEK:        wrapped,
		ExpectedActiveGen: targetGen, // current active after snapshot install
		ActiveKEKVer:      kekVer,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.applyDEKReplicatedRotate(99, cmd),
		"DEKReplicatedRotate to gen N+1 must apply (preconditions: active gen N + KEK ver match)")

	// Seal under the new active gen via the SHARED data-plane adapter and read it
	// back: proves the rotation reached the data plane with no manual re-wire.
	objFields := []encrypt.AADField{encrypt.FieldString("bucket/next")}
	ct, gen, err := dataPlane.Seal(encrypt.DomainShard, objFields, []byte("gen-N+1-payload"))
	require.NoError(t, err)
	require.Equal(t, uint32(nextGen), gen, "data-plane Seal must use the newly rotated active gen N+1")
	got, err := dataPlane.Open(encrypt.DomainShard, objFields, gen, ct)
	require.NoError(t, err)
	require.Equal(t, []byte("gen-N+1-payload"), got)
}

// TestApplySnapshotEntry_CrossKEKHistoricalFetch covers the cross-KEK
// historical-fetch branch of installSnapshotDEKs (the snapKEKVer !=
// keeper.ActiveKEKVersion() arm at meta_fsm_rotation.go). A follower whose LIVE
// keeper has already been KEK-rotated to active KEK v1 receives a snapshot whose
// DEK wraps were sealed under the OLDER KEK v0 (pendingActiveKEKVersion == 0).
// installSnapshotDEKs must fetch the historical KEK v0 from the keystore to
// authenticate the unwrap, install the gens, preserve keeper object identity, and
// leave a data-plane adapter over the shared keeper able to Open gen-N data.
//
// The follower keeper holds NO gens (empty-keeper-at-v1): a keeper actually
// holding v1-sealed wraps for gens 0..N would collide with the snapshot's
// v0-sealed bytes for the same gens (bytes mismatch → fatal halt), which would
// turn this into a halt test rather than a historical-fetch test. The branch
// depends only on the ActiveKEKVersion() mismatch, so empty-keeper-at-v1
// exercises it faithfully.
func TestApplySnapshotEntry_CrossKEKHistoricalFetch(t *testing.T) {
	t.Parallel()

	const targetGen = uint32(2)
	clusterID := snapDEKTestClusterID()
	kekV0 := newRandKEK(t)
	kekV1 := newRandKEK(t)

	// Snapshot keeper seals gens 0..N AND the object payload under KEK v0, and the
	// snapshot trailer records active_kek_version == 0 (the source FSM's default).
	snapKeeper := keeperWithGens(t, kekV0, clusterID, targetGen)
	snapEnc := storage.NewDEKKeeperAdapter(snapKeeper, clusterID)
	objPlain := []byte("cross-kek-object-payload")
	objFields := []encrypt.AADField{encrypt.FieldString("bucket/cross-kek")}
	objCT, objGen, err := snapEnc.Seal(encrypt.DomainShard, objFields, objPlain)
	require.NoError(t, err)
	require.Equal(t, targetGen, objGen, "object data must be sealed under active gen N")

	snapBytes := snapshotFromKeeper(t, snapKeeper, kekV0, clusterID)

	// Live follower keeper: rotated to active KEK v1, holding NO DEK gens yet.
	liveKeeper, err := encrypt.NewEmptyDEKKeeper(kekV1, clusterID)
	require.NoError(t, err)
	liveKeeper.SetActiveKEKVersion(1) // the seam that triggers the historical fetch

	fsm := NewMetaFSM()
	fsm.SetClusterID(clusterID)
	fsm.SetDEKKeeper(liveKeeper)

	// Keystore still holds the historical KEK v0 (and the active v1). The branch
	// fetches v0 to authenticate the v0-sealed snapshot wraps.
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(0, kekV0))
	require.NoError(t, store.Add(1, kekV1))
	fsm.SetKEKStore(store)

	dataPlane := storage.NewDEKKeeperAdapter(liveKeeper, clusterID) // captured ONCE

	keeperBefore := fsm.DEKKeeper()

	m := &MetaRaft{fsm: fsm}
	halt := m.applySnapshotEntry(raft.SnapshotMeta{Index: 77}, snapBytes, 77)
	require.False(t, halt, "cross-KEK historical-fetch install must succeed (not halt)")

	// Object identity preserved across the in-place install.
	require.Same(t, keeperBefore, fsm.DEKKeeper(),
		"cross-KEK install must preserve f.dekKeeper object identity")

	// The snapshot's active gen is now installed and is the keeper's active gen.
	require.Equal(t, targetGen, liveKeeper.ActiveDEKGeneration(),
		"snapshot active gen N must become the keeper's active gen after cross-KEK install")

	// Data-plane adapter over the shared keeper decrypts gen-N data sealed under
	// the historical KEK — proving the historical fetch authenticated the unwrap.
	gotObj, err := dataPlane.Open(encrypt.DomainShard, objFields, objGen, objCT)
	require.NoError(t, err, "data-plane adapter must Open gen-N data after cross-KEK historical-fetch install")
	require.Equal(t, objPlain, gotObj)
}

// TestApplySnapshotEntry_CrossKEKMissingHistoricalKEKHalts is the integration
// negative: a follower whose keystore does NOT hold the snapshot's KEK version
// must fatal-halt the apply loop and NOT advance lastApplied / lastSnapshotIndex.
//
// Reachability note: the halt actually fires at the Restore ENVELOPE gate
// (openSnapshotEnvelope → "resolve KEK v0", meta_fsm_snapshot_envelope.go), NOT
// at installSnapshotDEKs's keystore.Get. The snapshot envelope is sealed under
// the same KEK version the DKVS trailer records as active_kek_version, so Restore
// needs (and resolves) that KEK before installSnapshotDEKs is ever reached — its
// own keystore.Get can never be the first failure for an honestly produced
// snapshot. This test proves the real-world contract (missing historical KEK →
// safe halt, no silent divergence); installSnapshotDEKs's defensive keystore.Get
// error return is covered directly by the white-box test below.
func TestApplySnapshotEntry_CrossKEKMissingHistoricalKEKHalts(t *testing.T) {
	t.Parallel()

	const targetGen = uint32(1)
	clusterID := snapDEKTestClusterID()
	kekV0 := newRandKEK(t)
	kekV1 := newRandKEK(t)

	// Snapshot wraps sealed under KEK v0, trailer active_kek_version == 0.
	snapKeeper := keeperWithGens(t, kekV0, clusterID, targetGen)
	snapBytes := snapshotFromKeeper(t, snapKeeper, kekV0, clusterID)

	liveKeeper, err := encrypt.NewEmptyDEKKeeper(kekV1, clusterID)
	require.NoError(t, err)
	liveKeeper.SetActiveKEKVersion(1)

	fsm := NewMetaFSM()
	fsm.SetClusterID(clusterID)
	fsm.SetDEKKeeper(liveKeeper)

	// Non-nil keystore that LACKS the historical v0 → keystore.Get(0) errors.
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(1, kekV1))
	fsm.SetKEKStore(store)

	m := &MetaRaft{fsm: fsm, lastSnapshotIndex: 4}
	m.lastApplied.Store(4)

	halt := m.applySnapshotEntry(raft.SnapshotMeta{Index: 88}, snapBytes, 88)
	require.True(t, halt, "missing historical KEK must halt the apply loop")
	require.Error(t, fsm.FatalHaltedErr(), "missing historical KEK must mark the FSM fatally halted")
	require.Equal(t, uint64(4), m.lastApplied.Load(),
		"a halted snapshot must NOT advance lastApplied")
	require.Equal(t, uint64(4), m.lastSnapshotIndex,
		"a halted snapshot must NOT advance lastSnapshotIndex")
}

// TestInstallSnapshotDEKs_MissingHistoricalKEKReturnsError covers the defensive
// keystore.Get error return inside installSnapshotDEKs (the cross-KEK branch's
// failure arm). This branch is unreachable through applySnapshotEntry for an
// honest snapshot — Restore's envelope-open resolves the same KEK version first
// (see TestApplySnapshotEntry_CrossKEKMissingHistoricalKEKHalts) — so it is
// exercised white-box by calling installSnapshotDEKs directly with the pending
// state staged and the historical KEK absent from the keystore. It mirrors the
// reachable sibling branch in applyDEKReplicatedRotate (which log replay CAN hit,
// since log entries carry no envelope gate).
func TestInstallSnapshotDEKs_MissingHistoricalKEKReturnsError(t *testing.T) {
	t.Parallel()

	const targetGen = uint32(1)
	clusterID := snapDEKTestClusterID()
	kekV0 := newRandKEK(t)
	kekV1 := newRandKEK(t)

	snapKeeper := keeperWithGens(t, kekV0, clusterID, targetGen)
	versions, _ := snapKeeper.VersionsAndActive()

	// Live keeper at active KEK v1, holding no gens.
	liveKeeper, err := encrypt.NewEmptyDEKKeeper(kekV1, clusterID)
	require.NoError(t, err)
	liveKeeper.SetActiveKEKVersion(1)

	fsm := NewMetaFSM()
	fsm.SetClusterID(clusterID)
	fsm.SetDEKKeeper(liveKeeper)

	// Non-nil keystore LACKING the historical v0 → installSnapshotDEKs's
	// keystore.Get(0) errors at the targeted branch.
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(1, kekV1))
	fsm.SetKEKStore(store)

	// White-box: stage the pending snapshot DEK state (wraps sealed under v0).
	fsm.mu.Lock()
	fsm.pendingDEKVersions = versions
	fsm.pendingDEKActive = targetGen
	fsm.pendingActiveKEKVersion = 0
	fsm.mu.Unlock()

	err = fsm.installSnapshotDEKs()
	require.Error(t, err, "missing historical KEK must make installSnapshotDEKs error")
	require.Equal(t, uint32(0), liveKeeper.ActiveDEKGeneration(),
		"a failed install must not advance the keeper's active gen")
}

// TestInstallSnapshotDEKs_PinsActiveWhenNotMax exercises the post-loop active-gen
// pin for the present-but-not-max case (white-box). The snapshot carries gens
// {0..5} with pendingDEKActive == 1 — active is NOT the max gen. Because
// InstallReplicatedDEKWithKEK sets the keeper's active to each gen it installs
// and Go randomizes map iteration order, the in-loop installs leave the active
// gen at a RANDOM gen; the post-loop re-install of pendingDEKActive is what pins
// it back to 1. Asserting active == 1 (not 5) proves the pin runs.
//
// Mutation note: removing the post-loop pin makes this RED only probabilistically
// (~(N-1)/N per run, since map order is random). The pin's purpose is precisely
// to defeat that map-order nondeterminism. Run with -count to surface it.
func TestInstallSnapshotDEKs_PinsActiveWhenNotMax(t *testing.T) {
	t.Parallel()

	const maxGen = uint32(5)
	const activeGen = uint32(1) // deliberately NOT the max
	clusterID := snapDEKTestClusterID()
	kek := newRandKEK(t)

	// Reuse one keeper as both the source of wrapped bytes AND the install target:
	// the gens are already present (idempotent same-bytes install), so no AAD/bytes
	// plumbing is needed and the test isolates the active-gen pin behavior.
	keeper := keeperWithGens(t, kek, clusterID, maxGen)
	versions, _ := keeper.VersionsAndActive()

	fsm := NewMetaFSM()
	fsm.SetClusterID(clusterID)
	fsm.SetDEKKeeper(keeper)
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(0, kek))
	fsm.SetKEKStore(store)

	// White-box: stage the pending snapshot DEK state directly. The real
	// snapshot/keeper API derives active from max(versions), so active!=max can
	// only be constructed by setting the pending fields.
	fsm.mu.Lock()
	fsm.pendingDEKVersions = versions
	fsm.pendingDEKActive = activeGen
	fsm.pendingActiveKEKVersion = 0
	fsm.mu.Unlock()

	require.NoError(t, fsm.installSnapshotDEKs(), "install must succeed")
	require.Equal(t, activeGen, keeper.ActiveDEKGeneration(),
		"post-loop pin must leave the keeper active gen == pendingDEKActive (not the max gen)")
}

// TestApplySnapshotEntry_FatalHaltOnInstallError covers the install-failure
// branch: a snapshot whose gen-N wrapped bytes cannot be installed (the live
// keeper already holds gen N with DIFFERENT bytes → InstallReplicatedDEK returns
// a non-deterministic-bytes error) must halt the apply loop and NOT advance
// lastApplied / lastSnapshotIndex.
//
// Disclosure: map iteration order in installSnapshotDEKs is randomized, so the
// induced collision actually trips at gen 0 (the first gen whose live bytes
// differ from the snapshot's), not necessarily at gen N. The halt is gen-agnostic
// (any install error halts), so this still faithfully covers the fatal-halt path.
func TestApplySnapshotEntry_FatalHaltOnInstallError(t *testing.T) {
	t.Parallel()

	const targetGen = uint32(1)
	clusterID := snapDEKTestClusterID()
	kek := newRandKEK(t)

	// Snapshot keeper holds gens 0..N with one set of random material.
	snapKeeper := keeperWithGens(t, kek, clusterID, targetGen)
	snapBytes := snapshotFromKeeper(t, snapKeeper, kek, clusterID)

	// Live keeper independently holds gen N with DIFFERENT random material. The
	// snapshot install of gen N then collides on non-deterministic bytes.
	liveKeeper := keeperWithGens(t, kek, clusterID, targetGen)

	fsm := NewMetaFSM()
	fsm.SetClusterID(clusterID)
	fsm.SetDEKKeeper(liveKeeper)
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(0, kek))
	fsm.SetKEKStore(store)

	m := &MetaRaft{fsm: fsm, lastSnapshotIndex: 3}
	m.lastApplied.Store(3)

	halt := m.applySnapshotEntry(raft.SnapshotMeta{Index: 50}, snapBytes, 50)
	require.True(t, halt, "a non-deterministic-bytes install must halt the apply loop")
	require.Error(t, fsm.FatalHaltedErr(), "install failure must mark the FSM fatally halted")
	require.Equal(t, uint64(3), m.lastApplied.Load(),
		"a halted snapshot must NOT advance lastApplied")
	require.Equal(t, uint64(3), m.lastSnapshotIndex,
		"a halted snapshot must NOT advance lastSnapshotIndex")
}

// TestApplySnapshotEntry_NoTrailerPreservesKeeper covers the no-trailer path
// (linchpin 4 / M1): a snapshot with no DKVS trailer must reset pendingDEK* (so
// the install no-ops) and leave the EXISTING keeper object untouched. Pre-set a
// real keeper and assert the SAME pointer survives with empty pending versions.
func TestApplySnapshotEntry_NoTrailerPreservesKeeper(t *testing.T) {
	t.Parallel()

	clusterID := snapDEKTestClusterID()
	kek := newRandKEK(t)

	fsm := NewMetaFSM()
	fsm.SetClusterID(clusterID)
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(0, kek))
	fsm.SetKEKStore(store)

	// Pre-seed pendingDEK* by restoring a with-trailer snapshot, then wire a real
	// live keeper that must survive the no-trailer apply unchanged.
	withTrailer := snapshotFromKeeper(t, keeperWithGens(t, kek, clusterID, 1), kek, clusterID)
	require.NoError(t, fsm.Restore(raft.SnapshotMeta{}, withTrailer))
	pending, _ := fsm.PendingDEKVersions()
	require.NotEmpty(t, pending, "precondition: pendingDEKVersions populated by with-trailer restore")

	liveKeeper := keeperWithGens(t, kek, clusterID, 1)
	fsm.SetDEKKeeper(liveKeeper)

	// A no-trailer snapshot: a fresh FSM with no DEK keeper emits no DKVS trailer.
	noTrailerSrc := NewMetaFSM()
	noTrailerSrc.SetClusterID(clusterID)
	noTrailerSrc.SetKEKStore(store)
	noTrailerSnap, err := noTrailerSrc.Snapshot()
	require.NoError(t, err)

	m := &MetaRaft{fsm: fsm}
	halt := m.applySnapshotEntry(raft.SnapshotMeta{Index: 7}, noTrailerSnap, 7)
	require.False(t, halt, "no-trailer apply must not halt")

	after, _ := fsm.PendingDEKVersions()
	require.Empty(t, after,
		"no-trailer Restore must reset pendingDEKVersions so the install no-ops")
	require.Same(t, liveKeeper, fsm.DEKKeeper(),
		"no-trailer apply must leave the EXISTING keeper object unchanged (install no-ops)")
}

// TestRunApplyLoop_LiveSnapshotInstallsDEK is the durable regression guard for
// the live-routing link: it drives a synthetic LogEntrySnapshot through the REAL
// runApplyLoop (via snapshotApplyFakeNode's ApplyCh) and asserts the in-place
// install ran (the keeper gained the snapshot's gen N). This guards against a
// future refactor that extracts applySnapshotEntry but drops the live call site:
// every isolated test would still pass while the bug silently returns, so this
// exercises the actual channel-driven path.
func TestRunApplyLoop_LiveSnapshotInstallsDEK(t *testing.T) {
	const targetGen = uint32(1)
	clusterID := snapDEKTestClusterID()
	kek := newRandKEK(t)

	snapKeeper := keeperWithGens(t, kek, clusterID, targetGen)
	snapBytes := snapshotFromKeeper(t, snapKeeper, kek, clusterID)

	// Live keeper is stale (gen 0 only). After the live snapshot routes through
	// runApplyLoop → applySnapshotEntry → installSnapshotDEKs, it must hold gen N.
	staleKeeper := staleKeeperFromSnap(t, snapKeeper, kek, clusterID, 0)

	target := NewMetaFSM()
	target.SetClusterID(clusterID)
	target.SetDEKKeeper(staleKeeper)
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(0, kek))
	target.SetKEKStore(store)

	require.Equal(t, uint32(0), staleKeeper.ActiveDEKGeneration(),
		"precondition: live keeper active gen is 0 (stale)")

	ch := make(chan raft.LogEntry, 1)
	ch <- raft.LogEntry{Index: 9, Term: 1, Type: raft.LogEntrySnapshot, Command: snapBytes}
	node := &snapshotApplyFakeNode{ch: ch}

	m := &MetaRaft{
		node:           node,
		fsm:            target,
		capabilityGate: NewCapabilityGate(compat.DefaultRegistry, time.Minute),
		done:           make(chan struct{}),
		applyNotify:    make(chan struct{}),
		applyErrs:      make(map[uint64]error),
	}
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Start(context.Background(), nil))

	// The buffered live LogEntrySnapshot must drive an in-place install through
	// the real runApplyLoop. An unwired live call site would leave the keeper at
	// gen 0 (and lastApplied never advancing to 9).
	require.Eventually(t, func() bool {
		return m.lastApplied.Load() == 9
	}, 2*time.Second, 20*time.Millisecond,
		"live snapshot must be marked applied after the in-place install succeeds")
	require.Same(t, staleKeeper, target.DEKKeeper(),
		"runApplyLoop install must preserve keeper identity")
	require.Equal(t, uint32(targetGen), staleKeeper.ActiveDEKGeneration(),
		"the live runApplyLoop path must install the snapshot's gen N into the existing keeper")
}
