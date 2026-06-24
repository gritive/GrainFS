package cluster

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// dbSet is a helper to write a key/value directly into a badger DB in tests.
func dbSet(t *testing.T, db *badger.DB, key, val []byte) {
	t.Helper()
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	}))
}

// gbFrameGenOf reads the frame gen for a key from the GroupBackend DB.
func gbFrameGenOf(t *testing.T, gb *GroupBackend, key []byte) uint32 {
	t.Helper()
	return frameGenOf(t, gb.fsm, key)
}

// TestCollectStaleFSMValueKeys_PolicyAndObjOnly verifies that stale policy:
// values are collected, active obj: values are NOT collected, and mpu: values
// are EXCLUDED regardless of gen.
func TestCollectStaleFSMValueKeys_PolicyAndObjOnly(t *testing.T) {
	// Use newTestGroupBackend (keeper starts at gen 0).
	gb, gbDB := newTestGroupBackendWithDB(t, "group-1")
	ks := gb.ks()
	keeper := gb.shardSvc.dekKeeper
	require.NotNil(t, keeper)

	// At gen 0: seal stale policy: key.
	polKey := ks.Key([]byte("policy:b1"))
	polRaw, err := gb.fsm.sealValue(polKey, []byte(`{}`))
	require.NoError(t, err)
	dbSet(t, gbDB, polKey, polRaw)
	require.Equal(t, uint32(0), gbFrameGenOf(t, gb, polKey), "policy should be at gen 0 (stale)")

	// At gen 0: also seal mpu: key (should be excluded by prefix scan).
	mpuKey := ks.MultipartKey("up-1")
	mpuRaw, err := gb.fsm.sealValue(mpuKey, []byte(`{}`))
	require.NoError(t, err)
	dbSet(t, gbDB, mpuKey, mpuRaw)

	// Rotate keeper to gen 1 (now active == 1).
	require.NoError(t, keeper.Rotate())
	_, active := keeper.VersionsAndActive()
	require.Equal(t, uint32(1), active)

	// At gen 1: seal active obj: key.
	objKey := ks.ObjectMetaKey("b1", "k")
	objRaw, err := gb.fsm.sealValue(objKey, []byte(`{}`))
	require.NoError(t, err)
	dbSet(t, gbDB, objKey, objRaw)
	require.Equal(t, uint32(1), gbFrameGenOf(t, gb, objKey), "obj should be at gen 1 (active)")

	// CollectStaleFSMValueKeys with activeGen=1: only policy: should appear.
	keys, err := gb.CollectStaleFSMValueKeys(1, 100, 10<<20)
	require.NoError(t, err)
	require.Equal(t, []string{string(polKey)}, keys, "only the stale policy: key should be collected")
}

// TestCollectStaleFSMValueKeys_RespectsMaxBatch verifies that the count cap is respected.
func TestCollectStaleFSMValueKeys_RespectsMaxBatch(t *testing.T) {
	gb, gbDB := newTestGroupBackendWithDB(t, "group-1")
	ks := gb.ks()

	// Seed 5 stale policy: keys at gen 0.
	for i := 0; i < 5; i++ {
		key := ks.Key([]byte("policy:" + fmt.Sprintf("b%d", i)))
		raw, err := gb.fsm.sealValue(key, []byte(`{}`))
		require.NoError(t, err)
		dbSet(t, gbDB, key, raw)
	}

	// Rotate to gen 1 so all 5 are stale.
	require.NoError(t, gb.shardSvc.dekKeeper.Rotate())

	keys, err := gb.CollectStaleFSMValueKeys(1, 3, 10<<20) // maxBatch=3
	require.NoError(t, err)
	require.Len(t, keys, 3, "maxBatch=3 must cap the result")
}

// TestCollectStaleFSMValueKeys_RespectsMaxBytes verifies the byte-budget cap.
func TestCollectStaleFSMValueKeys_RespectsMaxBytes(t *testing.T) {
	gb, gbDB := newTestGroupBackendWithDB(t, "group-1")
	ks := gb.ks()

	// Seed 5 policy: keys with ~200-byte values.
	largeVal := bytes.Repeat([]byte("x"), 200)
	for i := 0; i < 5; i++ {
		key := ks.Key([]byte("policy:" + fmt.Sprintf("big%d", i)))
		raw, err := gb.fsm.sealValue(key, largeVal)
		require.NoError(t, err)
		dbSet(t, gbDB, key, raw)
	}

	// Rotate to gen 1 so all 5 are stale.
	require.NoError(t, gb.shardSvc.dekKeeper.Rotate())

	// Each raw frame is header(9 bytes) + AEAD overhead(~28) + 200 plaintext ≈ 237 bytes.
	// maxBytes=300 → only 1 should fit before the budget is hit.
	keys, err := gb.CollectStaleFSMValueKeys(1, 100, 300)
	require.NoError(t, err)
	require.Len(t, keys, 1, "maxBytes=300 must cap after first large value")
}

// TestDrainFSMValueRewrap_DrainsGroupToActive verifies full drain.
func TestDrainFSMValueRewrap_DrainsGroupToActive(t *testing.T) {
	gb, gbDB := newTestGroupBackendWithDB(t, "group-1")
	ks := gb.ks()
	keeper := gb.shardSvc.dekKeeper

	// Seed 7 stale policy: keys at gen 0.
	for i := 0; i < 7; i++ {
		key := ks.Key([]byte("policy:" + fmt.Sprintf("b%d", i)))
		raw, err := gb.fsm.sealValue(key, []byte(`{}`))
		require.NoError(t, err)
		dbSet(t, gbDB, key, raw)
	}

	// Rotate to gen 1 (active = 1).
	require.NoError(t, keeper.Rotate())

	// Drain with maxBatch=3 (will take ceil(7/3) = 3 iterations).
	require.NoError(t, DrainFSMValueRewrap(context.Background(), gb, 3))

	// All policy keys should now be at gen 1.
	left, err := gb.CollectStaleFSMValueKeys(1, 100, 10<<20)
	require.NoError(t, err)
	require.Empty(t, left, "all stale keys should be drained")
}

// TestDrainFSMValueRewrap_IdempotentWhenClean verifies drain on already-active data is safe.
func TestDrainFSMValueRewrap_IdempotentWhenClean(t *testing.T) {
	gb, gbDB := newTestGroupBackendWithDB(t, "group-1")
	ks := gb.ks()
	keeper := gb.shardSvc.dekKeeper

	// Rotate to gen 1.
	require.NoError(t, keeper.Rotate())

	// Seal a value at gen 1 (active).
	key := ks.Key([]byte("policy:b1"))
	raw, err := gb.fsm.sealValue(key, []byte(`{}`))
	require.NoError(t, err)
	dbSet(t, gbDB, key, raw)

	// Drain when keeper-current == 1 and value already at 1 — nothing to do.
	require.NoError(t, DrainFSMValueRewrap(context.Background(), gb, 100))
}

// TestDrainFSMValueRewrap_RotationMidDrainTerminatesAndConverges is the
// regression test for the back-to-back-rotation livelock. The OLD code pinned
// the drain to a fixed activeGen captured at trigger time: if a 2nd rotation
// advanced the keeper WHILE the drain was iterating, the scan kept collecting
// keys (gen != pinnedGen) that apply kept resealing onto keeper-current (a
// DIFFERENT gen than pinnedGen), so every subsequent scan re-collected the same
// keys → an infinite loop committing real raft entries, and the single-flight
// guard (held by the never-returning drain goroutine) leaked forever, so that
// group was skipped on all future rotations.
//
// To reproduce the LIVELOCK (not just a pre-drain rotation), this test uses the
// fsmValueRewrapAfterProposeHook to advance the keeper's active gen between the
// first and second drain iterations — exactly the "rotation lands mid-drain"
// shape. With a fixed-gen drain, the per-iteration target never matches the
// keeper-current reseal target after the rotation, so it never converges. With
// the fix (drain reads keeper-current each iteration), the rotation just shifts
// the target and the drain converges.
//
// Asserts: (a) the drain TERMINATES — bounded by a hard timeout so a regression
// hangs the test rather than looping forever; (b) the store ends all-at-current
// (the new gen, not the seed gen); (c) a SECOND drain on the same group runs
// cleanly (the drain always returns → the single-flight guard's defer releases,
// so the group is never permanently skipped).
func TestDrainFSMValueRewrap_RotationMidDrainTerminatesAndConverges(t *testing.T) {
	gb, gbDB := newTestGroupBackendWithDB(t, "group-1")
	ks := gb.ks()
	keeper := gb.shardSvc.dekKeeper

	// Seed 6 stale policy: keys at gen 0, then rotate to gen 1 (initial active).
	const n = 6
	for i := 0; i < n; i++ {
		key := ks.Key([]byte("policy:" + fmt.Sprintf("b%d", i)))
		raw, err := gb.fsm.sealValue(key, []byte(`{}`))
		require.NoError(t, err)
		dbSet(t, gbDB, key, raw)
	}
	require.NoError(t, keeper.Rotate()) // gen 1 active; values at gen 0 → stale

	// Mid-drain rotation: after the FIRST batch is proposed, advance the keeper
	// to gen 2. This is the back-to-back-rotation race. Fire exactly once so the
	// keeper settles and the drain can converge to gen 2.
	var hookOnce sync.Once
	fsmValueRewrapAfterProposeHook = func() {
		hookOnce.Do(func() {
			require.NoError(t, keeper.Rotate()) // gen 2, mid-drain
		})
	}
	t.Cleanup(func() { fsmValueRewrapAfterProposeHook = nil })

	// (a) Termination — bounded by a hard timeout. A regression livelocks here.
	// maxBatch=2 forces multiple iterations so the hook fires before the drain
	// would otherwise finish.
	done := make(chan error, 1)
	go func() {
		done <- DrainFSMValueRewrap(context.Background(), gb, 2)
	}()
	select {
	case err := <-done:
		require.NoError(t, err, "drain must terminate without error")
	case <-time.After(15 * time.Second):
		t.Fatal("DrainFSMValueRewrap did not terminate — back-to-back-rotation livelock regression")
	}

	// (b) Store converged to keeper-current (gen 2): no key is stale against gen 2.
	_, active := keeper.VersionsAndActive()
	require.Equal(t, uint32(2), active, "keeper advanced to gen 2 mid-drain")
	left, err := gb.CollectStaleFSMValueKeys(2, 100, 10<<20)
	require.NoError(t, err)
	require.Empty(t, left, "all values must be resealed onto keeper-current gen 2")
	for i := 0; i < n; i++ {
		key := ks.Key([]byte("policy:" + fmt.Sprintf("b%d", i)))
		require.Equal(t, uint32(2), gbFrameGenOf(t, gb, key), "value must end at keeper-current gen 2")
	}

	// (c) A second drain on the same group runs cleanly and terminates (no
	// mid-drain rotation this time — hookOnce is spent). Proves the drain
	// always returns so the single-flight guard releases.
	done2 := make(chan error, 1)
	go func() { done2 <- DrainFSMValueRewrap(context.Background(), gb, 2) }()
	select {
	case err := <-done2:
		require.NoError(t, err, "second drain must run cleanly")
	case <-time.After(15 * time.Second):
		t.Fatal("second DrainFSMValueRewrap did not terminate")
	}
}
