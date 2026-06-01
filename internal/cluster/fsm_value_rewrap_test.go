package cluster

import (
	"bytes"
	"context"
	"fmt"
	"testing"

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
	gb := newTestGroupBackend(t, "group-1")
	ks := gb.ks()
	keeper := gb.shardSvc.dekKeeper
	require.NotNil(t, keeper)

	// At gen 0: seal stale policy: key.
	polKey := ks.BucketPolicyKey("b1")
	polRaw, err := gb.fsm.sealValue(polKey, []byte(`{}`))
	require.NoError(t, err)
	dbSet(t, gb.db, polKey, polRaw)
	require.Equal(t, uint32(0), gbFrameGenOf(t, gb, polKey), "policy should be at gen 0 (stale)")

	// At gen 0: also seal mpu: key (should be excluded by prefix scan).
	mpuKey := ks.MultipartKey("up-1")
	mpuRaw, err := gb.fsm.sealValue(mpuKey, []byte(`{}`))
	require.NoError(t, err)
	dbSet(t, gb.db, mpuKey, mpuRaw)

	// Rotate keeper to gen 1 (now active == 1).
	require.NoError(t, keeper.Rotate())
	_, active := keeper.VersionsAndActive()
	require.Equal(t, uint32(1), active)

	// At gen 1: seal active obj: key.
	objKey := ks.ObjectMetaKey("b1", "k")
	objRaw, err := gb.fsm.sealValue(objKey, []byte(`{}`))
	require.NoError(t, err)
	dbSet(t, gb.db, objKey, objRaw)
	require.Equal(t, uint32(1), gbFrameGenOf(t, gb, objKey), "obj should be at gen 1 (active)")

	// CollectStaleFSMValueKeys with activeGen=1: only policy: should appear.
	keys, err := gb.CollectStaleFSMValueKeys(1, 100, 10<<20)
	require.NoError(t, err)
	require.Equal(t, []string{string(polKey)}, keys, "only the stale policy: key should be collected")
}

// TestCollectStaleFSMValueKeys_RespectsMaxBatch verifies that the count cap is respected.
func TestCollectStaleFSMValueKeys_RespectsMaxBatch(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	ks := gb.ks()

	// Seed 5 stale policy: keys at gen 0.
	for i := 0; i < 5; i++ {
		key := ks.BucketPolicyKey(fmt.Sprintf("b%d", i))
		raw, err := gb.fsm.sealValue(key, []byte(`{}`))
		require.NoError(t, err)
		dbSet(t, gb.db, key, raw)
	}

	// Rotate to gen 1 so all 5 are stale.
	require.NoError(t, gb.shardSvc.dekKeeper.Rotate())

	keys, err := gb.CollectStaleFSMValueKeys(1, 3, 10<<20) // maxBatch=3
	require.NoError(t, err)
	require.Len(t, keys, 3, "maxBatch=3 must cap the result")
}

// TestCollectStaleFSMValueKeys_RespectsMaxBytes verifies the byte-budget cap.
func TestCollectStaleFSMValueKeys_RespectsMaxBytes(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	ks := gb.ks()

	// Seed 5 policy: keys with ~200-byte values.
	largeVal := bytes.Repeat([]byte("x"), 200)
	for i := 0; i < 5; i++ {
		key := ks.BucketPolicyKey(fmt.Sprintf("big%d", i))
		raw, err := gb.fsm.sealValue(key, largeVal)
		require.NoError(t, err)
		dbSet(t, gb.db, key, raw)
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
	gb := newTestGroupBackend(t, "group-1")
	ks := gb.ks()
	keeper := gb.shardSvc.dekKeeper

	// Seed 7 stale policy: keys at gen 0.
	for i := 0; i < 7; i++ {
		key := ks.BucketPolicyKey(fmt.Sprintf("b%d", i))
		raw, err := gb.fsm.sealValue(key, []byte(`{}`))
		require.NoError(t, err)
		dbSet(t, gb.db, key, raw)
	}

	// Rotate to gen 1 (active = 1).
	require.NoError(t, keeper.Rotate())

	// Drain with maxBatch=3 (will take ceil(7/3) = 3 iterations).
	require.NoError(t, DrainFSMValueRewrap(context.Background(), gb, 1, 3))

	// All policy keys should now be at gen 1.
	left, err := gb.CollectStaleFSMValueKeys(1, 100, 10<<20)
	require.NoError(t, err)
	require.Empty(t, left, "all stale keys should be drained")
}

// TestDrainFSMValueRewrap_IdempotentWhenClean verifies drain on already-active data is safe.
func TestDrainFSMValueRewrap_IdempotentWhenClean(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	ks := gb.ks()
	keeper := gb.shardSvc.dekKeeper

	// Rotate to gen 1.
	require.NoError(t, keeper.Rotate())

	// Seal a value at gen 1 (active).
	key := ks.BucketPolicyKey("b1")
	raw, err := gb.fsm.sealValue(key, []byte(`{}`))
	require.NoError(t, err)
	dbSet(t, gb.db, key, raw)

	// Drain with activeGen=1 — nothing to do, no error.
	require.NoError(t, DrainFSMValueRewrap(context.Background(), gb, 1, 100))
}
