package cluster

import (
	"bytes"
	"context"
	"io"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
)

// setupECRewrapBackend mirrors setupECBackend but returns the keeper so the test
// can rotate the active DEK generation (gen 0 -> 1) after seeding a shard.
func setupECRewrapBackend(t *testing.T) (*DistributedBackend, *encrypt.DEKKeeper) {
	t.Helper()
	backend := NewSingletonBackendForTest(t)

	const selfAddr = "self"
	shardDir := t.TempDir()
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(shardDir, nil, WithShardDEKKeeper(keeper, clusterID))
	backend.shardSvc = svc
	backend.selfAddr = selfAddr
	backend.allNodes = []string{selfAddr, selfAddr, selfAddr}
	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1})
	return backend, keeper
}

// shardGenOnDisk reads the raw on-disk shard bytes (standalone file) and returns
// the encrypted header generation.
func shardGenOnDisk(t *testing.T, b *DistributedBackend, bucket, canonicalKey string, shardIdx int) uint32 {
	t.Helper()
	path := mustShardPath(b.shardSvc, bucket, canonicalKey, shardIdx)
	raw, err := readFileOrPack(b, bucket, canonicalKey, shardIdx, path)
	require.NoError(t, err)
	gen, ok := eccodec.EncryptedShardGen(raw)
	require.True(t, ok, "expected an encrypted shard header on disk")
	return gen
}

func readFileOrPack(b *DistributedBackend, bucket, canonicalKey string, shardIdx int, path string) ([]byte, error) {
	// Shard-packing was removed (S3); shards are always standalone files now.
	_, _, _ = b, bucket, canonicalKey
	_ = shardIdx
	return os.ReadFile(path)
}

func TestRewrapShardIfStale(t *testing.T) {
	backend, keeper := setupECRewrapBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "b"))

	content := bytes.Repeat([]byte("rewrap-shard-payload-"), 64)
	obj, err := backend.PutObject(ctx, "b", "obj", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, obj.VersionID)

	canonicalKey := ecObjectShardKey("obj", obj.VersionID)
	require.Equal(t, uint32(0), shardGenOnDisk(t, backend, "b", canonicalKey, 0), "seeded at gen 0")

	// Rotate the active DEK generation to 1.
	require.NoError(t, keeper.Rotate())
	if _, active := keeper.VersionsAndActive(); active != 1 {
		t.Fatalf("expected active gen 1 after rotate, got %d", active)
	}

	// First call migrates the stale (gen 0) shard onto the active gen (1).
	did, err := backend.RewrapShardIfStale("b", "obj", obj.VersionID, 0, 1)
	require.NoError(t, err)
	assert.True(t, did, "stale shard must be migrated")
	assert.Equal(t, uint32(1), shardGenOnDisk(t, backend, "b", canonicalKey, 0), "shard now at active gen")

	// Object still reads back the same plaintext after rewrap.
	rc, _, err := backend.GetObject(ctx, "b", "obj")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, err)
	assert.Equal(t, content, got, "plaintext preserved across rewrap")

	// Second call is idempotent (already at active gen).
	did, err = backend.RewrapShardIfStale("b", "obj", obj.VersionID, 0, 1)
	require.NoError(t, err)
	assert.False(t, did, "already-migrated shard must be a no-op")
}

// fakeECRewrapBackend is one fake data-group backend: it yields pre-seeded
// ECRewrapTargets via CollectECRewrapTargets and records RewrapShardIfStaleAt
// calls.
type fakeECRewrapBackend struct {
	targets    []ECRewrapTarget // shards to return from CollectECRewrapTargets
	collectErr error
	rewrapErr  error
	mu         sync.Mutex
	atCalls    []rewrapAtCall
	didOnFirst map[string]bool // canonicalKey|idx -> already returned true
}

type rewrapAtCall struct {
	bucket, canonicalKey string
	idx                  int
	activeGen            uint32
}

func (f *fakeECRewrapBackend) CollectECRewrapTargets() ([]ECRewrapTarget, error) {
	return f.targets, f.collectErr
}

func (f *fakeECRewrapBackend) RewrapShardIfStaleAt(bucket, canonicalKey string, shardIdx int, activeGen uint32) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.atCalls = append(f.atCalls, rewrapAtCall{bucket, canonicalKey, shardIdx, activeGen})
	if f.rewrapErr != nil {
		return false, f.rewrapErr
	}
	k := canonicalKey + "|" + strconv.Itoa(shardIdx)
	if f.didOnFirst == nil {
		f.didOnFirst = map[string]bool{}
	}
	if !f.didOnFirst[k] {
		f.didOnFirst[k] = true
		return true, nil
	}
	return false, nil
}

func laneFromGroups(nodeID string, gbs ...ECRewrapShardBackend) *ECRewrapLane {
	return NewECRewrapLane(nodeID, func() []ECRewrapShardBackend { return gbs })
}

// TestECRewrapLane_SweepAllOwnedShards proves the lane calls RewrapShardIfStaleAt
// for every shard positionally owned by this node.
func TestECRewrapLane_SweepAllOwnedShards(t *testing.T) {
	// Target: shards 0, 2, 5 owned by "n1" (NodeIDs[i] == "n1").
	fake := &fakeECRewrapBackend{
		targets: []ECRewrapTarget{
			{Bucket: "b", ShardKey: "k1/v1", NodeIDs: []string{"n1", "other", "n1", "other", "other", "n1"}},
		},
	}

	before := counterValue(t, 7)
	lane := laneFromGroups("n1", fake)
	require.Equal(t, "ec", lane.Name())
	require.NoError(t, lane.RewrapByGen(context.Background(), 3, 7))

	require.Len(t, fake.atCalls, 3, "every owned shard visited")
	for _, c := range fake.atCalls {
		assert.Equal(t, uint32(7), c.activeGen)
	}
	assert.Equal(t, float64(3), counterValue(t, 7)-before, "counter increments per did==true")
}

// TestECRewrapLane_SweepsAllGroups proves the lane visits targets across EVERY
// data group, not just one.
func TestECRewrapLane_SweepsAllGroups(t *testing.T) {
	g1 := &fakeECRewrapBackend{
		targets: []ECRewrapTarget{
			{Bucket: "b", ShardKey: "in-g1/v1", NodeIDs: []string{"n1", "other"}},
		},
	}
	g2 := &fakeECRewrapBackend{
		targets: []ECRewrapTarget{
			{Bucket: "b", ShardKey: "in-g2/v1", NodeIDs: []string{"n1", "other"}},
		},
	}
	lane := laneFromGroups("n1", g1, g2)
	require.NoError(t, lane.RewrapByGen(context.Background(), 0, 5))
	require.Len(t, g1.atCalls, 1, "group 1 shard swept")
	require.Len(t, g2.atCalls, 1, "group 2 shard swept")
}

// TestECRewrapLane_ContinuesOnShardError proves a per-shard error does NOT abort
// the sweep (forward progress preserved), but the sweep returns an aggregate
// error so the caller knows some shards were skipped.
func TestECRewrapLane_ContinuesOnShardError(t *testing.T) {
	fake := &fakeECRewrapBackend{
		targets: []ECRewrapTarget{
			{Bucket: "b", ShardKey: "k1/v1", NodeIDs: []string{"n1", "n1", "n1"}},
		},
		rewrapErr: assert.AnError,
	}
	lane := laneFromGroups("n1", fake)
	err := lane.RewrapByGen(context.Background(), 0, 5)
	require.Error(t, err, "per-shard errors must produce an aggregate error")
	require.Contains(t, err.Error(), "skipped", "error message must mention skipped shards")
	require.Len(t, fake.atCalls, 3, "every owned shard attempted despite errors (forward progress)")
}

// TestECRewrapLane_ReturnsErrorWhenGroupEnumerationFails proves that a
// CollectECRewrapTargets error causes RewrapByGen to return a non-nil error.
func TestECRewrapLane_ReturnsErrorWhenGroupEnumerationFails(t *testing.T) {
	fake := &fakeECRewrapBackend{
		collectErr: assert.AnError,
	}
	lane := laneFromGroups("n1", fake)
	err := lane.RewrapByGen(context.Background(), 0, 5)
	require.Error(t, err, "group enumeration error must produce an aggregate error")
	require.Contains(t, err.Error(), "skipped", "error message must mention skipped groups")
}

// TestECRewrapLane_NilWhenAllRewrapped proves the sweep returns nil when all
// owned shards rewrap successfully.
func TestECRewrapLane_NilWhenAllRewrapped(t *testing.T) {
	fake := &fakeECRewrapBackend{
		targets: []ECRewrapTarget{
			{Bucket: "b", ShardKey: "k1/v1", NodeIDs: []string{"n1", "other"}},
		},
	}
	lane := laneFromGroups("n1", fake)
	require.NoError(t, lane.RewrapByGen(context.Background(), 0, 5), "successful rewrap must return nil")
	require.Len(t, fake.atCalls, 1, "one owned shard visited")
}

func TestECRewrapLane_CtxCancelStops(t *testing.T) {
	fake := &fakeECRewrapBackend{
		targets: []ECRewrapTarget{
			{Bucket: "b", ShardKey: "k1/v1", NodeIDs: []string{"n1"}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	lane := laneFromGroups("n1", fake)
	err := lane.RewrapByGen(ctx, 0, 4)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, fake.atCalls, "cancelled ctx stops before any rewrap")
}

// TestECRewrapLane_SweepsSegmentShards verifies the lane visits segment/coalesced
// shards owned positionally by this node. The target has NodeIDs
// ["n1","other","n1"]; the lane must call RewrapShardIfStaleAt for indices 0
// and 2 (both owned by "n1") and NOT for index 1 (owned by "other").
func TestECRewrapLane_SweepsSegmentShards(t *testing.T) {
	fake := &fakeECRewrapBackend{
		targets: []ECRewrapTarget{
			{Bucket: "b", ShardKey: "k/segments/b1", NodeIDs: []string{"n1", "other", "n1"}},
		},
	}
	lane := laneFromGroups("n1", fake)
	require.NoError(t, lane.RewrapByGen(context.Background(), 0, 9))

	require.Len(t, fake.atCalls, 2, "only shards owned by n1 (idx 0 and 2) are visited")
	idxs := map[int]bool{}
	for _, c := range fake.atCalls {
		assert.Equal(t, "b", c.bucket)
		assert.Equal(t, "k/segments/b1", c.canonicalKey)
		assert.Equal(t, uint32(9), c.activeGen)
		idxs[c.idx] = true
	}
	assert.True(t, idxs[0], "shard idx 0 must be visited")
	assert.True(t, idxs[2], "shard idx 2 must be visited")
	assert.False(t, idxs[1], "shard idx 1 (owned by other) must NOT be visited")
}

func counterValue(t *testing.T, activeGen uint32) float64 {
	t.Helper()
	return testutil.ToFloat64(RewrapECShardsTotal.WithLabelValues(strconv.FormatUint(uint64(activeGen), 10)))
}
