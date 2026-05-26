package serveruntime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
)

// dataWALStartupRepairFunc adapts a function to the dataWALStartupRepairer
// interface so tests can inject a fake repairer. Test-only: production uses
// dataWALStartupRepairRuntime.
type dataWALStartupRepairFunc func(context.Context, cluster.DataWALRepairCandidate) dataWALStartupRepairResult

func (f dataWALStartupRepairFunc) RepairDataWALStartupCandidate(ctx context.Context, candidate cluster.DataWALRepairCandidate) dataWALStartupRepairResult {
	return f(ctx, candidate)
}

func TestSplitDataWALStartupRepairShardKey(t *testing.T) {
	key, versionID := splitDataWALStartupRepairShardKey("dir/object/v1")
	require.Equal(t, "dir/object", key)
	require.Equal(t, "v1", versionID)

	key, versionID = splitDataWALStartupRepairShardKey("legacy-object")
	require.Equal(t, "legacy-object", key)
	require.Equal(t, "", versionID)
}

func TestDataWALStartupRepairWorkerRunsSerially(t *testing.T) {
	var inFlight int32
	var maxInFlight int32
	repairer := dataWALStartupRepairFunc(func(ctx context.Context, candidate cluster.DataWALRepairCandidate) dataWALStartupRepairResult {
		now := atomic.AddInt32(&inFlight, 1)
		for {
			old := atomic.LoadInt32(&maxInFlight)
			if now <= old || atomic.CompareAndSwapInt32(&maxInFlight, old, now) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
		return dataWALStartupRepairResult{Repaired: true}
	})

	candidates := []cluster.DataWALRepairCandidate{
		{Bucket: "b", ShardKey: "obj/v1", ShardIdx: 0, Reason: cluster.DataWALRepairMissing},
		{Bucket: "b", ShardKey: "obj/v1", ShardIdx: 1, Reason: cluster.DataWALRepairMissing},
		{Bucket: "b", ShardKey: "obj/v1", ShardIdx: 2, Reason: cluster.DataWALRepairMissing},
	}
	got := runDataWALStartupRepairCandidates(context.Background(), candidates, repairer)

	require.Len(t, got, 3)
	require.Equal(t, int32(1), atomic.LoadInt32(&maxInFlight))
}

func TestClassifyDataWALStartupRepairFailure(t *testing.T) {
	require.Equal(t, "context_canceled", classifyDataWALStartupRepairFailure(context.Canceled))
	require.Equal(t, "insufficient_survivors", classifyDataWALStartupRepairFailure(assertErr("only 2 other shards readable")))
	require.Equal(t, "repair_failed", classifyDataWALStartupRepairFailure(assertErr("disk failed")))
}

type assertErr string

func (e assertErr) Error() string { return string(e) }

func TestDataWALStartupRepairWorkerStopsOnCancel(t *testing.T) {
	candidates := []cluster.DataWALRepairCandidate{
		{Bucket: "b", ShardKey: "obj/v1", ShardIdx: 0, Reason: cluster.DataWALRepairMissing},
		{Bucket: "b", ShardKey: "obj/v1", ShardIdx: 1, Reason: cluster.DataWALRepairMissing},
		{Bucket: "b", ShardKey: "obj/v1", ShardIdx: 2, Reason: cluster.DataWALRepairMissing},
	}

	t.Run("already canceled ctx returns zero results", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel before running

		calls := 0
		repairer := dataWALStartupRepairFunc(func(ctx context.Context, candidate cluster.DataWALRepairCandidate) dataWALStartupRepairResult {
			calls++
			return dataWALStartupRepairResult{Repaired: true}
		})

		got := runDataWALStartupRepairCandidates(ctx, candidates, repairer)

		require.Len(t, got, 0)
		require.Equal(t, 0, calls, "repairer should not be called when ctx is already canceled")
	})

	t.Run("repairer returning context_canceled stops the loop", func(t *testing.T) {
		calls := 0
		repairer := dataWALStartupRepairFunc(func(ctx context.Context, candidate cluster.DataWALRepairCandidate) dataWALStartupRepairResult {
			calls++
			return dataWALStartupRepairResult{Failed: "context_canceled"}
		})

		got := runDataWALStartupRepairCandidates(context.Background(), candidates, repairer)

		require.Len(t, got, 1)
		require.Equal(t, "context_canceled", got[0].Failed)
		require.Equal(t, 1, calls, "repairer should only be called once before stopping")
	})
}

func TestDataWALStartupRepairRuntimeSkipsNoGroup(t *testing.T) {
	mgr := cluster.NewDataGroupManager()
	router := cluster.NewRouter(mgr)
	repairer := dataWALStartupRepairRuntime{dgMgr: mgr, router: router}

	got := repairer.RepairDataWALStartupCandidate(context.Background(), cluster.DataWALRepairCandidate{
		Bucket: "b", ShardKey: "obj/v1", ShardIdx: 0,
	})

	require.Equal(t, dataWALStartupRepairResult{Skipped: "no_group"}, got)
}

func TestDataWALStartupRepairRuntimeSkipsGroupWithoutBackend(t *testing.T) {
	mgr := cluster.NewDataGroupManager()
	router := cluster.NewRouter(mgr)
	router.AssignBucket("b", "group-0")
	mgr.Add(cluster.NewDataGroup("group-0", []string{"n0"}))
	repairer := dataWALStartupRepairRuntime{dgMgr: mgr, router: router}

	got := repairer.RepairDataWALStartupCandidate(context.Background(), cluster.DataWALRepairCandidate{
		Bucket: "b", ShardKey: "obj/v1", ShardIdx: 0,
	})

	require.Equal(t, dataWALStartupRepairResult{Skipped: "no_backend"}, got)
}

func TestClassifyDataWALStartupRepairPlacement(t *testing.T) {
	const self = "10.0.0.1:9001"
	cfg := cluster.ECConfig{DataShards: 2, ParityShards: 1}
	okRec := cluster.PlacementRecord{K: 2, M: 1, Nodes: []string{self, "n1", "n2"}}

	cases := []struct {
		name      string
		objectKey string
		shardIdx  int
		rec       cluster.PlacementRecord
		lookupErr error
		wantSkip  string
	}{
		{"invalid empty key", "", 0, okRec, nil, "invalid_shard_key"},
		{"invalid negative idx", "obj", -1, okRec, nil, "invalid_shard_key"},
		{"placement corrupt on lookup error", "obj", 0, cluster.PlacementRecord{}, assertErr("boom"), "placement_corrupt"},
		{"stale empty nodes", "obj", 0, cluster.PlacementRecord{}, nil, "stale"},
		{"placement corrupt node count mismatch", "obj", 0, cluster.PlacementRecord{K: 2, M: 1, Nodes: []string{self, "n1"}}, nil, "placement_corrupt"},
		{"not local owner", "obj", 1, okRec, nil, "not_local_owner"},
		{"owned proceeds", "obj", 0, okRec, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			skip := classifyDataWALStartupRepairPlacement(tc.objectKey, tc.shardIdx, tc.rec, cfg, tc.lookupErr, self)
			require.Equal(t, tc.wantSkip, skip)
		})
	}
}
