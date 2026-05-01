package cluster

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

func makeAddNodeCmd(t *testing.T, id, addr string, role uint8) []byte {
	t.Helper()
	data, err := encodeMetaAddNodeCmd(MetaNodeEntry{ID: id, Address: addr, Role: role})
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeAddNode, data)
	require.NoError(t, err)
	return cmd
}

func makeRemoveNodeCmd(t *testing.T, id string) []byte {
	t.Helper()
	data, err := encodeMetaRemoveNodeCmd(id)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeRemoveNode, data)
	require.NoError(t, err)
	return cmd
}

func TestMetaFSM_Apply_AddNode(t *testing.T) {
	f := NewMetaFSM()
	err := f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.1:7001", 0))
	require.NoError(t, err)

	nodes := f.Nodes()
	require.Len(t, nodes, 1)
	assert.Equal(t, "node-1", nodes[0].ID)
	assert.Equal(t, "10.0.0.1:7001", nodes[0].Address)
	assert.Equal(t, uint8(0), nodes[0].Role)
}

func TestMetaFSM_Apply_RemoveNode(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.1:7001", 0)))
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-2", "10.0.0.2:7001", 0)))

	require.NoError(t, f.applyCmd(makeRemoveNodeCmd(t, "node-1")))

	nodes := f.Nodes()
	require.Len(t, nodes, 1)
	assert.Equal(t, "node-2", nodes[0].ID)
}

func TestMetaFSM_Apply_NoOp(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.1:7001", 0)))

	noopCmd, err := encodeMetaCmd(MetaCmdTypeNoOp, nil)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(noopCmd))

	assert.Len(t, f.Nodes(), 1, "NoOp must not change state")
}

func TestMetaFSM_Snapshot_Restore(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "addr-1:7001", 0)))
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-2", "addr-2:7001", 1)))

	snap, err := f.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(snap))

	nodes := f2.Nodes()
	require.Len(t, nodes, 2)
	ids := map[string]bool{}
	for _, n := range nodes {
		ids[n.ID] = true
	}
	assert.True(t, ids["node-1"])
	assert.True(t, ids["node-2"])
}

func makePutShardGroupCmd(t *testing.T, id string, peers []string) []byte {
	t.Helper()
	data, err := encodeMetaPutShardGroupCmd(ShardGroupEntry{ID: id, PeerIDs: peers})
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypePutShardGroup, data)
	require.NoError(t, err)
	return cmd
}

func TestMetaFSM_Apply_PutShardGroup(t *testing.T) {
	f := NewMetaFSM()
	err := f.applyCmd(makePutShardGroupCmd(t, "group-0", []string{"node-0", "node-1"}))
	require.NoError(t, err)

	groups := f.ShardGroups()
	require.Len(t, groups, 1)
	assert.Equal(t, "group-0", groups[0].ID)
	assert.Equal(t, []string{"node-0", "node-1"}, groups[0].PeerIDs)
}

// TestMetaFSM_OnShardGroupAdded_FiresOnApply verifies the callback registered
// via SetOnShardGroupAdded receives every applied PutShardGroup entry with
// independently allocated PeerIDs (so the callback can keep references safely).
func TestMetaFSM_OnShardGroupAdded_FiresOnApply(t *testing.T) {
	f := NewMetaFSM()

	var got []ShardGroupEntry
	var mu sync.Mutex
	f.SetOnShardGroupAdded(func(e ShardGroupEntry) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, e)
	})

	require.NoError(t, f.applyCmd(makePutShardGroupCmd(t, "g-1", []string{"a", "b", "c"})))
	require.NoError(t, f.applyCmd(makePutShardGroupCmd(t, "g-2", []string{"a", "d", "e"})))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 2)
	assert.Equal(t, "g-1", got[0].ID)
	assert.Equal(t, []string{"a", "b", "c"}, got[0].PeerIDs)
	assert.Equal(t, "g-2", got[1].ID)

	// Ensure the callback received a defensive copy — mutating must not
	// affect the FSM's stored state.
	got[0].PeerIDs[0] = "MUTATED"
	stored := f.ShardGroups()
	for _, g := range stored {
		if g.ID == "g-1" {
			assert.Equal(t, "a", g.PeerIDs[0], "FSM state must be insulated from callback mutation")
		}
	}
}

// TestMetaFSM_OnShardGroupAdded_AsyncCallbackDoesNotBlockApply verifies that
// callers who dispatch the callback to a goroutine do not block the apply
// path. The fix for the cold-start serve.go race: instantiateLocalGroup is
// heavy (BadgerDB+raft.Node), so wrapping in `go func()` keeps apply moving.
//
// REGRESSION GUARD: pre-fix, a slow callback (50ms) ran 8 times serially
// during the seed loop's apply → 400ms blocking → meta-Raft replication
// stalls → no leader. Async callback decouples apply from heavy startup.
func TestMetaFSM_OnShardGroupAdded_AsyncCallbackDoesNotBlockApply(t *testing.T) {
	f := NewMetaFSM()

	asyncDone := make(chan struct{}, 8)
	f.SetOnShardGroupAdded(func(e ShardGroupEntry) {
		go func() {
			time.Sleep(50 * time.Millisecond) // simulate heavy work
			asyncDone <- struct{}{}
		}()
	})

	start := time.Now()
	for i := 0; i < 8; i++ {
		require.NoError(t, f.applyCmd(makePutShardGroupCmd(t,
			fmt.Sprintf("g-%d", i), []string{"a"})))
	}
	elapsed := time.Since(start)

	// Apply path must complete fast (well under sum of callback delays).
	require.Less(t, elapsed, 100*time.Millisecond,
		"apply path took %v — callback blocking?", elapsed)

	// Drain async callbacks.
	for i := 0; i < 8; i++ {
		select {
		case <-asyncDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("async callback %d did not complete", i)
		}
	}
}

func TestMetaFSM_ShardGroups_Snapshot_Restore(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-0", "10.0.0.1:7001", 0)))
	require.NoError(t, f.applyCmd(makePutShardGroupCmd(t, "group-0", []string{"node-0"})))

	snap, err := f.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(snap))

	assert.Len(t, f2.Nodes(), 1)

	groups := f2.ShardGroups()
	require.Len(t, groups, 1)
	assert.Equal(t, "group-0", groups[0].ID)
	assert.Equal(t, []string{"node-0"}, groups[0].PeerIDs)
}

func TestMetaFSM_Apply_UnknownType_Noop(t *testing.T) {
	f := NewMetaFSM()
	// MetaCmdType 255 is unknown — must not panic
	unknownCmd, err := encodeMetaCmd(255, nil)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(unknownCmd))
	assert.Empty(t, f.Nodes())
}

func makePutBucketAssignmentCmd(t *testing.T, bucket, groupID string) []byte {
	t.Helper()
	data, err := encodeMetaPutBucketAssignmentCmd(bucket, groupID)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypePutBucketAssignment, data)
	require.NoError(t, err)
	return cmd
}

func TestMetaFSM_Apply_PutBucketAssignment(t *testing.T) {
	f := NewMetaFSM()
	err := f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0"))
	require.NoError(t, err)

	assignments := f.BucketAssignments()
	require.Len(t, assignments, 1)
	assert.Equal(t, "group-0", assignments["photos"])
}

func TestMetaFSM_Apply_PutBucketAssignment_Overwrite(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0")))
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-1")))

	assignments := f.BucketAssignments()
	require.Len(t, assignments, 1)
	assert.Equal(t, "group-1", assignments["photos"])
}

func TestMetaFSM_BucketAssignments_Snapshot_Restore(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0")))
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "videos", "group-1")))

	snap, err := f.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(snap))

	assignments := f2.BucketAssignments()
	require.Len(t, assignments, 2)
	assert.Equal(t, "group-0", assignments["photos"])
	assert.Equal(t, "group-1", assignments["videos"])
}

func TestMetaFSM_OnBucketAssigned_CallbackFired(t *testing.T) {
	f := NewMetaFSM()
	var cbBucket, cbGroup string
	f.SetOnBucketAssigned(func(bucket, groupID string) {
		cbBucket = bucket
		cbGroup = groupID
	})
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0")))
	assert.Equal(t, "photos", cbBucket)
	assert.Equal(t, "group-0", cbGroup)
}

func TestMetaFSM_Restore_FiresOnBucketAssignedCallback(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0")))
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "videos", "group-1")))

	snap, err := f.Snapshot()
	require.NoError(t, err)

	f2 := NewMetaFSM()
	got := make(map[string]string)
	f2.SetOnBucketAssigned(func(bucket, groupID string) {
		got[bucket] = groupID
	})
	require.NoError(t, f2.Restore(snap))

	assert.Equal(t, map[string]string{"photos": "group-0", "videos": "group-1"}, got)
}

// --- PR-D: LoadSnapshot + RebalancePlan tests ---

func makeSetLoadSnapshotCmd(t *testing.T, entries []LoadStatEntry) []byte {
	t.Helper()
	data, err := encodeMetaSetLoadSnapshotCmd(entries)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeSetLoadSnapshot, data)
	require.NoError(t, err)
	return cmd
}

func makeProposeRebalancePlanCmd(t *testing.T, plan RebalancePlan) []byte {
	t.Helper()
	data, err := encodeMetaProposeRebalancePlanCmd(plan)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeProposeRebalancePlan, data)
	require.NoError(t, err)
	return cmd
}

func makeAbortPlanCmd(t *testing.T, planID string) []byte {
	t.Helper()
	return makeAbortPlanCmdWithReason(t, planID, clusterpb.AbortPlanReasonUnknown)
}

func makeAbortPlanCmdWithReason(t *testing.T, planID string, reason clusterpb.AbortPlanReason) []byte {
	t.Helper()
	data, err := encodeMetaAbortPlanCmd(planID, reason)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeAbortPlan, data)
	require.NoError(t, err)
	return cmd
}

func TestMetaFSM_Apply_SetLoadSnapshot(t *testing.T) {
	f := NewMetaFSM()
	entries := []LoadStatEntry{
		{NodeID: "n1", DiskUsedPct: 80.0, DiskAvailBytes: 1000},
		{NodeID: "n2", DiskUsedPct: 20.0, DiskAvailBytes: 9000},
	}
	require.NoError(t, f.applyCmd(makeSetLoadSnapshotCmd(t, entries)))

	snap := f.LoadSnapshot()
	require.Len(t, snap, 2)
	assert.InDelta(t, 80.0, snap["n1"].DiskUsedPct, 0.01)
}

func TestMetaFSM_Apply_ProposeRebalancePlan(t *testing.T) {
	f := NewMetaFSM()
	plan := RebalancePlan{
		PlanID:    "plan-1",
		GroupID:   "group-0",
		FromNode:  "n1",
		ToNode:    "n2",
		CreatedAt: time.Now(),
	}
	require.NoError(t, f.applyCmd(makeProposeRebalancePlanCmd(t, plan)))
	assert.Equal(t, "plan-1", f.ActivePlanID())
}

func TestMetaFSM_Apply_ProposeRebalancePlan_RejectsIfActive(t *testing.T) {
	f := NewMetaFSM()
	plan1 := RebalancePlan{PlanID: "plan-1", GroupID: "g0", FromNode: "n1", ToNode: "n2", CreatedAt: time.Now()}
	plan2 := RebalancePlan{PlanID: "plan-2", GroupID: "g0", FromNode: "n1", ToNode: "n3", CreatedAt: time.Now()}
	require.NoError(t, f.applyCmd(makeProposeRebalancePlanCmd(t, plan1)))
	require.ErrorContains(t, f.applyCmd(makeProposeRebalancePlanCmd(t, plan2)), "active plan")
}

func TestMetaFSM_Apply_AbortPlan(t *testing.T) {
	f := NewMetaFSM()
	plan := RebalancePlan{PlanID: "plan-1", GroupID: "g0", FromNode: "n1", ToNode: "n2", CreatedAt: time.Now()}
	require.NoError(t, f.applyCmd(makeProposeRebalancePlanCmd(t, plan)))
	require.NoError(t, f.applyCmd(makeAbortPlanCmd(t, "plan-1")))
	assert.Empty(t, f.ActivePlanID())
}

func TestMetaFSM_Apply_AbortPlan_Idempotent(t *testing.T) {
	f := NewMetaFSM()
	// Aborting when no plan is active must be a no-op (not an error).
	require.NoError(t, f.applyCmd(makeAbortPlanCmd(t, "nonexistent")))
	assert.Empty(t, f.ActivePlanID())
}

// TestEncodeMetaAbortPlanCmd_ReasonRoundTrip verifies that every AbortPlanReason
// value survives a FlatBuffers encode → decode cycle intact.
func TestEncodeMetaAbortPlanCmd_ReasonRoundTrip(t *testing.T) {
	cases := []struct {
		reason clusterpb.AbortPlanReason
		name   string
	}{
		{clusterpb.AbortPlanReasonUnknown, "Unknown"},
		{clusterpb.AbortPlanReasonTimeout, "Timeout"},
		{clusterpb.AbortPlanReasonExecutionFailed, "ExecutionFailed"},
		{clusterpb.AbortPlanReasonCompleted, "Completed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := encodeMetaAbortPlanCmd("plan-rt", tc.reason)
			require.NoError(t, err)
			cmd := clusterpb.GetRootAsMetaAbortPlanCmd(data, 0)
			assert.Equal(t, tc.reason, cmd.Reason())
			assert.Equal(t, tc.name, cmd.Reason().String())
		})
	}
}

func TestMetaFSM_OnRebalancePlan_CallbackFired(t *testing.T) {
	f := NewMetaFSM()
	var got *RebalancePlan
	f.SetOnRebalancePlan(func(p *RebalancePlan) { got = p })

	plan := RebalancePlan{PlanID: "plan-1", GroupID: "g0", FromNode: "n1", ToNode: "n2", CreatedAt: time.Now()}
	require.NoError(t, f.applyCmd(makeProposeRebalancePlanCmd(t, plan)))
	require.NotNil(t, got)
	assert.Equal(t, "plan-1", got.PlanID)
}

func TestMetaFSM_LoadSnapshot_Snapshot_Restore(t *testing.T) {
	f := NewMetaFSM()
	entries := []LoadStatEntry{{NodeID: "n1", DiskUsedPct: 75.0}}
	require.NoError(t, f.applyCmd(makeSetLoadSnapshotCmd(t, entries)))

	snap, err := f.Snapshot()
	require.NoError(t, err)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(snap))
	ls := f2.LoadSnapshot()
	assert.InDelta(t, 75.0, ls["n1"].DiskUsedPct, 0.01)
}

func TestMetaFSM_ActivePlan_Snapshot_Restore(t *testing.T) {
	f := NewMetaFSM()
	plan := RebalancePlan{PlanID: "plan-99", GroupID: "g0", FromNode: "n1", ToNode: "n2", CreatedAt: time.Now()}
	require.NoError(t, f.applyCmd(makeProposeRebalancePlanCmd(t, plan)))

	snap, err := f.Snapshot()
	require.NoError(t, err)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(snap))
	assert.Equal(t, "plan-99", f2.ActivePlanID())
}
