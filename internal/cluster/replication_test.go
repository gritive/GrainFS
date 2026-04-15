package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicationMonitor_DetectsMissingReplicas(t *testing.T) {
	rm := NewReplicationMonitor(2) // want 2 replicas per object

	rm.RecordReplica("bucket/key1", "node-a")
	rm.RecordReplica("bucket/key1", "node-b")
	rm.RecordReplica("bucket/key2", "node-a") // only 1 replica

	under := rm.UnderReplicated()
	require.Len(t, under, 1)
	assert.Equal(t, "bucket/key2", under[0].Key)
	assert.Equal(t, 1, under[0].Current)
	assert.Equal(t, 2, under[0].Desired)
}

func TestReplicationMonitor_NodeRemovalCreatesDeficit(t *testing.T) {
	rm := NewReplicationMonitor(2)

	rm.RecordReplica("bucket/key1", "node-a")
	rm.RecordReplica("bucket/key1", "node-b")

	assert.Empty(t, rm.UnderReplicated())

	rm.RemoveNode("node-b")
	under := rm.UnderReplicated()
	require.Len(t, under, 1)
	assert.Equal(t, "bucket/key1", under[0].Key)
}

func TestReplicationMonitor_NoDeficitWhenFull(t *testing.T) {
	rm := NewReplicationMonitor(2)

	rm.RecordReplica("bucket/key1", "node-a")
	rm.RecordReplica("bucket/key1", "node-b")

	assert.Empty(t, rm.UnderReplicated())
}

func TestReplicationMonitor_ClearKey(t *testing.T) {
	rm := NewReplicationMonitor(2)

	rm.RecordReplica("bucket/key1", "node-a")
	rm.ClearKey("bucket/key1")

	assert.Empty(t, rm.UnderReplicated())
}

func TestReRepairJob_SchedulesRepair(t *testing.T) {
	rm := NewReplicationMonitor(2)
	rm.RecordReplica("bucket/key1", "node-a")
	// Only 1 replica, need 2

	repairs := rm.PlanRepairs([]string{"node-a", "node-b", "node-c"})
	require.Len(t, repairs, 1)
	assert.Equal(t, "bucket/key1", repairs[0].Key)
	assert.Equal(t, "node-a", repairs[0].SourceNode)
	assert.NotEqual(t, "node-a", repairs[0].TargetNode) // target must be different from source

	// Verify target is one of the remaining nodes
	assert.Contains(t, []string{"node-b", "node-c"}, repairs[0].TargetNode)
}

func TestReRepairJob_NoRepairsNeeded(t *testing.T) {
	rm := NewReplicationMonitor(2)
	rm.RecordReplica("bucket/key1", "node-a")
	rm.RecordReplica("bucket/key1", "node-b")

	repairs := rm.PlanRepairs([]string{"node-a", "node-b"})
	assert.Empty(t, repairs)
}

func TestReplicationMonitor_ConcurrentAccess(t *testing.T) {
	rm := NewReplicationMonitor(2)
	done := make(chan struct{})

	go func() {
		for i := 0; i < 100; i++ {
			rm.RecordReplica("bucket/key", "node-a")
		}
		close(done)
	}()

	for i := 0; i < 100; i++ {
		_ = rm.UnderReplicated()
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent access timed out")
	}
}
