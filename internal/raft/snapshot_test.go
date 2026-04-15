package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSnapshotter implements the Snapshotter interface for testing.
type mockSnapshotter struct {
	snapshotData []byte
	snapshotErr  error
	restoreErr   error
	restoreCalls int
	restoredData []byte
}

func (m *mockSnapshotter) Snapshot() ([]byte, error) {
	return m.snapshotData, m.snapshotErr
}

func (m *mockSnapshotter) Restore(data []byte) error {
	m.restoreCalls++
	m.restoredData = data
	return m.restoreErr
}

// makeTestEntries creates and appends n log entries starting at startIndex with the given term.
func makeTestEntries(t *testing.T, store LogStore, term, startIndex uint64, n int) {
	t.Helper()
	entries := make([]LogEntry, n)
	for i := range entries {
		entries[i] = LogEntry{Term: term, Index: startIndex + uint64(i), Command: []byte("cmd")}
	}
	require.NoError(t, store.AppendEntries(entries))
}

func TestSnapshotManager_TriggerOnThreshold(t *testing.T) {
	store := setupTestStore(t)
	snap := &mockSnapshotter{snapshotData: []byte("snap-state")}

	mgr := NewSnapshotManager(store, snap, SnapshotConfig{
		Threshold: 5, // snapshot every 5 entries
	})

	makeTestEntries(t, store, 1, 1, 5)

	triggered := mgr.MaybeTrigger(5, 1)
	assert.True(t, triggered, "snapshot should trigger at threshold")

	// Verify snapshot was saved
	idx, term, data, err := store.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, uint64(5), idx)
	assert.Equal(t, uint64(1), term)
	assert.Equal(t, []byte("snap-state"), data)
}

func TestSnapshotManager_NoTriggerBelowThreshold(t *testing.T) {
	store := setupTestStore(t)
	snap := &mockSnapshotter{snapshotData: []byte("snap")}

	mgr := NewSnapshotManager(store, snap, SnapshotConfig{
		Threshold: 10,
	})

	makeTestEntries(t, store, 1, 1, 3)

	triggered := mgr.MaybeTrigger(3, 1)
	assert.False(t, triggered, "should not trigger below threshold")

	// No snapshot saved
	_, _, data, err := store.LoadSnapshot()
	require.NoError(t, err)
	assert.Nil(t, data)
}

func TestSnapshotManager_CompactsLogAfterSnapshot(t *testing.T) {
	store := setupTestStore(t)
	snap := &mockSnapshotter{snapshotData: []byte("snap")}

	mgr := NewSnapshotManager(store, snap, SnapshotConfig{
		Threshold: 5,
	})

	makeTestEntries(t, store, 1, 1, 10)

	// Trigger snapshot at index 10
	triggered := mgr.MaybeTrigger(10, 1)
	require.True(t, triggered)

	// Entries up to snapshot index should be compacted
	lastIdx, err := store.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), lastIdx, "log should be compacted after snapshot")
}

func TestSnapshotManager_RestoreOnStartup(t *testing.T) {
	store := setupTestStore(t)
	snap := &mockSnapshotter{snapshotData: []byte("snap")}

	// Save a snapshot manually
	require.NoError(t, store.SaveSnapshot(5, 2, []byte("saved-state")))

	mgr := NewSnapshotManager(store, snap, SnapshotConfig{
		Threshold: 10,
	})

	// Restore should load and apply the snapshot
	idx, err := mgr.Restore()
	require.NoError(t, err)
	assert.Equal(t, uint64(5), idx)
	assert.Equal(t, 1, snap.restoreCalls)
	assert.Equal(t, []byte("saved-state"), snap.restoredData)
}

func TestSnapshotManager_RestoreNoSnapshot(t *testing.T) {
	store := setupTestStore(t)
	snap := &mockSnapshotter{}

	mgr := NewSnapshotManager(store, snap, SnapshotConfig{
		Threshold: 10,
	})

	idx, err := mgr.Restore()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), idx, "no snapshot means index 0")
	assert.Equal(t, 0, snap.restoreCalls, "should not call restore with no snapshot")
}

func TestSnapshotManager_ConsecutiveSnapshots(t *testing.T) {
	store := setupTestStore(t)
	snap := &mockSnapshotter{snapshotData: []byte("state")}

	mgr := NewSnapshotManager(store, snap, SnapshotConfig{
		Threshold: 5,
	})

	// First batch: entries 1-5
	makeTestEntries(t, store, 1, 1, 5)
	triggered := mgr.MaybeTrigger(5, 1)
	require.True(t, triggered)

	// Second batch: entries 6-10
	makeTestEntries(t, store, 1, 6, 5)
	triggered = mgr.MaybeTrigger(10, 1)
	require.True(t, triggered)

	// Snapshot should be at index 10
	idx, term, _, err := store.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, uint64(10), idx)
	assert.Equal(t, uint64(1), term)
}

func TestSnapshotManager_SnapshotError(t *testing.T) {
	store := setupTestStore(t)
	snap := &mockSnapshotter{snapshotErr: fmt.Errorf("snapshot failed")}

	mgr := NewSnapshotManager(store, snap, SnapshotConfig{
		Threshold: 5,
	})

	makeTestEntries(t, store, 1, 1, 5)

	triggered := mgr.MaybeTrigger(5, 1)
	assert.False(t, triggered, "should not trigger when Snapshot() fails")

	// No snapshot should be saved
	_, _, data, err := store.LoadSnapshot()
	require.NoError(t, err)
	assert.Nil(t, data)
}

func TestSnapshotManager_SaveSnapshotError(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)

	snap := &mockSnapshotter{snapshotData: []byte("state")}
	mgr := NewSnapshotManager(store, snap, SnapshotConfig{
		Threshold: 5,
	})

	makeTestEntries(t, store, 1, 1, 5)

	// Close store to force SaveSnapshot error
	store.Close()

	triggered := mgr.MaybeTrigger(5, 1)
	assert.False(t, triggered, "should not trigger when SaveSnapshot fails")
}

func TestSnapshotManager_RestoreError(t *testing.T) {
	store := setupTestStore(t)
	snap := &mockSnapshotter{restoreErr: fmt.Errorf("restore failed")}

	// Save a snapshot manually
	require.NoError(t, store.SaveSnapshot(5, 2, []byte("saved-state")))

	mgr := NewSnapshotManager(store, snap, SnapshotConfig{
		Threshold: 10,
	})

	idx, err := mgr.Restore()
	assert.Error(t, err, "should return error when Restore() fails")
	assert.Equal(t, uint64(0), idx)
	assert.Equal(t, 1, snap.restoreCalls, "should have attempted restore")
}

// --- Leader Transfer tests ---

func TestLeaderTransfer_StepsDown(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)
	termBefore := leader.Term()

	// Transfer leadership (simple step-down)
	err := leader.TransferLeadership()
	require.NoError(t, err)

	// Immediately after transfer, the old leader should be a follower
	assert.Equal(t, Follower, leader.State(), "leader should step down to follower")

	// Eventually a leader should be elected (may be the same or different node)
	newLeader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, newLeader, "cluster should elect a leader after transfer")

	// Term must advance (a new election happened)
	assert.Greater(t, newLeader.Term(), termBefore, "term should advance after re-election")
}

func TestLeaderTransfer_NotLeader(t *testing.T) {
	config := DefaultConfig("A", []string{"B", "C"})
	node := NewNode(config)
	// Node starts as Follower
	err := node.TransferLeadership()
	assert.Equal(t, ErrNotLeader, err)
}

func TestLeaderTransfer_SingleNode(t *testing.T) {
	config := DefaultConfig("A", nil)
	node := NewNode(config)
	node.Start()
	defer node.Stop()

	require.Eventually(t, func() bool {
		return node.State() == Leader
	}, 3*time.Second, 10*time.Millisecond, "single node should become leader")

	// Single node can't transfer (no peers)
	err := node.TransferLeadership()
	assert.Error(t, err, "should fail with no peers to transfer to")
}
