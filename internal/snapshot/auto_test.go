package snapshot_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
)

// mockSnapshotable is a minimal Snapshotable for testing.
type mockSnapshotable struct {
	objects []storage.SnapshotObject
}

func (m *mockSnapshotable) ListAllObjects() ([]storage.SnapshotObject, error) {
	return m.objects, nil
}

func (m *mockSnapshotable) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	m.objects = objects
	return len(objects), nil, nil
}

func TestAutoSnapshotter_CreatesSnapshotOnInterval(t *testing.T) {
	dir := t.TempDir()
	mgr, err := snapshot.NewManager(dir, &mockSnapshotable{}, "")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	interval := 100 * time.Millisecond
	as := snapshot.NewAutoSnapshotter(mgr, interval, 5)
	as.Start(ctx)

	// Wait long enough for at least 2 snapshots
	time.Sleep(350 * time.Millisecond)
	cancel()
	as.Wait()

	snaps, err := mgr.List()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(snaps), 2, "at least 2 snapshots should have been created")
}

func TestAutoSnapshotter_RespectsRetention(t *testing.T) {
	dir := t.TempDir()
	mgr, err := snapshot.NewManager(dir, &mockSnapshotable{}, "")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	const maxRetain = 3
	interval := 80 * time.Millisecond
	as := snapshot.NewAutoSnapshotter(mgr, interval, maxRetain)
	as.Start(ctx)

	// Wait long enough to exceed retention
	time.Sleep(600 * time.Millisecond)
	cancel()
	as.Wait()

	snaps, err := mgr.List()
	require.NoError(t, err)
	assert.LessOrEqual(t, len(snaps), maxRetain,
		"auto-snapshotter must not keep more than maxRetain snapshots")
}

// TestAutoSnapshotter_PruneOld_PreservesManual verifies that manual snapshots
// (Reason != "auto") are NOT deleted even when total count exceeds maxRetain.
// Regression for Known Issue #2.
func TestAutoSnapshotter_PruneOld_PreservesManual(t *testing.T) {
	dir := t.TempDir()
	mgr, err := snapshot.NewManager(dir, &mockSnapshotable{}, "")
	require.NoError(t, err)

	// Create 2 manual snapshots
	for i := 0; i < 2; i++ {
		_, err := mgr.Create("manual")
		require.NoError(t, err)
	}
	// Create 3 auto snapshots
	for i := 0; i < 3; i++ {
		_, err := mgr.Create("auto")
		require.NoError(t, err)
	}

	// maxRetain=1 means we want at most 1 auto snapshot kept.
	// Running the auto-snapshotter's internal prune via a manual trigger:
	as := snapshot.NewAutoSnapshotter(mgr, time.Hour, 1)
	snapshot.RunPruneOld(as) // test-only helper

	snaps, err := mgr.List()
	require.NoError(t, err)

	var autoCount, manualCount int
	for _, s := range snaps {
		if s.Reason == "auto" {
			autoCount++
		} else {
			manualCount++
		}
	}
	assert.Equal(t, 2, manualCount, "all manual snapshots must be preserved")
	assert.LessOrEqual(t, autoCount, 1, "auto snapshots must be pruned to maxRetain")
}

// TestAutoSnapshotter_LegacyReason verifies that snapshots with empty Reason
// (from before the reason field was populated) are treated as auto snapshots.
func TestAutoSnapshotter_LegacyReason(t *testing.T) {
	dir := t.TempDir()
	mgr, err := snapshot.NewManager(dir, &mockSnapshotable{}, "")
	require.NoError(t, err)

	// Create 3 "legacy" snapshots with empty reason (simulated by passing "")
	for i := 0; i < 3; i++ {
		_, err := mgr.Create("")
		require.NoError(t, err)
	}

	as := snapshot.NewAutoSnapshotter(mgr, time.Hour, 1)
	snapshot.RunPruneOld(as)

	snaps, err := mgr.List()
	require.NoError(t, err)
	assert.LessOrEqual(t, len(snaps), 1, "legacy empty-reason snapshots must be treated as auto and pruned")
}

// TestAutoSnapshotter_AllManual_NoOp verifies that a population of only manual
// snapshots is never touched by prune, even far exceeding maxRetain.
func TestAutoSnapshotter_AllManual_NoOp(t *testing.T) {
	dir := t.TempDir()
	mgr, err := snapshot.NewManager(dir, &mockSnapshotable{}, "")
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		_, err := mgr.Create("manual")
		require.NoError(t, err)
	}

	as := snapshot.NewAutoSnapshotter(mgr, time.Hour, 1)
	snapshot.RunPruneOld(as)

	snaps, err := mgr.List()
	require.NoError(t, err)
	assert.Equal(t, 5, len(snaps), "no manual snapshots may be deleted")
}

func TestAutoSnapshotter_StopsOnContextCancel(t *testing.T) {
	dir := t.TempDir()
	mgr, err := snapshot.NewManager(dir, &mockSnapshotable{}, "")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	as := snapshot.NewAutoSnapshotter(mgr, 50*time.Millisecond, 10)
	as.Start(ctx)

	time.Sleep(80 * time.Millisecond)
	cancel()
	as.Wait() // must return promptly

	snapsBefore, _ := mgr.List()
	count := len(snapsBefore)

	time.Sleep(200 * time.Millisecond)
	snapsAfter, _ := mgr.List()
	assert.Equal(t, count, len(snapsAfter), "no new snapshots after context cancel")
}
