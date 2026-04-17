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
