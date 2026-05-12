package snapshot_test

import (
	"context"
	"sync/atomic"
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

// fakePolicy is a test-only SnapshotPolicy with mutable values for hot-reload tests.
type fakePolicy struct {
	interval atomic.Int64 // nanoseconds
	retain   atomic.Int32
}

func (p *fakePolicy) SnapshotInterval() time.Duration { return time.Duration(p.interval.Load()) }
func (p *fakePolicy) SnapshotRetain() int32           { return p.retain.Load() }

func newFakePolicy(interval time.Duration, retain int32) *fakePolicy {
	p := &fakePolicy{}
	p.interval.Store(int64(interval))
	p.retain.Store(retain)
	return p
}

// countAutoSnaps counts auto-created snapshots (Reason == "auto" || "").
func countAutoSnaps(t *testing.T, mgr *snapshot.Manager) int {
	t.Helper()
	snaps, err := mgr.List()
	require.NoError(t, err)
	n := 0
	for _, s := range snaps {
		if s.Reason == "auto" || s.Reason == "" {
			n++
		}
	}
	return n
}

func TestAutoSnapshotter_CreatesSnapshotOnInterval(t *testing.T) {
	dir := t.TempDir()
	mgr, err := snapshot.NewManager(dir, &mockSnapshotable{}, "")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pol := newFakePolicy(100*time.Millisecond, 5)
	as := snapshot.NewAutoSnapshotter(mgr, pol, 10*time.Millisecond)
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

	const maxRetain int32 = 3
	pol := newFakePolicy(80*time.Millisecond, maxRetain)
	as := snapshot.NewAutoSnapshotter(mgr, pol, 10*time.Millisecond)
	as.Start(ctx)

	// Wait long enough to exceed retention
	time.Sleep(600 * time.Millisecond)
	cancel()
	as.Wait()

	snaps, err := mgr.List()
	require.NoError(t, err)
	assert.LessOrEqual(t, len(snaps), int(maxRetain),
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
	pol := newFakePolicy(time.Hour, 1)
	as := snapshot.NewAutoSnapshotter(mgr, pol, 0)
	snapshot.RunPruneOld(as, 1) // test-only helper

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

	pol := newFakePolicy(time.Hour, 1)
	as := snapshot.NewAutoSnapshotter(mgr, pol, 0)
	snapshot.RunPruneOld(as, 1)

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

	pol := newFakePolicy(time.Hour, 1)
	as := snapshot.NewAutoSnapshotter(mgr, pol, 0)
	snapshot.RunPruneOld(as, 1)

	snaps, err := mgr.List()
	require.NoError(t, err)
	assert.Equal(t, 5, len(snaps), "no manual snapshots may be deleted")
}

func TestAutoSnapshotter_StopsOnContextCancel(t *testing.T) {
	dir := t.TempDir()
	mgr, err := snapshot.NewManager(dir, &mockSnapshotable{}, "")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	pol := newFakePolicy(50*time.Millisecond, 10)
	as := snapshot.NewAutoSnapshotter(mgr, pol, 10*time.Millisecond)
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

// TestAutoSnapshotter_HotReloadInterval verifies that changing
// SnapshotInterval at runtime takes effect within bounded latency:
// - interval=50ms produces snapshots
// - interval=0 stops producing (idle poll)
// - interval=20ms resumes producing
func TestAutoSnapshotter_HotReloadInterval(t *testing.T) {
	dir := t.TempDir()
	mgr, err := snapshot.NewManager(dir, &mockSnapshotable{}, "")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pol := newFakePolicy(50*time.Millisecond, 100)
	as := snapshot.NewAutoSnapshotter(mgr, pol, 20*time.Millisecond)
	as.Start(ctx)

	// Phase 1: interval=50ms → expect at least 1 snapshot after 300ms
	time.Sleep(300 * time.Millisecond)
	phase1 := countAutoSnaps(t, mgr)
	assert.GreaterOrEqual(t, phase1, 1, "phase1: expected at least 1 snapshot with interval=50ms")

	// Phase 2: disable → snapshot count must not grow significantly.
	// One in-flight wait of up to 50ms may still fire before the disabled
	// re-check kicks in, so allow at most 1 additional snapshot.
	pol.interval.Store(0)
	// Wait long enough that any pending 50ms tick fires + idle re-checks
	// observe the disabled state (idle=20ms, so several re-checks fit).
	time.Sleep(200 * time.Millisecond)
	phase2 := countAutoSnaps(t, mgr)
	assert.LessOrEqual(t, phase2, phase1+1,
		"phase2: snapshot count must not grow more than 1 (in-flight tick) after disable; phase1=%d phase2=%d",
		phase1, phase2)

	// Phase 3: re-enable with shorter interval → expect growth resumes.
	pol.interval.Store(int64(20 * time.Millisecond))
	time.Sleep(200 * time.Millisecond)
	phase3 := countAutoSnaps(t, mgr)
	assert.Greater(t, phase3, phase2,
		"phase3: snapshot count must grow after re-enable; phase2=%d phase3=%d", phase2, phase3)

	cancel()
	as.Wait()
}

// TestAutoSnapshotter_HotReloadRetain verifies that lowering SnapshotRetain
// at runtime causes the next prune cycle to drop excess auto snapshots.
func TestAutoSnapshotter_HotReloadRetain(t *testing.T) {
	dir := t.TempDir()
	mgr, err := snapshot.NewManager(dir, &mockSnapshotable{}, "")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pol := newFakePolicy(20*time.Millisecond, 5)
	as := snapshot.NewAutoSnapshotter(mgr, pol, 10*time.Millisecond)
	as.Start(ctx)

	// Accumulate up to retain=5 snapshots.
	require.Eventually(t, func() bool {
		return countAutoSnaps(t, mgr) >= 5
	}, 2*time.Second, 10*time.Millisecond, "should reach retain=5 auto snapshots")

	// Snapshot count should stabilize at ~5 (no more than retain).
	time.Sleep(100 * time.Millisecond)
	before := countAutoSnaps(t, mgr)
	assert.LessOrEqual(t, before, 5, "should be capped at retain=5")

	// Lower retain to 2; subsequent takeAndPrune cycles must drop excess.
	pol.retain.Store(2)
	require.Eventually(t, func() bool {
		return countAutoSnaps(t, mgr) <= 2
	}, 2*time.Second, 10*time.Millisecond, "should prune down to retain=2")

	cancel()
	as.Wait()
}
