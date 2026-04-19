package scrubber_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// orphanBackend embeds mockBackend and adds OrphanWalkable support.
type orphanBackend struct {
	*mockBackend
	orphanDirs    map[string]time.Time // dir → creation time (age gate simulation)
	deletedOrphan []string
}

func newOrphanBackend() *orphanBackend {
	return &orphanBackend{
		mockBackend: newMockBackend(),
		orphanDirs:  make(map[string]time.Time),
	}
}

// addOrphan registers a shard dir that exists on disk but has no metadata object.
// age controls how long ago it was created (for age gate simulation).
func (o *orphanBackend) addOrphan(dir string, age time.Duration) {
	o.orphanDirs[dir] = time.Now().Add(-age)
}

// WalkOrphanShards calls fn for each orphan dir not in known and older than 5 minutes.
func (o *orphanBackend) WalkOrphanShards(known map[string]bool, fn func(string) error) error {
	const minAge = 5 * time.Minute
	now := time.Now()
	for dir, createdAt := range o.orphanDirs {
		if now.Sub(createdAt) < minAge {
			continue // age gate: too young, skip
		}
		if !known[dir] {
			if err := fn(dir); err != nil {
				return err
			}
		}
	}
	return nil
}

// DeleteOrphanDir removes the orphan dir from the mock and records the deletion.
func (o *orphanBackend) DeleteOrphanDir(dir string) (int, error) {
	delete(o.orphanDirs, dir)
	o.deletedOrphan = append(o.deletedOrphan, dir)
	return 2, nil // simulate 2 shard files removed
}

// addKnownObject registers an object in the mock so its shard dir appears in knownDirs.
func (o *orphanBackend) addKnownObject(bucket, key string) {
	rec := scrubber.ObjectRecord{
		Bucket:       bucket,
		Key:          key,
		DataShards:   4,
		ParityShards: 2,
	}
	o.records[bucket] = append(o.records[bucket], rec)
	// Store all healthy shards so the scrubber doesn't try to repair them.
	for i := range 6 {
		o.shards[fmt.Sprintf("%s/%s/%d", bucket, key, i)] = []byte("shard")
	}
}

// shardDir returns the expected shard directory for bucket/key in the mock.
func shardDir(bucket, key string) string {
	return fmt.Sprintf("%s/%s", bucket, key)
}

// ----------------------------------------------------------------------------
// Age gate tests
// ----------------------------------------------------------------------------

// TestOrphanSweep_AgeGate_TooRecent: a shard dir < 5min old is never deleted.
func TestOrphanSweep_AgeGate_TooRecent(t *testing.T) {
	b := newOrphanBackend()
	b.addOrphan(shardDir("bucket", "orphan-key"), 2*time.Minute) // too young

	s := scrubber.New(b, time.Hour, scrubber.WithNoRetry())
	ctx := context.Background()

	s.RunOnce(ctx) // cycle 1
	s.RunOnce(ctx) // cycle 2

	assert.Empty(t, b.deletedOrphan, "orphan < 5min old must not be deleted")
}

// TestOrphanSweep_AgeGate_Old: a shard dir > 5min old is deleted after 2 cycles.
func TestOrphanSweep_AgeGate_Old(t *testing.T) {
	b := newOrphanBackend()
	b.addOrphan(shardDir("bucket", "orphan-key"), 10*time.Minute) // old enough

	s := scrubber.New(b, time.Hour, scrubber.WithNoRetry())
	ctx := context.Background()

	s.RunOnce(ctx) // cycle 1: tombstone
	s.RunOnce(ctx) // cycle 2: delete

	require.Len(t, b.deletedOrphan, 1)
	assert.Equal(t, shardDir("bucket", "orphan-key"), b.deletedOrphan[0])
}

// ----------------------------------------------------------------------------
// Tombstone delay tests
// ----------------------------------------------------------------------------

// TestOrphanSweep_TombstoneDelay: first cycle tombstones; second cycle deletes.
func TestOrphanSweep_TombstoneDelay(t *testing.T) {
	b := newOrphanBackend()
	b.addOrphan(shardDir("bucket", "orphan-key"), 10*time.Minute)

	s := scrubber.New(b, time.Hour, scrubber.WithNoRetry())
	ctx := context.Background()

	s.RunOnce(ctx)
	assert.Empty(t, b.deletedOrphan, "first cycle must NOT delete — only tombstone")

	s.RunOnce(ctx)
	assert.Len(t, b.deletedOrphan, 1, "second cycle must delete the confirmed orphan")
}

// ----------------------------------------------------------------------------
// Cap tests
// ----------------------------------------------------------------------------

// TestOrphanSweep_MaxOrphansPerCycle: deletion is capped at maxOrphansPerCycle per cycle.
func TestOrphanSweep_MaxOrphansPerCycle(t *testing.T) {
	b := newOrphanBackend()
	for i := range 100 {
		b.addOrphan(shardDir("bucket", fmt.Sprintf("orphan-%d", i)), 10*time.Minute)
	}

	s := scrubber.New(b, time.Hour, scrubber.WithNoRetry())
	ctx := context.Background()

	s.RunOnce(ctx) // cycle 1: tombstone all 100, delete 0
	assert.Empty(t, b.deletedOrphan, "first cycle: tombstone only, no deletions")

	s.RunOnce(ctx) // cycle 2: delete up to 50 (maxOrphansPerCycle)
	assert.LessOrEqual(t, len(b.deletedOrphan), 50,
		"deletions must not exceed maxOrphansPerCycle=50 per cycle")
	assert.NotEmpty(t, b.deletedOrphan, "some orphans must be deleted on second cycle")
}

// ----------------------------------------------------------------------------
// Edge cases
// ----------------------------------------------------------------------------

// TestOrphanSweep_EmptyDataDir: no orphans → no panic, no deletions.
func TestOrphanSweep_EmptyDataDir(t *testing.T) {
	b := newOrphanBackend()

	s := scrubber.New(b, time.Hour, scrubber.WithNoRetry())
	ctx := context.Background()

	require.NotPanics(t, func() {
		s.RunOnce(ctx)
		s.RunOnce(ctx)
	})
	assert.Empty(t, b.deletedOrphan)
}

// TestOrphanSweep_KnownObjectNotDeleted: a dir present in both disk and metadata is NOT orphan.
func TestOrphanSweep_KnownObjectNotDeleted(t *testing.T) {
	b := newOrphanBackend()
	b.addKnownObject("bucket", "known-key")
	// Also register as an orphan dir — but it IS in metadata, so not orphaned.
	b.orphanDirs[shardDir("bucket", "known-key")] = time.Now().Add(-10 * time.Minute)

	s := scrubber.New(b, time.Hour, scrubber.WithNoRetry())
	ctx := context.Background()

	s.RunOnce(ctx)
	s.RunOnce(ctx)

	assert.Empty(t, b.deletedOrphan, "dir with live metadata must not be deleted")
}

// ----------------------------------------------------------------------------
// Crash simulation test
// ----------------------------------------------------------------------------

// TestOrphanSweep_CrashBetweenPhase3And4 simulates the Phase 3→4 crash gap:
// CmdMigrationDone is applied (object removed from src metadata), but before
// the Phase 4 src shard delete occurs, the process crashes. The scrubber must
// detect and clean up the stranded shard directory within 2 scrub cycles.
func TestOrphanSweep_CrashBetweenPhase3And4(t *testing.T) {
	b := newOrphanBackend()

	// The shard dir exists on disk (old enough) but has no metadata entry.
	orphanDir := shardDir("src-bucket", "migrated-key")
	b.addOrphan(orphanDir, 6*time.Minute) // 6 min > 5 min age gate

	// No records in mock → ScanObjects returns nothing → knownDirs is empty.
	// WalkOrphanShards finds orphanDir not in knownDirs.

	s := scrubber.New(b, time.Hour, scrubber.WithNoRetry())
	ctx := context.Background()

	s.RunOnce(ctx) // cycle 1: orphan found → tombstoned
	assert.Empty(t, b.deletedOrphan, "after cycle 1: tombstoned only, not deleted")

	s.RunOnce(ctx) // cycle 2: still orphan → deleted
	require.Len(t, b.deletedOrphan, 1)
	assert.Equal(t, orphanDir, b.deletedOrphan[0], "orphan shard dir must be cleaned up")
}
