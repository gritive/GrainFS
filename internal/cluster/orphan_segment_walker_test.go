package cluster

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// backDate sets mtime to (now - dur) on path.
func backDate(t *testing.T, path string, dur time.Duration) {
	t.Helper()
	past := time.Now().Add(-dur)
	require.NoError(t, os.Chtimes(path, past, past))
}

func TestWalkOrphanSegments_Production(t *testing.T) {
	tmpRoot := t.TempDir()
	segDir := filepath.Join(tmpRoot, "data", "bucket", "key_segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	// 3 files: old (orphan), young (skip), known (skip).
	oldPath := filepath.Join(segDir, "blob-old")
	require.NoError(t, os.WriteFile(oldPath, []byte("seg"), 0o644))
	backDate(t, oldPath, 10*time.Minute)

	youngPath := filepath.Join(segDir, "blob-young")
	require.NoError(t, os.WriteFile(youngPath, []byte("seg"), 0o644))

	knownPath := filepath.Join(segDir, "blob-known")
	require.NoError(t, os.WriteFile(knownPath, []byte("seg"), 0o644))
	backDate(t, knownPath, 10*time.Minute)

	b := &DistributedBackend{root: tmpRoot, scrubOrphanAge: 5 * time.Minute}
	known := map[string]bool{"bucket/key_segments/blob-known": true}
	var found []string
	err := b.WalkOrphanSegments("bucket", known, func(p string) error {
		found = append(found, p)
		return nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"bucket/key_segments/blob-old"}, found)
}

func TestWalkOrphanSegments_NestedKey(t *testing.T) {
	tmpRoot := t.TempDir()
	// Nested key "folder/sub/file"
	segDir := filepath.Join(tmpRoot, "data", "bucket", "folder", "sub", "file_segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))

	oldPath := filepath.Join(segDir, "blob-old")
	require.NoError(t, os.WriteFile(oldPath, []byte("seg"), 0o644))
	backDate(t, oldPath, 10*time.Minute)

	b := &DistributedBackend{root: tmpRoot, scrubOrphanAge: 5 * time.Minute}
	var found []string
	err := b.WalkOrphanSegments("bucket", nil, func(p string) error {
		found = append(found, p)
		return nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"bucket/folder/sub/file_segments/blob-old"}, found)
}

func TestWalkOrphanSegments_BucketENOENT(t *testing.T) {
	tmpRoot := t.TempDir()
	b := &DistributedBackend{root: tmpRoot, scrubOrphanAge: 5 * time.Minute}
	err := b.WalkOrphanSegments("nonexistent", nil, func(string) error { return nil })
	require.NoError(t, err)
}

func TestDeleteOrphanSegment_Production(t *testing.T) {
	tmpRoot := t.TempDir()
	segPath := filepath.Join(tmpRoot, "data", "bucket", "key_segments", "blob1")
	require.NoError(t, os.MkdirAll(filepath.Dir(segPath), 0o755))
	require.NoError(t, os.WriteFile(segPath, []byte("seg"), 0o644))

	b := &DistributedBackend{root: tmpRoot, scrubOrphanAge: 5 * time.Minute}

	require.NoError(t, b.DeleteOrphanSegment("bucket/key_segments/blob1"))
	_, statErr := os.Stat(segPath)
	require.True(t, os.IsNotExist(statErr))

	// Second delete: ENOENT swallow.
	require.NoError(t, b.DeleteOrphanSegment("bucket/key_segments/blob1"))
}

func TestWalkOrphanSegments_ErrorPaths(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("permission test requires non-root")
	}
	tmpRoot := t.TempDir()
	segDir := filepath.Join(tmpRoot, "data", "bucket", "blocked_segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(segDir, "blob1"), []byte("s"), 0o644))

	require.NoError(t, os.Chmod(segDir, 0o000))
	defer os.Chmod(segDir, 0o755)

	b := &DistributedBackend{root: tmpRoot, scrubOrphanAge: 5 * time.Minute}
	var found []string
	err := b.WalkOrphanSegments("bucket", nil, func(p string) error {
		found = append(found, p)
		return nil
	})
	require.NoError(t, err) // walker swallows readdir error per segDir
	require.Empty(t, found)
}

// --- multi-group (per-group dispatch) -----------------------------------

// siblingSegmentBackend builds a second data-group backend with its OWN root
// (segments are per-group on disk under b.root/data/...), sharing b's store and
// raft node. The analogue of a node hosting a second data group whose segments
// live in a separate subtree.
func siblingSegmentBackend(t *testing.T, b *DistributedBackend, groupID string) *DistributedBackend {
	t.Helper()
	sib, err := NewDistributedBackendForGroup(t.TempDir(), b.store, b.node, groupID)
	require.NoError(t, err)
	sib.SetScrubOrphanAge(time.Second)
	t.Cleanup(func() {
		if sib.coalesceCancel != nil {
			sib.coalesceCancel()
		}
	})
	return sib
}

// writeSegmentFile creates <root>/data/<bucket>/<key>_segments/<blobID>, back-dated.
func writeSegmentFile(t *testing.T, root, bucket, key, blobID string, age time.Duration) string {
	t.Helper()
	dir := filepath.Join(root, "data", bucket, key+segmentsDirSuffix)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	p := filepath.Join(dir, blobID)
	require.NoError(t, os.WriteFile(p, []byte("seg"), 0o644))
	backDate(t, p, age)
	return p
}

func TestOwningGroupBackend_DefaultsToSelf(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.Same(t, b, b.owningGroupBackend("any-bucket"),
		"un-wired owningGroupBackendFn must resolve every bucket to self (single-group)")
}

// TestWalkOrphanSegments_DispatchesToOwningGroup proves the per-bucket walk runs
// on the OWNING group's b.root subtree, not this backend's: the orphan lives only
// under the sibling's root.
func TestWalkOrphanSegments_DispatchesToOwningGroup(t *testing.T) {
	b := newTestDistributedBackend(t)
	b.SetScrubOrphanAge(time.Second)
	waitCaughtUp(t, b)
	sib := siblingSegmentBackend(t, b, "group-3")
	b.SetOwningGroupBackendSource(func(bucket string) *DistributedBackend {
		if bucket == "bkt" {
			return sib
		}
		return b
	})

	writeSegmentFile(t, sib.root, "bkt", "key", "blob1", 10*time.Minute)

	var got []string
	require.NoError(t, b.WalkOrphanSegments("bkt", nil, func(key string) error {
		got = append(got, key)
		return nil
	}))
	require.Equal(t, []string{"bkt/key_segments/blob1"}, got,
		"must walk the sibling owner's subtree (dispatch), not this backend's root")
}

// TestWalkOrphanSegments_SkipsLaggingOwner proves a not-caught-up owning group GCs
// nothing (a stale FSM could otherwise false-orphan a committed segment).
func TestWalkOrphanSegments_SkipsLaggingOwner(t *testing.T) {
	b := newTestDistributedBackend(t)
	b.SetScrubOrphanAge(time.Second)
	waitCaughtUp(t, b)
	lag := laggingGroupBackend(t, b, "group-lag") // CaughtUp() == false
	b.SetOwningGroupBackendSource(func(string) *DistributedBackend { return lag })

	writeSegmentFile(t, lag.root, "bkt", "key", "blob1", 10*time.Minute)

	var got []string
	require.NoError(t, b.WalkOrphanSegments("bkt", nil, func(key string) error {
		got = append(got, key)
		return nil
	}))
	require.Empty(t, got, "a not-caught-up owning group must yield no segment candidates")
}

// TestDeleteOrphanSegment_RoutesByBucket proves the delete routes to the owning
// group's root (bucket = the key's first component).
func TestDeleteOrphanSegment_RoutesByBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	sib := siblingSegmentBackend(t, b, "group-3")
	b.SetOwningGroupBackendSource(func(bucket string) *DistributedBackend {
		if bucket == "bkt" {
			return sib
		}
		return b
	})

	p := writeSegmentFile(t, sib.root, "bkt", "key", "blob1", time.Minute)
	require.FileExists(t, p)

	require.NoError(t, b.DeleteOrphanSegment("bkt/key_segments/blob1"))
	require.NoFileExists(t, p, "delete must route to the owning group's root and remove the file")
}

// twoBucketBackends builds two independent backends, each with one bucket + one
// live object record, for the union tests.
func twoBucketBackends(t *testing.T) (b1, b2 *DistributedBackend) {
	t.Helper()
	ctx := context.Background()
	b1 = newTestDistributedBackend(t)
	b2 = newTestDistributedBackend(t)
	require.NoError(t, b1.CreateBucket(ctx, "b1bkt"))
	require.NoError(t, b2.CreateBucket(ctx, "b2bkt"))
	putObjMeta(t, b1, "b1bkt", "k", "v1", "e1")
	putObjMeta(t, b2, "b2bkt", "k", "v2", "e2")
	return b1, b2
}

func TestSegmentSweepBuckets_UnionsHostedGroupsAndDefaultsToSelf(t *testing.T) {
	b1, b2 := twoBucketBackends(t)

	got, err := b1.SegmentSweepBuckets(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"b1bkt"}, got, "default (un-wired) is self buckets only")

	b1.SetHostedGroupBackendsSource(func() []*DistributedBackend { return []*DistributedBackend{b1, b2} })
	got, err = b1.SegmentSweepBuckets(context.Background())
	require.NoError(t, err)
	sort.Strings(got)
	require.Equal(t, []string{"b1bkt", "b2bkt"}, got, "wired union covers every hosted group's buckets")
}

func TestListAllObjectsStrict_UnionsHostedGroups(t *testing.T) {
	b1, b2 := twoBucketBackends(t)
	b1.SetHostedGroupBackendsSource(func() []*DistributedBackend { return []*DistributedBackend{b1, b2} })

	objs, err := b1.ListAllObjectsStrict()
	require.NoError(t, err)
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Bucket+"/"+o.Key)
	}
	sort.Strings(keys)
	require.Equal(t, []string{"b1bkt/k", "b2bkt/k"}, keys,
		"the GC known-set must union every hosted group's live objects")

	objsSelf, err := b2.ListAllObjectsStrict()
	require.NoError(t, err)
	require.Len(t, objsSelf, 1, "default (un-wired) is self only")
	require.Equal(t, "b2bkt", objsSelf[0].Bucket)
}
