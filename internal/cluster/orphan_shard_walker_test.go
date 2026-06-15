package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// waitCaughtUp blocks until the leader's ReadIndex lease is established so
// CaughtUp() returns true deterministically (it is false for a brief window
// right after bootstrap, before the leader commits its term's no-op).
func waitCaughtUp(t *testing.T, b *DistributedBackend) {
	t.Helper()
	require.Eventually(t, func() bool {
		return b.CaughtUp(context.Background())
	}, 5*time.Second, 10*time.Millisecond, "node never reported caught-up")
}

// --- harness ------------------------------------------------------------

// orphanWalkerBackend returns a single-dataDir backend wired so the orphan-shard
// sweep is ALLOWED (gate on, empty frozen source). The age gate is floored to
// minOrphanShardAge (60s), so "old" shards must be back-dated > that.
func orphanWalkerBackend(t *testing.T) *DistributedBackend {
	t.Helper()
	b := newTestDistributedBackend(t)
	b.SetScrubOrphanAge(time.Second) // floored to minOrphanShardAge (60s) by the walker
	b.SetOrphanShardSweepGate(func() bool { return true })
	b.SetFrozenObjectVersionSource(func() ([]storage.SnapshotObjectRef, error) { return nil, nil })
	waitCaughtUp(t, b)
	return b
}

const oldEnough = minOrphanShardAge + time.Minute // safely past the floored age gate

// writeShardLeaf creates <root>/<rel>/shard_<i> for each i and back-dates them.
func writeShardLeaf(t *testing.T, root, rel string, indices []int, backdate time.Duration) string {
	t.Helper()
	dir := filepath.Join(root, filepath.FromSlash(rel))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	past := time.Now().Add(-backdate)
	for _, i := range indices {
		p := filepath.Join(dir, "shard_"+strconv.Itoa(i))
		require.NoError(t, os.WriteFile(p, []byte("shard"), 0o644))
		require.NoError(t, os.Chtimes(p, past, past))
	}
	return filepath.Clean(dir)
}

// putObjMeta writes a live FSM obj: record for (bucket,key,versionID).
func putObjMeta(t *testing.T, b *DistributedBackend, bucket, key, versionID, etag string) {
	t.Helper()
	raw, err := marshalObjectMeta(objectMeta{Key: key, ETag: etag, ECData: 1})
	require.NoError(t, err)
	require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
		return txn.Set(b.ks().ObjectMetaKeyV(bucket, key, versionID), raw)
	}))
}

func collectOrphans(t *testing.T, b *DistributedBackend, known map[string]bool) []string {
	t.Helper()
	var got []string
	require.NoError(t, b.WalkOrphanShards(known, func(dir string) error {
		got = append(got, dir)
		return nil
	}))
	return got
}

// --- tests --------------------------------------------------------------

func TestWalkOrphanShards_TrulyOrphanYielded(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	dir := writeShardLeaf(t, root, "bkt/key/ver-orphan", []int{0}, oldEnough)
	got := collectOrphans(t, b, nil)
	require.Equal(t, []string{dir}, got)
}

func TestWalkOrphanShards_LiveVersionedRecordProtected(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	// v1 and v2 both on disk; both have live obj: records; known only has v2.
	v1 := writeShardLeaf(t, root, "bkt/key/v1", []int{0}, oldEnough)
	v2 := writeShardLeaf(t, root, "bkt/key/v2", []int{0}, oldEnough)
	putObjMeta(t, b, "bkt", "key", "v1", "e1")
	putObjMeta(t, b, "bkt", "key", "v2", "e2")
	got := collectOrphans(t, b, map[string]bool{v2: true})
	require.Empty(t, got, "v1 has a live obj: record; must not be swept (data-loss guard)")
	_ = v1
}

func TestWalkOrphanShards_AgeGateSkipsRecent(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	writeShardLeaf(t, root, "bkt/key/ver-fresh", []int{0}, 0) // just written
	require.Empty(t, collectOrphans(t, b, nil))
}

func TestWalkOrphanShards_InFlightTmpSkipped(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	dir := writeShardLeaf(t, root, "bkt/key/ver-wip", []int{0}, oldEnough)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "shard_0.123.456.tmp"), []byte("wip"), 0o644))
	require.Empty(t, collectOrphans(t, b, nil), "a dir with an in-flight .tmp must not be swept")
}

func TestWalkOrphanShards_SegmentAndCoalescedSkipped(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	writeShardLeaf(t, root, "bkt/key/segments/blob1", []int{0}, oldEnough)
	writeShardLeaf(t, root, "bkt/key/coalesced/c1", []int{0}, oldEnough)
	require.Empty(t, collectOrphans(t, b, nil), "segment/coalesced shard dirs are a different class; never delete")
}

func TestWalkOrphanShards_FrozenPinProtects(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	dir := writeShardLeaf(t, root, "bkt/key/ver-snap", []int{0}, oldEnough)
	// Pin via the snapshot frozen-object source: no live obj:/quorum-meta record.
	b.SetFrozenObjectVersionSource(func() ([]storage.SnapshotObjectRef, error) {
		return []storage.SnapshotObjectRef{{Bucket: "bkt", Key: "key", VersionID: "ver-snap"}}, nil
	})
	require.Empty(t, collectOrphans(t, b, nil), "snapshot-pinned version must not be swept")
	// Mutation: empty frozen source => the same dir becomes an orphan.
	b.SetFrozenObjectVersionSource(func() ([]storage.SnapshotObjectRef, error) { return nil, nil })
	require.Equal(t, []string{dir}, collectOrphans(t, b, nil))
}

func TestWalkOrphanShards_FrozenSourceErrorFailsClosed(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	writeShardLeaf(t, root, "bkt/key/ver-orphan", []int{0}, oldEnough)
	b.SetFrozenObjectVersionSource(func() ([]storage.SnapshotObjectRef, error) {
		return nil, os.ErrPermission
	})
	require.Empty(t, collectOrphans(t, b, nil), "frozen-source error must fail closed (no sweep)")
}

func TestWalkOrphanShards_GateOffNoOp(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	dir := writeShardLeaf(t, root, "bkt/key/ver-orphan", []int{0}, oldEnough)
	b.SetOrphanShardSweepGate(func() bool { return false })
	require.Empty(t, collectOrphans(t, b, nil), "gate off => no sweep (multi-group safety)")
	b.SetOrphanShardSweepGate(func() bool { return true })
	require.Equal(t, []string{dir}, collectOrphans(t, b, nil))
}

func TestWalkOrphanShards_UnversionedDirKept(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	// <bucket>/<key> with shard files but no versionID leaf (only 2 components).
	writeShardLeaf(t, root, "bkt/key", []int{0}, oldEnough)
	require.Empty(t, collectOrphans(t, b, nil), "unparseable (unversioned) dir is kept, never deleted")
}

func TestWalkOrphanShards_StripedAcrossDataDirs(t *testing.T) {
	b, _ := newTestDistributedBackendWithDB(t)
	keeper, clusterID := testDEKKeeper(t)
	d0, d1, d2 := t.TempDir(), t.TempDir(), t.TempDir()
	svc := NewMultiRootShardService([]string{d0, d1, d2}, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	b.SetShardService(svc, []string{b.selfAddr})
	b.SetScrubOrphanAge(time.Second)
	b.SetOrphanShardSweepGate(func() bool { return true })
	b.SetFrozenObjectVersionSource(func() ([]storage.SnapshotObjectRef, error) { return nil, nil })
	waitCaughtUp(t, b)

	dirs := b.shardSvc.DataDirs() // each = <dN>/shards
	// Orphan whose shards live only under dataDirs[1] and dataDirs[2] (none under [0]).
	writeShardLeaf(t, dirs[1], "bkt/key/ver-orphan", []int{1}, oldEnough)
	writeShardLeaf(t, dirs[2], "bkt/key/ver-orphan", []int{2}, oldEnough)
	canonical := filepath.Clean(filepath.Join(dirs[0], "bkt/key/ver-orphan"))

	got := collectOrphans(t, b, nil)
	require.Equal(t, []string{canonical}, got, "must yield once as the dataDirs[0]-rooted canonical")
}

func TestDeleteOrphanDir_RemovesAcrossDataDirsIdempotent(t *testing.T) {
	b, _ := newTestDistributedBackendWithDB(t)
	keeper, clusterID := testDEKKeeper(t)
	d0, d1 := t.TempDir(), t.TempDir()
	svc := NewMultiRootShardService([]string{d0, d1}, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	b.SetShardService(svc, []string{b.selfAddr})
	b.SetScrubOrphanAge(time.Second)
	b.SetOrphanShardSweepGate(func() bool { return true })
	b.SetFrozenObjectVersionSource(func() ([]storage.SnapshotObjectRef, error) { return nil, nil })
	waitCaughtUp(t, b)

	dirs := b.shardSvc.DataDirs()
	writeShardLeaf(t, dirs[0], "bkt/key/ver", []int{0, 2}, oldEnough)
	writeShardLeaf(t, dirs[1], "bkt/key/ver", []int{1}, oldEnough)
	canonical := filepath.Clean(filepath.Join(dirs[0], "bkt/key/ver"))

	n, err := b.DeleteOrphanDir(canonical)
	require.NoError(t, err)
	require.Equal(t, 3, n, "3 shard files removed across 2 dataDirs")
	require.NoDirExists(t, filepath.Join(dirs[0], "bkt/key/ver"))
	require.NoDirExists(t, filepath.Join(dirs[1], "bkt/key/ver"))

	n2, err := b.DeleteOrphanDir(canonical) // idempotent
	require.NoError(t, err)
	require.Equal(t, 0, n2)
}

func TestDeleteOrphanDir_ContainmentGuard(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	// An escaping canonical must never delete outside the shard root. Whether
	// it is stopped by the pre-delete revalidation or the containment guard, the
	// invariant is: nothing is deleted.
	escaping := filepath.Join(root, "..", "..", "etc")
	n, err := b.DeleteOrphanDir(escaping)
	require.Equal(t, 0, n, "escaping path must delete nothing; err=%v", err)
}

func TestDeleteOrphanDir_RevalidatesBeforeDelete(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	dir := writeShardLeaf(t, root, "bkt/key/ver", []int{0}, oldEnough)

	// Gate flipped off between walk and delete => no deletion (TOCTOU close).
	b.SetOrphanShardSweepGate(func() bool { return false })
	n, err := b.DeleteOrphanDir(dir)
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.DirExists(t, dir)

	// Became live (obj: appeared) between walk and delete => no deletion.
	b.SetOrphanShardSweepGate(func() bool { return true })
	putObjMeta(t, b, "bkt", "key", "ver", "e1")
	n, err = b.DeleteOrphanDir(dir)
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.DirExists(t, dir)
}

// TestWalkOrphanShards_CleanableKeyRetainedVersionProtected guards the P0 from
// the code-gate: shard writes filepath.Join-clean the key, so a logical key like
// "a/../b" lands in physical dir ".../bkt/b/<ver>". Reverse-parsing that path
// gives the wrong logical key and would miss the real obj: record, deleting a
// live retained version. The forward live-set (logical key -> getShardDir) must
// protect it.
func TestWalkOrphanShards_CleanableKeyRetainedVersionProtected(t *testing.T) {
	for _, logicalKey := range []string{"a/../b", "dir/", "a//b", "a/./b"} {
		t.Run(logicalKey, func(t *testing.T) {
			b := orphanWalkerBackend(t)
			const versionID = "v1"
			physDir, err := b.shardSvc.getShardDir("bkt", logicalKey+"/"+versionID, 0)
			require.NoError(t, err)
			require.NoError(t, os.MkdirAll(physDir, 0o755))
			sp := filepath.Join(physDir, "shard_0")
			require.NoError(t, os.WriteFile(sp, []byte("shard"), 0o644))
			past := time.Now().Add(-oldEnough)
			require.NoError(t, os.Chtimes(sp, past, past))
			canonical := filepath.Clean(physDir)

			// Live obj: record under the LOGICAL key; NOT in the latest-only known-set.
			putObjMeta(t, b, "bkt", logicalKey, versionID, "e1")

			require.Empty(t, collectOrphans(t, b, nil),
				"retained version with cleanable key %q must not be swept", logicalKey)
			n, derr := b.DeleteOrphanDir(canonical)
			require.NoError(t, derr)
			require.Equal(t, 0, n, "DeleteOrphanDir must refuse a live cleanable-key version")
			require.DirExists(t, physDir)

			// Mutation: drop the obj: record => the same dir is now a true orphan.
			require.NoError(t, b.store.Update(func(txn MetadataTxn) error {
				return txn.Delete(b.ks().ObjectMetaKeyV("bkt", logicalKey, versionID))
			}))
			require.Equal(t, []string{canonical}, collectOrphans(t, b, nil),
				"without the obj: record the cleanable-key dir is a true orphan")
		})
	}
}

// TestDeleteOrphanDir_CrossRootInflightGuard proves a striped in-flight write
// under a non-walked dataDir blocks deletion of the whole canonical dir.
func TestDeleteOrphanDir_CrossRootInflightGuard(t *testing.T) {
	b, _ := newTestDistributedBackendWithDB(t)
	keeper, clusterID := testDEKKeeper(t)
	d0, d1 := t.TempDir(), t.TempDir()
	svc := NewMultiRootShardService([]string{d0, d1}, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	b.SetShardService(svc, []string{b.selfAddr})
	b.SetScrubOrphanAge(time.Second)
	b.SetOrphanShardSweepGate(func() bool { return true })
	b.SetFrozenObjectVersionSource(func() ([]storage.SnapshotObjectRef, error) { return nil, nil })
	waitCaughtUp(t, b)

	dirs := b.shardSvc.DataDirs()
	// dataDirs[0] has an OLD shard (would be yielded); dataDirs[1] is mid-write (.tmp).
	writeShardLeaf(t, dirs[0], "bkt/key/ver", []int{0}, oldEnough)
	inflight := filepath.Join(dirs[1], "bkt/key/ver")
	require.NoError(t, os.MkdirAll(inflight, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(inflight, "shard_1.9.9.tmp"), []byte("wip"), 0o644))
	canonical := filepath.Clean(filepath.Join(dirs[0], "bkt/key/ver"))

	n, err := b.DeleteOrphanDir(canonical)
	require.NoError(t, err)
	require.Equal(t, 0, n, "an in-flight .tmp under any dataDir must block deletion")
	require.DirExists(t, filepath.Join(dirs[0], "bkt/key/ver"))
	require.DirExists(t, inflight)
}
