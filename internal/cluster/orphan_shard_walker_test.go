package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
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
// minOrphanShardAge (the EC write+commit window, ~466s), so "old" shards must be
// back-dated > that — use oldEnough.
func orphanWalkerBackend(t *testing.T) *DistributedBackend {
	t.Helper()
	b := newTestDistributedBackend(t)
	b.SetScrubOrphanAge(time.Second) // floored to minOrphanShardAge by the walker
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

// putObjMeta writes a live per-version quorum-meta blob for (bucket,key,versionID)
// on a versioning-enabled bucket — the shard-liveness authority hasLiveShardRecord
// reads. The bucket is created + versioning-enabled if not already, so the
// blob-authoritative liveness path resolves it.
func putObjMeta(t *testing.T, b *DistributedBackend, bucket, key, versionID, etag string) {
	t.Helper()
	ctx := context.Background()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		require.NoError(t, b.CreateBucket(ctx, bucket))
		setVersioningForTest(t, b, bucket, "Enabled")
	}
	seedVersionBlob(t, b, bucket, key, versionID, PutObjectMetaCmd{
		ETag: etag, ECData: 1, NodeIDs: []string{b.currentSelfAddr()},
	})
}

// deleteVersionBlob removes the per-version quorum-meta blob seeded by putObjMeta
// / seedVersionBlob, turning a previously-live version into a true orphan.
func deleteVersionBlob(t *testing.T, b *DistributedBackend, bucket, key, versionID string) {
	t.Helper()
	require.NoError(t, b.shardSvc.deleteQuorumMetaVersionLocal(bucket, key, versionID))
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

// TestWalkOrphanShards_InFlightSlowWriteProtected proves a shard from a SLOW
// in-flight EC write is NOT reclaimed. An EC write renames each shard_N BEFORE the
// metadata commit, and a remote shard write retries ecShardWriteAttempts times,
// each bounded by shardRPCTimeout — so an early-written shard can be MINUTES old
// while the write is still in flight (a sibling remote shard still retrying), far
// older than the legacy 60s floor. The age floor MUST exceed that bounded
// write+commit window, else the scrubber deletes live, about-to-commit data
// (the pre-2026-06 in-flight data-loss bug). RED on the legacy 2*proposeForwardTimeout
// floor; GREEN once the floor covers the EC-write+commit window.
func TestWalkOrphanShards_InFlightSlowWriteProtected(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	// A shard whose write took two RPC timeouts: squarely inside the bounded
	// EC-write window, and uncommitted (no obj:/quorum-meta record).
	inFlightAge := 2 * shardRPCTimeout
	require.Less(t, inFlightAge, minOrphanShardAge,
		"orphan age floor must exceed the bounded EC-write+commit window so an in-flight slow write is never reclaimed")
	writeShardLeaf(t, root, "bkt/key/v-inflight", []int{0}, inFlightAge)
	require.Empty(t, collectOrphans(t, b, nil),
		"a shard younger than the EC-write+commit window must be kept (may be an in-flight slow write)")
}

func TestWalkOrphanShards_InFlightTmpSkipped(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	dir := writeShardLeaf(t, root, "bkt/key/ver-wip", []int{0}, oldEnough)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "shard_0.123.456.tmp"), []byte("wip"), 0o644))
	require.Empty(t, collectOrphans(t, b, nil), "a dir with an in-flight .tmp must not be swept")
}

func TestWalkOrphanShards_SegmentSkipped(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	writeShardLeaf(t, root, "bkt/key/segments/blob1", []int{0}, oldEnough)
	require.Empty(t, collectOrphans(t, b, nil), "segment shard dirs are a separate class; never delete")
}

func TestParseCoalescedRel(t *testing.T) {
	cases := []struct {
		name, rel, bucket, key, cid string
		ok                          bool
	}{
		{"simple", "bkt/key/coalesced/c1", "bkt", "key", "c1", true},
		{"nested key", "bkt/a/b/c/coalesced/c1", "bkt", "a/b/c", "c1", true},
		{"empty key", "bkt/coalesced/c1", "", "", "", false},
		{"not coalesced", "bkt/key/v1", "", "", "", false},
		{"marker not penultimate", "bkt/key/coalesced/sub/c1", "", "", "", false},
		{"empty cid", "bkt/key/coalesced/", "", "", "", false},
		{"lookalike object key parses", "bkt/foo/coalesced/v1", "bkt", "foo", "v1", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bk, k, c, ok := parseCoalescedRel(tc.rel)
			require.Equal(t, tc.ok, ok)
			if tc.ok {
				require.Equal(t, tc.bucket, bk)
				require.Equal(t, tc.key, k)
				require.Equal(t, tc.cid, c)
			}
		})
	}
}

// coalCmd builds a versioning-disabled appendable object manifest carrying the
// given coalesced refs, written via the live quorum-meta blob path.
func coalCmd(b *DistributedBackend, bkt, key string, refs []CoalescedShardRef) PutObjectMetaCmd {
	return PutObjectMetaCmd{
		Bucket: bkt, Key: key, ETag: "e", IsAppendable: true,
		NodeIDs: []string{b.currentSelfAddr()}, ECData: 1, Coalesced: refs,
	}
}

func TestHasLiveCoalescedRef(t *testing.T) {
	ctx := context.Background()
	t.Run("referenced by ShardKey is live", func(t *testing.T) {
		b := orphanWalkerBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "cb"))
		require.NoError(t, b.writeQuorumMeta(ctx, coalCmd(b, "cb", "obj", []CoalescedShardRef{{CoalescedID: "live", ShardKey: "obj/coalesced/live"}})))
		live, certain := b.hasLiveCoalescedRef("cb", "obj", "live")
		require.True(t, live)
		require.True(t, certain)
	})
	t.Run("unreferenced is orphan-eligible", func(t *testing.T) {
		b := orphanWalkerBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "cb"))
		require.NoError(t, b.writeQuorumMeta(ctx, coalCmd(b, "cb", "obj", []CoalescedShardRef{{CoalescedID: "other", ShardKey: "obj/coalesced/other"}})))
		live, certain := b.hasLiveCoalescedRef("cb", "obj", "dead")
		require.False(t, live)
		require.True(t, certain)
	})
	t.Run("absent object is orphan-eligible", func(t *testing.T) {
		b := orphanWalkerBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "cb"))
		live, certain := b.hasLiveCoalescedRef("cb", "gone", "c1")
		require.False(t, live)
		require.True(t, certain)
	})
	t.Run("versioning-enabled keeps (fail-closed)", func(t *testing.T) {
		b := orphanWalkerBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "cben"))
		setVersioningForTest(t, b, "cben", "Enabled")
		live, certain := b.hasLiveCoalescedRef("cben", "obj", "c1")
		require.False(t, live)
		require.False(t, certain)
	})
}

func TestWalkOrphanShards_CoalescedOrphanYielded(t *testing.T) {
	ctx := context.Background()
	b := orphanWalkerBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bkt"))
	root := b.shardSvc.DataDirs()[0]
	require.NoError(t, b.writeQuorumMeta(ctx, coalCmd(b, "bkt", "key", []CoalescedShardRef{{CoalescedID: "live", ShardKey: "key/coalesced/live"}})))
	live := writeShardLeaf(t, root, "bkt/key/coalesced/live", []int{0}, oldEnough)
	dead := writeShardLeaf(t, root, "bkt/key/coalesced/dead", []int{0}, oldEnough)
	got := collectOrphans(t, b, nil)
	require.Equal(t, []string{dead}, got, "unreferenced coalesced shard is reclaimed")
	require.NotContains(t, got, live, "referenced coalesced shard must be kept")
}

func TestWalkOrphanShards_CoalescedKeptVersioningEnabled(t *testing.T) {
	ctx := context.Background()
	b := orphanWalkerBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "ben"))
	setVersioningForTest(t, b, "ben", "Enabled")
	root := b.shardSvc.DataDirs()[0]
	writeShardLeaf(t, root, "ben/key/coalesced/c1", []int{0}, oldEnough)
	require.Empty(t, collectOrphans(t, b, nil), "Enabled bucket: coalesced kept (fail-closed)")
}

func TestWalkOrphanShards_RegularObjectKeyedCoalescedProtected(t *testing.T) {
	// T4: key whose PENULTIMATE segment is "coalesced" (foo/coalesced, vid v1) — same
	// shape as a coalesced shard. Live full-object record must protect it (dual-interp).
	ctx := context.Background()
	b := orphanWalkerBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bkt"))
	root := b.shardSvc.DataDirs()[0]
	require.NoError(t, b.writeQuorumMeta(ctx, PutObjectMetaCmd{Bucket: "bkt", Key: "foo/coalesced", ETag: "e", VersionID: "v1", NodeIDs: []string{b.currentSelfAddr()}, ECData: 1}))
	writeShardLeaf(t, root, "bkt/foo/coalesced/v1", []int{0}, oldEnough)
	require.Empty(t, collectOrphans(t, b, nil), "a real object keyed .../coalesced must not be mis-reclaimed")
}

func TestWalkOrphanShards_CoalescedComponentInKeyNotMisrouted(t *testing.T) {
	// Regression lock: a full-object key with a "coalesced" component that is NOT the
	// penultimate segment (key "coalesced/foo.txt" → path bkt/coalesced/foo.txt/v1) must
	// route to the FULL-OBJECT branch (parseCoalescedRel fails), so a genuine orphan IS
	// reclaimed. FAILS on strings.Contains routing (intercepted → kept → leaked).
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	dir := writeShardLeaf(t, root, "bkt/coalesced/foo.txt/v1", []int{0}, oldEnough) // no metadata → genuine orphan
	require.Equal(t, []string{dir}, collectOrphans(t, b, nil),
		"a full-object key with a non-penultimate 'coalesced' component must fall through to full-object reclaim")
}

func TestWalkOrphanShards_CoalescedReclaimedWhenKeyPathIsDir(t *testing.T) {
	// EISDIR symmetry: an object "k/sub" exists → .quorum_meta/bkt/k is a DIRECTORY, so
	// reading the manifest for key "k" hits EISDIR. That must read as not-found (no blob
	// at "k"), so a genuine orphan coalesced shard for "k" IS reclaimed, not leaked.
	// Without the EISDIR→not-found mapping the coalesced interp reads uncertain → kept.
	ctx := context.Background()
	b := orphanWalkerBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bkt"))
	root := b.shardSvc.DataDirs()[0]
	require.NoError(t, b.writeQuorumMeta(ctx, PutObjectMetaCmd{Bucket: "bkt", Key: "k/sub", ETag: "e", VersionID: "v1", NodeIDs: []string{b.currentSelfAddr()}, ECData: 1}))
	dir := writeShardLeaf(t, root, "bkt/k/coalesced/orphan", []int{0}, oldEnough) // object "k" never existed → genuine orphan
	require.Equal(t, []string{dir}, collectOrphans(t, b, nil),
		"a coalesced orphan for a key whose manifest path is a directory (EISDIR) must be reclaimed, not leaked")
}

func TestDeleteOrphanDir_CoalescedRevalidation(t *testing.T) {
	ctx := context.Background()
	t.Run("live coalesced NOT deleted", func(t *testing.T) {
		b := orphanWalkerBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bkt"))
		root := b.shardSvc.DataDirs()[0]
		require.NoError(t, b.writeQuorumMeta(ctx, coalCmd(b, "bkt", "key", []CoalescedShardRef{{CoalescedID: "live", ShardKey: "key/coalesced/live"}})))
		dir := writeShardLeaf(t, root, "bkt/key/coalesced/live", []int{0}, oldEnough)
		n, err := b.DeleteOrphanDir(dir)
		require.NoError(t, err)
		require.Zero(t, n)
		_, statErr := os.Stat(filepath.Join(dir, "shard_0"))
		require.NoError(t, statErr)
	})
	t.Run("orphan coalesced IS deleted", func(t *testing.T) {
		b := orphanWalkerBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "bkt"))
		root := b.shardSvc.DataDirs()[0]
		require.NoError(t, b.writeQuorumMeta(ctx, coalCmd(b, "bkt", "key", []CoalescedShardRef{{CoalescedID: "other", ShardKey: "key/coalesced/other"}})))
		dir := writeShardLeaf(t, root, "bkt/key/coalesced/dead", []int{0}, oldEnough)
		n, err := b.DeleteOrphanDir(dir)
		require.NoError(t, err)
		require.Equal(t, 1, n)
	})
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

// TestWalkOrphanShards_CleanableKeyRetainedVersionProtected guards a P0 data-loss
// hole: shard writes filepath.Join-clean the key, so a logical key like "a/../b"
// lands in physical dir ".../bkt/b/<ver>". The per-version quorum-meta blob is
// written under the SAME cleaned path, so hasLiveShardRecord's reverse-parsed
// lookup (key="b") finds the live blob and protects the shard. This verifies the
// removal of the legacy FSM forward-map is safe: blob-backed cleanable-key
// versions stay protected.
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

			// Mutation: drop the per-version blob => the same dir is now a true orphan.
			deleteVersionBlob(t, b, "bkt", logicalKey, versionID)
			require.Equal(t, []string{canonical}, collectOrphans(t, b, nil),
				"without the per-version blob the cleanable-key dir is a true orphan")
		})
	}
}

// --- multi-group harness -----------------------------------------------

// siblingGroupBackend builds a second data-group backend that shares b's store,
// raft node, and ShardService but scans its own ks (groupID) prefix — the unit
// analogue of a node hosting more than one data group.
func siblingGroupBackend(t *testing.T, b *DistributedBackend, groupID string) *DistributedBackend {
	t.Helper()
	sib, err := NewDistributedBackendForGroup(b.root, b.store, b.node, groupID)
	require.NoError(t, err)
	sib.shardSvc = b.shardSvc // share the node-level ShardService (same dataDirs)
	t.Cleanup(func() {
		if sib.coalesceCancel != nil {
			sib.coalesceCancel()
		}
	})
	return sib
}

// laggingGroupBackend builds a hosted-group backend whose raft node can NEVER win
// an election (a 3-voter config whose 2 peers are unreachable, so no quorum), so
// ReadIndex — hence CaughtUp() — fails deterministically and permanently. (A
// single unbootstrapped node self-elects within milliseconds, which is racy.)
func laggingGroupBackend(t *testing.T, b *DistributedBackend, groupID string) *DistributedBackend {
	t.Helper()
	cfg := raft.DefaultConfig("lagging-node", []string{"127.0.0.1:1", "127.0.0.1:2"})
	node, closeRaft, err := newRaftNode(cfg, t.TempDir())
	require.NoError(t, err)
	node.SetTransport(
		func(string, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers")
		},
		func(string, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers")
		},
	)
	node.Start()
	require.NoError(t, node.Bootstrap())
	sib, err := NewDistributedBackendForGroup(b.root, b.store, node, groupID)
	require.NoError(t, err)
	sib.shardSvc = b.shardSvc
	require.False(t, sib.CaughtUp(context.Background()), "no-quorum node must never report caught-up")
	t.Cleanup(func() {
		if sib.coalesceCancel != nil {
			sib.coalesceCancel()
		}
		node.Close()
		if closeRaft != nil {
			_ = closeRaft()
		}
	})
	return sib
}

// TestWalkOrphanShards_MultiGroup_SiblingLiveVersionProtected proves the shard
// sweep, when run in multi-group mode (this node hosts MORE than one data group),
// still protects a live versioned object's shard. Two facts make this genuinely
// multi-group, not a single-group test in disguise:
//
//  1. The sweep is gated on EVERY hosted group being caught up
//     (orphanShardSweepAllowed unions hostedGroupBackends); the sibling group must
//     be present-and-caught-up or the sweep would not run at all. We assert it DOES
//     run by observing a true orphan get reclaimed below.
//  2. Object metadata lives in the node-level off-raft quorum-meta blob store (NOT
//     a per-group ks prefix), so hasLiveShardRecord — called once on the walking
//     backend by (bucket,key,vid) — protects the shard regardless of which hosted
//     group owns the bucket. This is exactly why the legacy per-group FSM
//     forward-map (liveVersionedShardDirs) could be removed: a blob is shared.
func TestWalkOrphanShards_MultiGroup_SiblingLiveVersionProtected(t *testing.T) {
	b := orphanWalkerBackend(t)
	sib := siblingGroupBackend(t, b, "group-3")
	waitCaughtUp(t, sib) // the sweep runs only if every hosted group is caught up
	b.SetHostedGroupBackendsSource(func() []*DistributedBackend { return []*DistributedBackend{b, sib} })
	b.SetOwningGroupHostedChecker(func(string) bool { return true })

	root := b.shardSvc.DataDirs()[0]
	dir := writeShardLeaf(t, root, "bkt/key/v-sib", []int{0}, oldEnough)
	// The per-version blob is node-shared across hosted groups (co-located with shards).
	putObjMeta(t, b, "bkt", "key", "v-sib", "e1")

	require.Empty(t, collectOrphans(t, b, nil),
		"a live versioned object's shard must be protected by its node-shared per-version blob")

	deleteVersionBlob(t, b, "bkt", "key", "v-sib")
	require.Equal(t, []string{dir}, collectOrphans(t, b, nil),
		"without the per-version blob the dir is a true orphan")
}

// TestWalkOrphanShards_MultiGroup_FloatedInShardKept proves a shard whose owning
// group is not locally hosted (balancer-floated) is kept, while a locally-owned
// orphan is still reclaimed.
func TestWalkOrphanShards_MultiGroup_FloatedInShardKept(t *testing.T) {
	b := orphanWalkerBackend(t)
	b.SetHostedGroupBackendsSource(func() []*DistributedBackend { return []*DistributedBackend{b} })
	b.SetOwningGroupHostedChecker(func(bucket string) bool { return bucket != "foreign" })

	root := b.shardSvc.DataDirs()[0]
	writeShardLeaf(t, root, "foreign/key/v1", []int{0}, oldEnough) // owning group not hosted
	own := writeShardLeaf(t, root, "mine/key/v1", []int{0}, oldEnough)

	require.Equal(t, []string{own}, collectOrphans(t, b, nil),
		"floated-in shard is kept; locally-owned orphan is still reclaimed")
}

// TestDeleteOrphanDir_MultiGroup_RefusesNowFloatedIn proves the delete-time
// reconfirm refuses a dir whose ownership moved to a non-hosted group between the
// walk and the delete (TOCTOU close).
func TestDeleteOrphanDir_MultiGroup_RefusesNowFloatedIn(t *testing.T) {
	b := orphanWalkerBackend(t)
	hosted := true
	b.SetOwningGroupHostedChecker(func(string) bool { return hosted })

	root := b.shardSvc.DataDirs()[0]
	dir := writeShardLeaf(t, root, "bkt/key/v1", []int{0}, oldEnough)
	require.Equal(t, []string{dir}, collectOrphans(t, b, nil)) // owning group hosted => yielded

	hosted = false // ownership moved to a non-hosted group between walk and delete
	n, err := b.DeleteOrphanDir(dir)
	require.NoError(t, err)
	require.Equal(t, 0, n, "reconfirm must refuse to delete a now-floated-in dir")
	require.DirExists(t, dir)
}

// TestOrphanShardSweepAllowed_LaggingHostedGroupBlocks proves a single not-caught-up
// hosted group blocks the WHOLE sweep (the caught-up gate covers every hosted group,
// not just self).
func TestOrphanShardSweepAllowed_LaggingHostedGroupBlocks(t *testing.T) {
	b := orphanWalkerBackend(t)
	lag := laggingGroupBackend(t, b, "group-lag")
	b.SetHostedGroupBackendsSource(func() []*DistributedBackend { return []*DistributedBackend{b, lag} })

	root := b.shardSvc.DataDirs()[0]
	dir := writeShardLeaf(t, root, "bkt/key/v1", []int{0}, oldEnough)
	require.Empty(t, collectOrphans(t, b, nil),
		"a not-caught-up hosted group must block the whole sweep")

	// Control: with only the caught-up self, the same dir is reclaimed.
	b.SetHostedGroupBackendsSource(func() []*DistributedBackend { return []*DistributedBackend{b} })
	require.Equal(t, []string{dir}, collectOrphans(t, b, nil))
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
