package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestRestoreObjects_SoleAuthOn_RawVIDs proves that RestoreObjects, for a
// soleauth-on bucket, uses the RAW snapshot VersionID (not resolveRestoreObjectVersionIDs)
// and force-overwrites the per-version blob under the quiesce lock, restoring
// the snapshot state even when a newer in-place blob already exists.
func TestRestoreObjects_SoleAuthOn_RawVIDs(t *testing.T) {
	b := newSnapshotTestBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "vb"))

	// Seed the per-version blob BEFORE advancing to soleAuthOn, matching the
	// brief's test order: the blob pre-exists, then cutover happens.
	seedVersionBlob(t, b, "vb", "k", "v1", PutObjectMetaCmd{
		ETag:     "live-new",
		ModTime:  500,
		MetaSeq:  7,
		ECData:   2,
		ECParity: 1,
		NodeIDs:  []string{"n1", "n2", "n3"},
	})
	setVersioningForTest(t, b, "vb", "Enabled")
	setSoleAuthForTest(t, b, "vb", soleAuthOn)

	// Snapshot says this key/version is a delete marker at time 100.
	// IsDeleteMarker=true skips the blob existence check so we can exercise
	// the force-write path directly without a real data blob.
	// The expected ETag after restore is deleteMarkerETag ("DEL"), not "snap-old",
	// because putObjectMetaCmdFromSnapshot sets ETag=deleteMarkerETag for markers.
	snap := []storage.SnapshotObject{{
		Bucket:         "vb",
		Key:            "k",
		VersionID:      "v1",
		ETag:           "snap-old",
		Modified:       100,
		ECData:         2,
		ECParity:       1,
		NodeIDs:        []string{"n1", "n2", "n3"},
		IsLatest:       true,
		IsDeleteMarker: true, // skip blob check; exercise force-write path
	}}

	_, _, err := b.RestoreObjects(snap)
	require.NoError(t, err)

	// The per-version blob must now reflect the snapshot state — the blob was
	// force-overwritten (bypassing the LWW guard that would have kept "live-new").
	// For a delete marker, the stored ETag is always deleteMarkerETag.
	gv, rErr := b.shardSvc.readQuorumMetaVersionsLocal("vb", "k")
	require.NoError(t, rErr)
	require.Len(t, gv, 1, "expected exactly one version blob for vb/k")
	require.Equal(t, deleteMarkerETag, gv[0].ETag, "restore must force-overwrite live blob with snapshot delete-marker via raw VID")
	require.True(t, gv[0].IsDeleteMarker, "restored blob must have IsDeleteMarker=true")
}

// TestRestoreObjects_SoleAuthOn_ForceRevertsNewerBlob proves the CENTRAL
// correctness path: a NON-marker on-bucket object whose data blob IS present is
// force-overwritten — reverting a strictly-NEWER live per-version blob to the
// snapshot's OLDER one, beating the LWW guard that the normal guarded write path
// would have blocked. This is the behavior the task exists for; the delete-marker
// test above skips blobExistsForRestore, so this test exercises the real path.
func TestRestoreObjects_SoleAuthOn_ForceRevertsNewerBlob(t *testing.T) {
	b := newSnapshotTestBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "vbf"))

	// Seed a NEWER live per-version blob (ETag "live-new", high ModTime/MetaSeq)
	// BEFORE cutover. Under the guarded write path this would win LWW; force wins.
	seedVersionBlob(t, b, "vbf", "k", "v1", PutObjectMetaCmd{
		ETag:     "live-new",
		ModTime:  500,
		MetaSeq:  7,
		ECData:   2,
		ECParity: 1,
		NodeIDs:  []string{"n1", "n2", "n3"},
	})
	setVersioningForTest(t, b, "vbf", "Enabled")
	setSoleAuthForTest(t, b, "vbf", soleAuthOn)

	// Seed a REAL filesystem data blob at the versioned path so blobExistsForRestore
	// returns true (HeadObject returns ETag "live-new" != snap "snap-old", so it
	// falls through to blobExists → os.Stat(objectPathV) hit). The stale branch is
	// thus NOT taken and the force-write proceeds. Mirrors the off-path seeding in
	// TestRestoreObjects_SoleAuthOff_Unchanged.
	p := b.objectPathV("vbf", "k", "v1")
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, []byte("data"), 0o644))

	// Non-marker snapshot entry for the SAME (key, raw vid) with the OLDER ETag.
	snap := []storage.SnapshotObject{{
		Bucket:    "vbf",
		Key:       "k",
		VersionID: "v1",
		ETag:      "snap-old",
		Size:      4,
		Modified:  100, // older than the live blob's 500
		ECData:    2,
		ECParity:  1,
		NodeIDs:   []string{"n1", "n2", "n3"},
		IsLatest:  true,
	}}

	count, stale, err := b.RestoreObjects(snap)
	require.NoError(t, err)
	require.Empty(t, stale, "non-marker object with present data blob must NOT be reported stale")
	require.Equal(t, 1, count, "exactly one on-bucket object force-written")

	// The force-write must have reverted the newer "live-new" blob to "snap-old".
	// The guarded write path (writeQuorumMetaVersionLocal) would have KEPT "live-new"
	// because it wins LWW (higher ModTime/MetaSeq). Force wins → "snap-old".
	gv, rErr := b.shardSvc.readQuorumMetaVersionsLocal("vbf", "k")
	require.NoError(t, rErr)
	require.Len(t, gv, 1, "expected exactly one version blob for vbf/k")
	require.Equal(t, "snap-old", gv[0].ETag, "force-overwrite must revert newer live blob to snapshot's older one")
}

// TestRestoreObjects_SoleAuthOn_StaleSkip proves that RestoreObjects records a
// StaleBlob and does NOT write the per-version blob when a soleauth-on object's
// data blob is absent from this node.
func TestRestoreObjects_SoleAuthOn_StaleSkip(t *testing.T) {
	b := newSnapshotTestBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "vb2"))
	setVersioningForTest(t, b, "vb2", "Enabled")
	setSoleAuthForTest(t, b, "vb2", soleAuthOn)

	// No blob is seeded — it's absent. IsDeleteMarker=false → blobExistsForRestore
	// is called → absent → StaleBlob.
	snap := []storage.SnapshotObject{{
		Bucket:    "vb2",
		Key:       "missing",
		VersionID: "v-absent",
		ETag:      "etag-missing",
		Modified:  100,
		ECData:    1,
		ECParity:  1,
		NodeIDs:   []string{"n1", "n2"},
		IsLatest:  true,
	}}

	count, stale, err := b.RestoreObjects(snap)
	require.NoError(t, err)
	require.Zero(t, count, "no objects must be written when data blob is absent")
	require.Len(t, stale, 1, "must record one StaleBlob for the absent blob")
	require.Equal(t, "vb2", stale[0].Bucket)
	require.Equal(t, "missing", stale[0].Key)
	require.Equal(t, "etag-missing", stale[0].ExpectedETag)

	// Also verify no per-version blob was written.
	gv, _ := b.shardSvc.readQuorumMetaVersionsLocal("vb2", "missing")
	require.Empty(t, gv, "no quorum-meta blob must be written for a stale (absent-data) object")
}

// TestRestoreObjects_SoleAuthOn_StaleSkip_ExactVersion proves that the on-path
// presence check is EXACT-VERSION, not a latest-content shortcut. A snapshot
// version "v-aaa" whose own data blob is ABSENT must be recorded stale and NOT
// force-written, even when a strictly-newer live version "v-bbb" with IDENTICAL
// content (same ETag/Size — a re-upload of the same bytes) exists and is the
// derived latest. The HeadObject(latest) ETag/Size shortcut in blobExistsForRestore
// would falsely vouch v-aaa as present (latest "v-bbb" matches v-aaa's snapshot
// ETag/Size), publishing authoritative per-version metadata that points at data
// shares this node never had — a dangling, unreadable version. The on-path must
// bypass that shortcut.
func TestRestoreObjects_SoleAuthOn_StaleSkip_ExactVersion(t *testing.T) {
	b := newSnapshotTestBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "vex"))

	// Two per-version blobs for the SAME key with IDENTICAL content ("dup"/4):
	// v-aaa (older) and v-bbb (newer = derived latest by max-VID). Re-upload of
	// the same bytes is a legitimate way two distinct versions share ETag/Size.
	seedVersionBlob(t, b, "vex", "k", "v-aaa", PutObjectMetaCmd{ETag: "dup", Size: 4, ModTime: 100})
	seedVersionBlob(t, b, "vex", "k", "v-bbb", PutObjectMetaCmd{ETag: "dup", Size: 4, ModTime: 200})
	setVersioningForTest(t, b, "vex", "Enabled")
	setSoleAuthForTest(t, b, "vex", soleAuthOn)

	// Real data blob ONLY for v-bbb (the live latest). v-aaa's data is ABSENT on
	// this node — exactly the case the exact-version check must catch.
	p := b.objectPathV("vex", "k", "v-bbb")
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, []byte("dupx"), 0o644))

	// Snapshot carries BOTH versions (both in want → purge keeps both), each with
	// the shared ETag/Size. v-aaa's data is absent; v-bbb's is present.
	snap := []storage.SnapshotObject{
		{Bucket: "vex", Key: "k", VersionID: "v-aaa", ETag: "dup", Size: 4, Modified: 100},
		{Bucket: "vex", Key: "k", VersionID: "v-bbb", ETag: "dup", Size: 4, Modified: 200, IsLatest: true},
	}

	count, stale, err := b.RestoreObjects(snap)
	require.NoError(t, err)
	// v-aaa absent → stale, skipped. v-bbb present → written. The latest-content
	// shortcut must NOT vouch v-aaa just because live latest v-bbb matches it.
	require.Equal(t, 1, count, "only v-bbb (data present) must be force-written; v-aaa (data absent) skipped")
	require.Len(t, stale, 1, "v-aaa must be recorded stale despite identical-content newer latest")
	require.Equal(t, "k", stale[0].Key)
	require.Equal(t, "dup", stale[0].ExpectedETag)
}

// TestRestoreObjects_SoleAuthOn_StaleSkip_LegacyPlainFile proves the exact-version
// presence check does NOT vouch an absent snapshot version via a legacy
// unversioned plain file (data/<bucket>/<key>, no version suffix). blobExists
// falls through to os.Stat(objectPath(bucket,key)) for ANY versionID, which would
// publish authoritative per-version metadata for a version whose exact shares are
// missing. The on-path must use an exact-version-only check.
func TestRestoreObjects_SoleAuthOn_StaleSkip_LegacyPlainFile(t *testing.T) {
	b := newSnapshotTestBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "vlg"))
	setVersioningForTest(t, b, "vlg", "Enabled")
	setSoleAuthForTest(t, b, "vlg", soleAuthOn)

	// Seed ONLY a legacy unversioned plain file (no version suffix). The exact
	// version "v-leg" has NO objectPathV/shard data on this node.
	lp := b.objectPath("vlg", "k")
	require.NoError(t, os.MkdirAll(filepath.Dir(lp), 0o755))
	require.NoError(t, os.WriteFile(lp, []byte("legacy"), 0o644))

	snap := []storage.SnapshotObject{{
		Bucket:    "vlg",
		Key:       "k",
		VersionID: "v-leg",
		ETag:      "etag-leg",
		Size:      6,
		Modified:  100,
		IsLatest:  true,
	}}

	count, stale, err := b.RestoreObjects(snap)
	require.NoError(t, err)
	require.Zero(t, count, "absent exact version must NOT be force-written via a legacy plain file")
	require.Len(t, stale, 1, "absent exact version must be recorded stale")
	require.Equal(t, "k", stale[0].Key)

	gv, _ := b.shardSvc.readQuorumMetaVersionsLocal("vlg", "k")
	require.Empty(t, gv, "no per-version blob must be published for the absent version")
}

// TestRestoreObjects_SoleAuthOff_Unchanged proves that the off-path behaviour is
// byte-identical: existing resolveRestoreObjectVersionIDs + CmdPutObjectMeta
// re-propose still runs for off-bucket objects, and the function returns the
// expected count.
func TestRestoreObjects_SoleAuthOff_Unchanged(t *testing.T) {
	b := newSnapshotTestBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "offbkt"))
	// soleauth is off by default — no proposal needed.

	// Seed a real filesystem blob so blobExistsForRestore finds it.
	p := b.objectPathV("offbkt", "obj", "voff1")
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, []byte("data"), 0o644))

	snap := []storage.SnapshotObject{{
		Bucket:    "offbkt",
		Key:       "obj",
		VersionID: "voff1",
		ETag:      "etag-off",
		Modified:  200,
		IsLatest:  true,
	}}
	count, stale, err := b.RestoreObjects(snap)
	require.NoError(t, err)
	require.Empty(t, stale)
	require.Equal(t, 1, count)
}

// TestRestoreObjects_SoleAuthOn_PurgesAbsent proves that RestoreObjects, for a
// soleauth-on bucket, purges every on-disk version blob absent from the snapshot
// BEFORE writing the restore entries. A newer absent blob (v9) must be deleted so
// max-VID derive yields the kept snapshot version (v1) as IsLatest.
func TestRestoreObjects_SoleAuthOn_PurgesAbsent(t *testing.T) {
	b := newSnapshotTestBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "vb"))

	// Seed BEFORE cutover to soleAuthOn (seeding requires writeQuorumMetaVersionLocal
	// which checks the epoch fence; must happen before the fence is armed).
	seedVersionBlob(t, b, "vb", "k", "v1", PutObjectMetaCmd{
		ETag: "keep", ModTime: 100, ECData: 2, ECParity: 1, NodeIDs: []string{"n1", "n2", "n3"},
	})
	seedVersionBlob(t, b, "vb", "k", "v9", PutObjectMetaCmd{
		ETag: "absent", ModTime: 900, ECData: 2, ECParity: 1, NodeIDs: []string{"n1", "n2", "n3"},
	})

	setVersioningForTest(t, b, "vb", "Enabled")
	setSoleAuthForTest(t, b, "vb", soleAuthOn)

	// Snapshot only contains v1 (IsLatest=true, IsDeleteMarker=true to skip blob check).
	snap := []storage.SnapshotObject{{
		Bucket: "vb", Key: "k", VersionID: "v1", ETag: "keep", Modified: 100,
		ECData: 2, ECParity: 1, NodeIDs: []string{"n1", "n2", "n3"}, IsLatest: true,
		IsDeleteMarker: true,
	}}
	_, _, err := b.RestoreObjects(snap)
	require.NoError(t, err)

	// v9 must have been purged; only v1 remains.
	gv, rErr := b.shardSvc.readQuorumMetaVersionsLocal("vb", "k")
	require.NoError(t, rErr)
	require.Len(t, gv, 1, "v9 must be purged; only v1 remains")
	require.Equal(t, "v1", gv[0].VersionID, "the remaining blob must be v1")
}

// TestRestoreObjects_SoleAuthOn_PurgesAbsent_PresentKept proves that a version
// present in BOTH the on-disk blobs and the snapshot is NOT deleted by the purge.
func TestRestoreObjects_SoleAuthOn_PurgesAbsent_PresentKept(t *testing.T) {
	b := newSnapshotTestBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "vb3"))

	// Seed BEFORE cutover (epoch fence).
	// Seed v1 (in snapshot) and v2 (in snapshot) — both must survive.
	// v3 is absent from snapshot and must be purged.
	seedVersionBlob(t, b, "vb3", "k", "v1", PutObjectMetaCmd{
		ETag: "e1", ModTime: 100, ECData: 2, ECParity: 1, NodeIDs: []string{"n1", "n2", "n3"},
	})
	seedVersionBlob(t, b, "vb3", "k", "v2", PutObjectMetaCmd{
		ETag: "e2", ModTime: 200, ECData: 2, ECParity: 1, NodeIDs: []string{"n1", "n2", "n3"},
	})
	seedVersionBlob(t, b, "vb3", "k", "v3", PutObjectMetaCmd{
		ETag: "absent", ModTime: 300, ECData: 2, ECParity: 1, NodeIDs: []string{"n1", "n2", "n3"},
	})

	setVersioningForTest(t, b, "vb3", "Enabled")
	setSoleAuthForTest(t, b, "vb3", soleAuthOn)

	snap := []storage.SnapshotObject{
		{Bucket: "vb3", Key: "k", VersionID: "v1", ETag: "e1", Modified: 100,
			ECData: 2, ECParity: 1, NodeIDs: []string{"n1", "n2", "n3"}, IsDeleteMarker: true},
		{Bucket: "vb3", Key: "k", VersionID: "v2", ETag: "e2", Modified: 200,
			ECData: 2, ECParity: 1, NodeIDs: []string{"n1", "n2", "n3"}, IsLatest: true, IsDeleteMarker: true},
	}
	_, _, err := b.RestoreObjects(snap)
	require.NoError(t, err)

	gv, rErr := b.shardSvc.readQuorumMetaVersionsLocal("vb3", "k")
	require.NoError(t, rErr)
	require.Len(t, gv, 2, "v1 and v2 must survive; only v3 is purged")
	vids := make([]string, 0, len(gv))
	for _, v := range gv {
		vids = append(vids, v.VersionID)
	}
	require.ElementsMatch(t, []string{"v1", "v2"}, vids, "both kept versions must remain")
}
