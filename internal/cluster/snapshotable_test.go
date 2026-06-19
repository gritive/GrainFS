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
