package cluster

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestVersioning_PutThenGet verifies a single PUT is readable via GetObject
// and that the returned Object carries a VersionID.
func TestVersioning_PutThenGet(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "vbucket"))

	obj, err := b.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	assert.NotEmpty(t, obj.VersionID, "PutObject must return a VersionID")

	rc, got, err := b.GetObject(context.Background(), "vbucket", "k")
	require.NoError(t, err)
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	assert.Equal(t, "v1", string(data))
	assert.Equal(t, obj.VersionID, got.VersionID)
}

// TestVersioning_PutTwiceListVersions verifies that two PUTs to the same key
// produce two versions listed DESC by VersionID (newest first).
func TestVersioning_PutTwiceListVersions(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "vbucket"))

	o1, err := b.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)

	o2, err := b.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v2-longer"), "text/plain")
	require.NoError(t, err)

	versions, err := b.ListObjectVersions("vbucket", "k", 0)
	require.NoError(t, err)
	require.Len(t, versions, 2, "expected 2 versions")

	// ULIDs sort ASC by time; ListObjectVersions returns DESC — newest first.
	assert.Equal(t, o2.VersionID, versions[0].VersionID, "newest version first")
	assert.True(t, versions[0].IsLatest)
	assert.Equal(t, o1.VersionID, versions[1].VersionID)
	assert.False(t, versions[1].IsLatest)
	assert.False(t, versions[0].IsDeleteMarker)

	// Latest GET serves the newer payload.
	rc, _, err := b.GetObject(context.Background(), "vbucket", "k")
	require.NoError(t, err)
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	assert.Equal(t, "v2-longer", string(data))
}

// TestVersioning_DeleteCreatesTombstone verifies that DeleteObject creates a
// delete marker (listed via ListObjectVersions) and that HeadObject returns
// ErrObjectNotFound while prior versions remain addressable.
func TestVersioning_DeleteCreatesTombstone(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "vbucket"))

	_, err := b.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, b.DeleteObject(context.Background(), "vbucket", "k"))

	// HeadObject must 404 — delete marker is the latest version.
	_, err = b.HeadObject(context.Background(), "vbucket", "k")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound)

	versions, err := b.ListObjectVersions("vbucket", "k", 0)
	require.NoError(t, err)
	require.Len(t, versions, 2, "expected original version + delete marker")
	assert.True(t, versions[0].IsDeleteMarker, "newest version is the tombstone")
	assert.True(t, versions[0].IsLatest)
	assert.False(t, versions[1].IsDeleteMarker)

	// The original version remains readable via GetObjectVersion.
	rc, got, err := b.GetObjectVersion("vbucket", "k", versions[1].VersionID)
	require.NoError(t, err)
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	assert.Equal(t, "v1", string(data))
	assert.Equal(t, versions[1].VersionID, got.VersionID)
}

// TestVersioning_GetObjectVersionInvalid verifies that GetObjectVersion returns
// ErrObjectNotFound for a version that doesn't exist.
func TestVersioning_GetObjectVersionInvalid(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "vbucket"))

	_, err := b.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)

	_, _, err = b.GetObjectVersion("vbucket", "k", "01ABCDEFGHIJKLMNOPQRSTUVWX")
	assert.ErrorIs(t, err, storage.ErrObjectNotFound)
}

// TestVersioning_DeleteObjectVersion verifies that hard-delete of a specific
// version removes just that version's metadata and updates latest when needed.
func TestVersioning_DeleteObjectVersion(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "vbucket"))

	o1, err := b.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	o2, err := b.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v2"), "text/plain")
	require.NoError(t, err)

	// Hard-delete the latest → the prior version becomes the new latest.
	require.NoError(t, b.DeleteObjectVersion("vbucket", "k", o2.VersionID))

	versions, err := b.ListObjectVersions("vbucket", "k", 0)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	assert.Equal(t, o1.VersionID, versions[0].VersionID)
	assert.True(t, versions[0].IsLatest)

	// HeadObjectVersion on the deleted version → 404.
	_, err = b.HeadObjectVersion("vbucket", "k", o2.VersionID)
	assert.ErrorIs(t, err, storage.ErrObjectNotFound)
}

// TestVersioning_ListObjectsDedupes verifies that ListObjects returns a single
// row per base key even when multiple versions exist.
func TestVersioning_ListObjectsDedupes(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "vbucket"))

	_, err := b.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	_, err = b.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v2"), "text/plain")
	require.NoError(t, err)
	_, err = b.PutObject(context.Background(), "vbucket", "other", strings.NewReader("o"), "text/plain")
	require.NoError(t, err)

	objs, err := b.ListObjects(context.Background(), "vbucket", "", 100)
	require.NoError(t, err)
	require.Len(t, objs, 2, "expected 2 distinct keys despite 3 versions")
}

// TestVersioning_ListObjectsSkipsTombstones verifies that delete markers do
// not appear in ListObjects.
func TestVersioning_ListObjectsSkipsTombstones(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "vbucket"))

	_, err := b.PutObject(context.Background(), "vbucket", "k", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	_, err = b.PutObject(context.Background(), "vbucket", "kept", strings.NewReader("k"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, b.DeleteObject(context.Background(), "vbucket", "k"))

	objs, err := b.ListObjects(context.Background(), "vbucket", "", 100)
	require.NoError(t, err)
	require.Len(t, objs, 1, "tombstoned key should be excluded from ListObjects")
	assert.Equal(t, "kept", objs[0].Key)
}
