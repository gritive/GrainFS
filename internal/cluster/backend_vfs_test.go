package cluster

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestSetVFSFixedVersionEnabled_default(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.True(t, b.VFSFixedVersionEnabled(),
		"default must be true so disk-amplification fix is active out of the box")
}

func TestSetVFSFixedVersionEnabled_toggle(t *testing.T) {
	b := newTestDistributedBackend(t)
	b.SetVFSFixedVersionEnabled(false)
	require.False(t, b.VFSFixedVersionEnabled())
	b.SetVFSFixedVersionEnabled(true)
	require.True(t, b.VFSFixedVersionEnabled())
}

func TestPutObject_VFSBucket_FixedVersionID(t *testing.T) {
	b := newTestDistributedBackend(t)
	bucket := storage.VFSBucketPrefix + "vol1"
	require.NoError(t, b.CreateBucket(bucket))

	// First PUT.
	o1, err := b.PutObject(bucket, "data.bin", strings.NewReader("aaa"), "application/octet-stream")
	require.NoError(t, err)
	require.Equal(t, "current", o1.VersionID, "VFS bucket PUT must use fixed versionID 'current'")

	// Second PUT to same key.
	o2, err := b.PutObject(bucket, "data.bin", strings.NewReader("bbbbbb"), "application/octet-stream")
	require.NoError(t, err)
	require.Equal(t, "current", o2.VersionID)

	// GetObject returns latest data.
	rc, _, err := b.GetObject(bucket, "data.bin")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, err)
	require.Equal(t, "bbbbbb", string(got))

	// ListObjectVersions returns exactly one entry.
	versions, err := b.ListObjectVersions(bucket, "", 100)
	require.NoError(t, err)
	require.Len(t, versions, 1, "VFS bucket should never accumulate multiple versions")
	require.Equal(t, "current", versions[0].VersionID)
}

func TestPutObject_VFSBucket_DisabledTogglesLegacy(t *testing.T) {
	b := newTestDistributedBackend(t)
	b.SetVFSFixedVersionEnabled(false)
	bucket := storage.VFSBucketPrefix + "legacy"
	require.NoError(t, b.CreateBucket(bucket))

	o, err := b.PutObject(bucket, "k", strings.NewReader("x"), "application/octet-stream")
	require.NoError(t, err)
	require.NotEqual(t, "current", o.VersionID, "with toggle off, legacy ULID must be used")
}

func TestPutObject_NormalBucket_StillUsesULID(t *testing.T) {
	b := newTestDistributedBackend(t)
	bucket := "normal-bucket"
	require.NoError(t, b.CreateBucket(bucket))

	o1, err := b.PutObject(bucket, "k", strings.NewReader("a"), "application/octet-stream")
	require.NoError(t, err)
	o2, err := b.PutObject(bucket, "k", strings.NewReader("b"), "application/octet-stream")
	require.NoError(t, err)
	require.NotEqual(t, o1.VersionID, o2.VersionID, "non-VFS buckets must keep multi-version behavior")
}
