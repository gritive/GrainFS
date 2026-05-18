package packblob

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestPackedBackend_SaveLoadIndex_FB(t *testing.T) {
	// Build pb manually (not via newTestPackedBackend) so we can explicitly
	// close it before opening pb2 — the blobDir has an exclusive flock.
	dir := t.TempDir()
	blobDir := dir + "/blobs"
	inner, err := storage.NewLocalBackend(dir + "/local")
	require.NoError(t, err)
	pb, err := NewPackedBackend(inner, blobDir, 64*1024)
	require.NoError(t, err)

	pk := packedKey{bucket: "b1", key: "k1"}
	entry := &indexEntry{
		Location:     BlobLocation{BlobID: 7, Offset: 4096, Length: 1234},
		OriginalSize: 1234,
		ContentType:  "application/octet-stream",
		ETag:         "deadbeef",
		LastModified: 1700000000,
		UserMetadata: map[string]string{"x-amz-meta-foo": "bar"},
		SSEAlgorithm: "AES256",
	}
	entry.Refcount.Store(1)
	pb.index.Store(pk, entry)

	require.NoError(t, pb.SaveIndex())

	indexPath := filepath.Join(blobDir, "index.bin")
	require.FileExists(t, indexPath)

	// Close pb to release the exclusive blob-dir lock before opening pb2.
	require.NoError(t, pb.Close())

	// New PackedBackend reading the same blobDir.
	pb2 := newTestPackedBackendSharingDir(t, blobDir)
	require.NoError(t, pb2.LoadIndex())

	rawLoaded, ok := pb2.index.Load(pk)
	require.True(t, ok, "entry should round-trip")
	loaded := rawLoaded.(*indexEntry)
	require.Equal(t, entry.Location, loaded.Location)
	require.Equal(t, entry.OriginalSize, loaded.OriginalSize)
	require.Equal(t, entry.ETag, loaded.ETag)
	require.Equal(t, entry.UserMetadata, loaded.UserMetadata)
	require.Equal(t, entry.ContentType, loaded.ContentType)
	require.Equal(t, entry.LastModified, loaded.LastModified)
	require.Equal(t, entry.SSEAlgorithm, loaded.SSEAlgorithm)
	require.EqualValues(t, 1, loaded.Refcount.Load())
}

func TestPackedBackend_LoadIndex_LegacyJSONRejected(t *testing.T) {
	pb := newTestPackedBackend(t)
	// Write legacy JSON bytes to where new code expects index.bin.
	require.NoError(t, os.WriteFile(filepath.Join(pb.blobDir, "index.bin"), []byte("{\"b1/k1\":{}}"), 0o644))

	err := pb.LoadIndex()
	require.ErrorIs(t, err, ErrLegacyStorageFormat)
}

func TestPackedBackend_LoadIndex_MissingFile_TriggersScanAll(t *testing.T) {
	pb := newTestPackedBackend(t)
	// Fresh PackedBackend has no index.bin and no blobs. LoadIndex
	// should succeed (ScanAll returns empty).
	require.NoError(t, pb.LoadIndex())
}

// newTestPackedBackendSharingDir constructs a second PackedBackend pointed
// at an existing blobDir (for save-then-load round-trip tests).
// Implement using the same pattern as newTestPackedBackend (test file :19)
// but reusing the passed blobDir instead of t.TempDir() for blobs. If the
// production constructor requires extra setup (inner backend dir), allocate
// a fresh t.TempDir() for that side only.
func newTestPackedBackendSharingDir(t *testing.T, blobDir string) *PackedBackend {
	t.Helper()
	innerDir := t.TempDir()
	inner, err := storage.NewLocalBackend(innerDir + "/local")
	require.NoError(t, err)
	pb, err := NewPackedBackend(inner, blobDir, 64*1024)
	require.NoError(t, err)
	t.Cleanup(func() { pb.Close() })
	return pb
}
