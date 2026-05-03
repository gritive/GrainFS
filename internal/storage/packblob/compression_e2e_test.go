package packblob

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestCompression_E2E verifies compressible objects round-trip correctly
// through a PackedBackend with compression enabled.
func TestCompression_E2E_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	inner, err := storage.NewLocalBackend(tmpDir + "/inner")
	require.NoError(t, err)

	pb, err := NewPackedBackendWithOptions(inner, tmpDir+"/blobs", 64*1024, PackedBackendOptions{Compress: true})
	require.NoError(t, err)
	defer pb.Close()

	require.NoError(t, pb.CreateBucket(context.Background(), "test"))

	// Highly compressible data below threshold → goes into blob
	data := bytes.Repeat([]byte("hello world "), 500)
	_, err = pb.PutObject(context.Background(), "test", "key1", bytes.NewReader(data), "text/plain")
	require.NoError(t, err)

	rc, obj, err := pb.GetObject(context.Background(), "test", "key1")
	require.NoError(t, err)
	defer rc.Close()

	require.Equal(t, int64(len(data)), obj.Size)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

// TestCompression_E2E_LargeObjectPassthrough verifies large objects (above threshold)
// pass through uncompressed to the inner backend.
func TestCompression_E2E_LargeObjectPassthrough(t *testing.T) {
	tmpDir := t.TempDir()
	inner, err := storage.NewLocalBackend(tmpDir + "/inner")
	require.NoError(t, err)

	pb, err := NewPackedBackendWithOptions(inner, tmpDir+"/blobs", 64*1024, PackedBackendOptions{Compress: true})
	require.NoError(t, err)
	defer pb.Close()

	require.NoError(t, pb.CreateBucket(context.Background(), "test"))

	// Data above threshold → passes through to inner backend (no blob)
	data := bytes.Repeat([]byte("X"), 128*1024)
	_, err = pb.PutObject(context.Background(), "test", "large", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err)

	rc, obj, err := pb.GetObject(context.Background(), "test", "large")
	require.NoError(t, err)
	defer rc.Close()

	require.Equal(t, int64(len(data)), obj.Size)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, data, got)
}
