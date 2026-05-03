package packblob

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestCopyObject_SmallPackedObject(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "src"))
	require.NoError(t, pb.CreateBucket(context.Background(), "dst"))

	_, err := pb.PutObject(context.Background(), "src", "original.txt", strings.NewReader("copy me"), "text/plain")
	require.NoError(t, err)

	// Copy via Copier interface
	var copier storage.Copier = pb
	obj, err := copier.CopyObject("src", "original.txt", "dst", "copy.txt")
	require.NoError(t, err)
	assert.Equal(t, "copy.txt", obj.Key)
	assert.Equal(t, int64(7), obj.Size)

	// Read the copy
	rc, _, err := pb.GetObject(context.Background(), "dst", "copy.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, "copy me", string(data))
}

func TestCopyObject_RefcountIncremented(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "b"))

	_, err := pb.PutObject(context.Background(), "b", "orig.txt", strings.NewReader("shared data"), "text/plain")
	require.NoError(t, err)

	// Copy
	_, err = pb.CopyObject("b", "orig.txt", "b", "copy.txt")
	require.NoError(t, err)

	// Delete original — copy should still be readable
	require.NoError(t, pb.DeleteObject(context.Background(), "b", "orig.txt"))

	rc, _, err := pb.GetObject(context.Background(), "b", "copy.txt")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, "shared data", string(data))
}

func TestCopyObject_LargeObjectFallback(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "b"))

	// Large object (>= 64KB threshold)
	largeData := strings.Repeat("L", 65*1024)
	_, err := pb.PutObject(context.Background(), "b", "large.bin", strings.NewReader(largeData), "application/octet-stream")
	require.NoError(t, err)

	obj, err := pb.CopyObject("b", "large.bin", "b", "large-copy.bin")
	require.NoError(t, err)
	assert.Equal(t, int64(65*1024), obj.Size)

	rc, _, err := pb.GetObject(context.Background(), "b", "large-copy.bin")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, len(largeData), len(data))
}

func TestCopyObject_SourceNotFound(t *testing.T) {
	pb := newTestPackedBackend(t)
	require.NoError(t, pb.CreateBucket(context.Background(), "b"))

	_, err := pb.CopyObject("b", "nonexistent.txt", "b", "copy.txt")
	assert.Error(t, err)
}
