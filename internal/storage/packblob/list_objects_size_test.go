package packblob

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// markerBackend simulates the inner backend behavior for packed objects:
// PutObject stores zero-byte markers, ListObjects returns them with size=0.
type markerBackend struct {
	mockBackend
	stored map[string]*storage.Object // bucket/key → object with size=0
}

func newMarkerBackend() *markerBackend {
	return &markerBackend{stored: make(map[string]*storage.Object)}
}

func (m *markerBackend) PutObject(bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	// Inner backend stores zero-byte markers for packed objects
	m.stored[bucket+"/"+key] = &storage.Object{Key: key, Size: 0, ContentType: contentType}
	return m.stored[bucket+"/"+key], nil
}

func (m *markerBackend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	if obj, ok := m.stored[bucket+"/"+key]; ok {
		return io.NopCloser(bytes.NewReader(nil)), obj, nil
	}
	return nil, nil, fmt.Errorf("not found: %s/%s", bucket, key)
}

func (m *markerBackend) HeadObject(bucket, key string) (*storage.Object, error) {
	if obj, ok := m.stored[bucket+"/"+key]; ok {
		return obj, nil
	}
	return nil, fmt.Errorf("not found: %s/%s", bucket, key)
}

func (m *markerBackend) ListObjects(bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	var result []*storage.Object
	pfx := bucket + "/" + prefix
	for ikey, obj := range m.stored {
		if strings.HasPrefix(ikey, pfx) {
			result = append(result, obj) // size=0 for all markers
		}
	}
	return result, nil
}

// TestPackedBackend_ListObjectsSizeCorrect verifies that packed objects report
// their actual size (not 0) when listed with an empty prefix.
func TestPackedBackend_ListObjectsSizeCorrect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "packblob-list-size")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	inner := newMarkerBackend()
	pb, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)
	defer pb.Close()

	content := []byte("hello packed world")
	_, err = pb.PutObject("bucket", "key1", bytes.NewReader(content), "text/plain")
	require.NoError(t, err)

	objs, err := pb.ListObjects("bucket", "", 100)
	require.NoError(t, err)
	require.Len(t, objs, 1)

	assert.Equal(t, "key1", objs[0].Key)
	assert.Equal(t, int64(len(content)), objs[0].Size,
		"ListObjects must return actual content size, not zero-byte marker size")
}

// TestPackedBackend_ListObjectsSizeCorrect_WithPrefix verifies size fix with prefix.
func TestPackedBackend_ListObjectsSizeCorrect_WithPrefix(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "packblob-list-size-prefix")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	inner := newMarkerBackend()
	pb, err := NewPackedBackend(inner, tmpDir, 1024)
	require.NoError(t, err)
	defer pb.Close()

	content := []byte("prefixed content")
	_, err = pb.PutObject("bucket", "pre/key1", bytes.NewReader(content), "text/plain")
	require.NoError(t, err)

	objs, err := pb.ListObjects("bucket", "pre/", 100)
	require.NoError(t, err)
	require.Len(t, objs, 1)

	assert.Equal(t, int64(len(content)), objs[0].Size,
		"ListObjects with prefix must return actual content size")
}
