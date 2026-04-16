package packblob

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompaction_RemovesTombstones(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	defer bs.Close()

	// Write 3 entries
	loc1, _ := bs.Append("bucket/key1", []byte("data1"))
	_, _ = bs.Append("bucket/key2", []byte("data2"))
	_, _ = bs.Append("bucket/key3", []byte("data3"))

	// Mark key2 as tombstoned (refcount 0)
	tombstones := map[string]bool{"bucket/key2": true}

	newLocs, err := bs.Compact(loc1.BlobID, tombstones)
	require.NoError(t, err)

	// key1 and key3 should be in new blob, key2 should be gone
	assert.Len(t, newLocs, 2)
	assert.Contains(t, newLocs, "bucket/key1")
	assert.Contains(t, newLocs, "bucket/key3")

	// Verify data in new locations
	got1, err := bs.Read(newLocs["bucket/key1"])
	require.NoError(t, err)
	assert.Equal(t, "data1", string(got1))

	got3, err := bs.Read(newLocs["bucket/key3"])
	require.NoError(t, err)
	assert.Equal(t, "data3", string(got3))
}

func TestCompaction_NoTombstones_NoChange(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	defer bs.Close()

	bs.Append("bucket/key1", []byte("data1"))

	newLocs, err := bs.Compact(1, map[string]bool{})
	require.NoError(t, err)
	assert.Len(t, newLocs, 1)
}

func TestCompaction_AllTombstoned(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	defer bs.Close()

	bs.Append("bucket/only", []byte("data"))

	newLocs, err := bs.Compact(1, map[string]bool{"bucket/only": true})
	require.NoError(t, err)
	assert.Empty(t, newLocs)
}
