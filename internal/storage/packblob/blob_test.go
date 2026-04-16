package packblob

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlobStore_WriteAndRead(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024) // 256MB max
	require.NoError(t, err)
	defer bs.Close()

	data := []byte("hello packed blob")
	loc, err := bs.Append("bucket/key1", data)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), loc.Offset) // first entry starts at offset 0
	assert.Equal(t, uint32(len(data)), loc.Length)

	got, err := bs.Read(loc)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestBlobStore_MultipleAppends(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	defer bs.Close()

	entries := []string{"first", "second", "third"}
	var locs []BlobLocation
	for i, e := range entries {
		loc, err := bs.Append("bucket/key"+string(rune('0'+i)), []byte(e))
		require.NoError(t, err)
		locs = append(locs, loc)
	}

	for i, loc := range locs {
		got, err := bs.Read(loc)
		require.NoError(t, err)
		assert.Equal(t, entries[i], string(got))
	}
}

func TestBlobStore_CRCValidation(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)

	loc, err := bs.Append("bucket/key", []byte("valid data"))
	require.NoError(t, err)

	got, err := bs.Read(loc)
	require.NoError(t, err)
	assert.Equal(t, "valid data", string(got))

	bs.Close()
}

func TestBlobStore_RotatesOnMaxSize(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 100) // tiny max for rotation testing
	require.NoError(t, err)
	defer bs.Close()

	data := bytes.Repeat([]byte("X"), 50)
	loc1, err := bs.Append("bucket/key1", data)
	require.NoError(t, err)

	loc2, err := bs.Append("bucket/key2", data)
	require.NoError(t, err)

	// Should be in different blob files
	assert.NotEqual(t, loc1.BlobID, loc2.BlobID)

	// Both readable
	got1, err := bs.Read(loc1)
	require.NoError(t, err)
	assert.Equal(t, data, got1)

	got2, err := bs.Read(loc2)
	require.NoError(t, err)
	assert.Equal(t, data, got2)
}

func TestBlobStore_EmptyData(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	defer bs.Close()

	loc, err := bs.Append("bucket/empty", []byte{})
	require.NoError(t, err)

	got, err := bs.Read(loc)
	require.NoError(t, err)
	assert.Empty(t, got)
}
