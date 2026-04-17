package packblob

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBlobStore_Recovery_PreservesExistingBlobs verifies that creating a new
// BlobStore in an existing directory does not overwrite prior blob files.
// Previously, NewBlobStore always started IDs at 1, destroying existing blobs.
func TestBlobStore_Recovery_PreservesExistingBlobs(t *testing.T) {
	dir := t.TempDir()

	bs1, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)

	loc1, err := bs1.Append("key1", []byte("data1"))
	require.NoError(t, err)
	loc2, err := bs1.Append("key2", []byte("data2"))
	require.NoError(t, err)
	bs1.Close()

	// Simulate restart: create new BlobStore in same directory
	bs2, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	defer bs2.Close()

	// Existing blobs must still be readable with original locations
	got1, err := bs2.Read(loc1)
	require.NoError(t, err)
	require.Equal(t, []byte("data1"), got1)

	got2, err := bs2.Read(loc2)
	require.NoError(t, err)
	require.Equal(t, []byte("data2"), got2)
}

// TestBlobStore_Recovery_ScanAll_AfterTruncation verifies that ScanAll recovers
// all complete entries even when the last entry is partially written (crash mid-write).
func TestBlobStore_Recovery_ScanAll_AfterTruncation(t *testing.T) {
	dir := t.TempDir()

	bs, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)

	_, err = bs.Append("key1", []byte("data1"))
	require.NoError(t, err)
	_, err = bs.Append("key2", []byte("data2"))
	require.NoError(t, err)
	loc3, err := bs.Append("key3", []byte("data3"))
	require.NoError(t, err)
	bs.Close()

	// Truncate the blob to cut off entry3 halfway through its data payload
	blobPath := bs.blobPath(loc3.BlobID)
	truncateAt := int64(loc3.Offset) + int64(entryOverhead+4+len("key3"))/2
	require.NoError(t, truncateFile(t, blobPath, truncateAt))

	// New BlobStore in same dir, then ScanAll
	bs2, err := NewBlobStore(dir, 256*1024*1024)
	require.NoError(t, err)
	defer bs2.Close()

	locs, err := bs2.ScanAll()
	require.NoError(t, err)

	require.Contains(t, locs, "key1", "key1 must survive truncation")
	require.Contains(t, locs, "key2", "key2 must survive truncation")
	require.NotContains(t, locs, "key3", "key3 was partially written; must not appear")

	got1, err := bs2.Read(locs["key1"])
	require.NoError(t, err)
	require.Equal(t, []byte("data1"), got1)
}

// TestBlobStore_Recovery_MultipleBlobs verifies that ScanAll recovers entries
// from all blob files after rotation.
func TestBlobStore_Recovery_MultipleBlobs(t *testing.T) {
	dir := t.TempDir()

	// Very small max size forces rotation after each entry
	bs, err := NewBlobStore(dir, 50)
	require.NoError(t, err)

	_, err = bs.Append("k1", []byte("aaaa"))
	require.NoError(t, err)
	_, err = bs.Append("k2", []byte("bbbb"))
	require.NoError(t, err)
	_, err = bs.Append("k3", []byte("cccc"))
	require.NoError(t, err)
	bs.Close()

	bs2, err := NewBlobStore(dir, 50)
	require.NoError(t, err)
	defer bs2.Close()

	locs, err := bs2.ScanAll()
	require.NoError(t, err)

	require.Contains(t, locs, "k1")
	require.Contains(t, locs, "k2")
	require.Contains(t, locs, "k3")

	for key, loc := range locs {
		expected := map[string][]byte{"k1": []byte("aaaa"), "k2": []byte("bbbb"), "k3": []byte("cccc")}
		got, err := bs2.Read(loc)
		require.NoError(t, err)
		require.Equal(t, expected[key], got, "data mismatch for %s", key)
	}
}

func truncateFile(t *testing.T, path string, size int64) error {
	t.Helper()
	return os.Truncate(path, size)
}
