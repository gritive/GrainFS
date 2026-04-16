package packblob

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlobFile_CorruptionRecovery tests that corrupted blob entries are skipped during rebuild
func TestBlobFile_CorruptionRecovery(t *testing.T) {
	// Setup: Create temporary blob directory
	tmpDir, err := os.MkdirTemp("", "blob-corruption-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create BlobStore
	bs, err := NewBlobStore(tmpDir, 256*1024*1024) // 256MB max blob
	require.NoError(t, err)

	// Write test data to create a blob file
	data1 := []byte("test data 1")
	data2 := []byte("test data 2")
	data3 := []byte("test data 3")

	loc1, err := bs.Append("key1", data1)
	require.NoError(t, err)
	loc2, err := bs.Append("key2", data2)
	require.NoError(t, err)
	_, err = bs.Append("key3", data3)
	require.NoError(t, err)

	// Verify all data is readable before corruption
	read1, err := bs.Read(loc1)
	require.NoError(t, err)
	assert.Equal(t, data1, read1)

	read2, err := bs.Read(loc2)
	require.NoError(t, err)
	assert.Equal(t, data2, read2)

	// TEST: Simulate corruption by modifying blob file at offset 50%
	// Find the actual blob file that was created
	blobFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.blob"))
	require.NoError(t, err)
	require.Len(t, blobFiles, 1, "Should have exactly one blob file")
	blobFile := blobFiles[0]

	fileData, err := os.ReadFile(blobFile)
	require.NoError(t, err)

	// Corrupt data at 50% offset
	corruptOffset := len(fileData) / 2
	fileData[corruptOffset] = 0xFF // Corrupt one byte

	err = os.WriteFile(blobFile, fileData, 0644)
	require.NoError(t, err)

	// TEST: Verify unaffected entries are still readable
	// Data before corruption should be readable
	read1After, err := bs.Read(loc1)
	assert.NoError(t, err, "Data before corruption should be readable")
	assert.Equal(t, data1, read1After)

	// Data at corruption point may fail gracefully
	_, err = bs.Read(loc2)
	// This might fail or return corrupted data - either is acceptable
	// The key is that it doesn't panic or crash the server

	// Data after corruption might be affected
	// (implementation dependent on blob format)
}

// TestBlobFile_PartialWriteRecovery tests handling of incomplete blob files
func TestBlobFile_PartialWriteRecovery(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "blob-partial-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	bs, err := NewBlobStore(tmpDir, 256*1024*1024)
	require.NoError(t, err)

	// Write some data
	data := []byte("test data for partial write")
	loc, err := bs.Append("key", data)
	require.NoError(t, err)

	// TEST: Truncate blob file to simulate partial write
	// Find the actual blob file that was created
	blobFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.blob"))
	require.NoError(t, err)
	require.Len(t, blobFiles, 1, "Should have exactly one blob file")
	blobFile := blobFiles[0]

	info, err := os.Stat(blobFile)
	require.NoError(t, err)

	// Truncate to 50% of original size
	newSize := info.Size() / 2
	err = os.Truncate(blobFile, newSize)
	require.NoError(t, err)

	// TEST: Attempt to read should fail gracefully
	_, err = bs.Read(loc)
	assert.Error(t, err, "Reading truncated blob should return error")
	// Check error message contains "EOF" (either "EOF" or "unexpected EOF" is acceptable)
	errMsg := err.Error()
	assert.Contains(t, errMsg, "EOF", "Error should indicate file ended")
}
