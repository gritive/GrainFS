package storage

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// B: writing a multi-chunk encrypted object must not allocate a fresh sealed
// buffer per 128 KiB record. The per-chunk Seal output is pooled (SealTo into a
// reused buffer), so steady-state per-write allocation stays small regardless of
// object size. A 1 MiB object is 8 records; before pooling that is ~1 MiB of
// transient sealed slices.
func TestWriteEncryptedObjectFile_MultiChunkSealPooled(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("race instrumentation inflates TotalAlloc, making the byte threshold meaningless")
	}
	enc := testSegEnc(t)
	dir := t.TempDir()
	plaintext := bytes.Repeat([]byte("y"), 1<<20) // 8 * 128 KiB records
	fields := objectFileAADFields("b", "k")

	// Warm pools so the first allocation is not counted.
	_, err := writeEncryptedObjectFile(filepath.Join(dir, "warm"), enc, fields, bytes.NewReader(plaintext), io.Discard)
	require.NoError(t, err)

	perOp := allocBytesPerRunForStorageTest(t, 20, func() error {
		_, e := writeEncryptedObjectFile(filepath.Join(dir, "obj"), enc, fields, bytes.NewReader(plaintext), io.Discard)
		return e
	})
	// Before pooling: ~1 MiB+ (a fresh sealed slice per record). After: « 256 KiB.
	require.Less(t, perOp, uint64(256<<10), "per-op alloc bytes too high: %d", perOp)
}
