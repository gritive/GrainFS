package directio

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageSize_NonNegative(t *testing.T) {
	// Whatever the platform, page size must be at least 1.
	assert.GreaterOrEqual(t, PageSize(), 1)
}

func TestAlignedCopy_PreservesContent(t *testing.T) {
	// The first len(data) bytes of the aligned buffer must be a faithful
	// copy of data, regardless of platform.
	data := []byte("the quick brown fox jumps over the lazy dog")
	buf, alignedLen := AlignedCopy(data)
	require.GreaterOrEqual(t, alignedLen, len(data))
	require.GreaterOrEqual(t, len(buf), len(data))
	assert.Equal(t, data, buf[:len(data)])
}

func TestAlignedCopy_TailIsZero(t *testing.T) {
	// On Linux the buffer is rounded up to PageSize and the tail must be
	// zero so the post-write Truncate can drop it without leaving garbage.
	// On platforms with PageSize 1 there is no tail; the test trivially holds.
	data := []byte("abc")
	buf, alignedLen := AlignedCopy(data)
	if alignedLen == len(data) {
		t.Skip("platform does not pad — no tail to inspect")
	}
	tail := buf[len(data):alignedLen]
	for i, b := range tail {
		if b != 0 {
			t.Fatalf("tail byte %d is %d, want 0", i, b)
		}
	}
}

func TestAlignedCopy_LengthIsMultipleOfPageSize(t *testing.T) {
	ps := PageSize()
	data := make([]byte, 1234)
	for i := range data {
		data[i] = byte(i)
	}
	_, alignedLen := AlignedCopy(data)
	if alignedLen%ps != 0 {
		t.Fatalf("alignedLen %d is not a multiple of page size %d", alignedLen, ps)
	}
}

// TestOpenFile_RoundTrip verifies the file we get back is fully usable and
// returns the data we wrote — the AlignedCopy + Truncate pattern that the
// caller is supposed to follow on every platform.
func TestOpenFile_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shard")
	payload := bytes.Repeat([]byte{0x42}, 8000)

	f, err := OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		// Some filesystems (e.g. tmpfs in certain configs) reject O_DIRECT.
		// We don't want CI failure on every host, so skip when the kernel
		// says no — the production code does the same fallback.
		t.Skipf("direct-I/O not supported on this filesystem: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	buf, alignedLen := AlignedCopy(payload)
	n, err := f.Write(buf)
	require.NoError(t, err)
	require.Equal(t, alignedLen, n, "Write must consume the full aligned buffer")

	require.NoError(t, f.Sync())

	// Drop the alignment pad before we read it back — the test exercises
	// the truncate-after-write protocol the package documents.
	if alignedLen != len(payload) {
		require.NoError(t, f.Truncate(int64(len(payload))))
	}
	require.NoError(t, f.Close())

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}
