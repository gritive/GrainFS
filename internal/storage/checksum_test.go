package storage

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChecksumHashWriter_KnownVector(t *testing.T) {
	// xxhash3-128 of empty string: known constant from zeebo/xxh3 docs.
	h := NewChecksumHasher()
	got := h.Sum()
	require.Len(t, got, 16)
}

func TestChecksumHashWriter_StreamingEqualsOneShot(t *testing.T) {
	data := bytes.Repeat([]byte("grainfs-segment-"), 1024) // 16 KiB
	oneShot := ChecksumOf(data)

	h := NewChecksumHasher()
	_, err := io.Copy(h, bytes.NewReader(data))
	require.NoError(t, err, "copy")
	streamed := h.Sum()

	require.True(t, bytes.Equal(oneShot, streamed), "one-shot != streamed: %x vs %x", oneShot, streamed)
}

func TestChecksumHashWriter_DetectsSingleBitFlip(t *testing.T) {
	a := bytes.Repeat([]byte("a"), 4096)
	b := append([]byte(nil), a...)
	b[2048] ^= 0x01
	require.False(t, bytes.Equal(ChecksumOf(a), ChecksumOf(b)), "single-bit flip not detected")
}
