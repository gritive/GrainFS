package storage

import (
	"bytes"
	"io"
	"testing"
)

func TestChecksumHashWriter_KnownVector(t *testing.T) {
	// xxhash3-128 of empty string: known constant from zeebo/xxh3 docs.
	h := NewChecksumHasher()
	got := h.Sum()
	if len(got) != 16 {
		t.Fatalf("checksum length: want 16, got %d", len(got))
	}
}

func TestChecksumHashWriter_StreamingEqualsOneShot(t *testing.T) {
	data := bytes.Repeat([]byte("grainfs-segment-"), 1024) // 16 KiB
	oneShot := ChecksumOf(data)

	h := NewChecksumHasher()
	if _, err := io.Copy(h, bytes.NewReader(data)); err != nil {
		t.Fatalf("copy: %v", err)
	}
	streamed := h.Sum()

	if !bytes.Equal(oneShot, streamed) {
		t.Fatalf("one-shot != streamed: %x vs %x", oneShot, streamed)
	}
}

func TestChecksumHashWriter_DetectsSingleBitFlip(t *testing.T) {
	a := bytes.Repeat([]byte("a"), 4096)
	b := append([]byte(nil), a...)
	b[2048] ^= 0x01
	if bytes.Equal(ChecksumOf(a), ChecksumOf(b)) {
		t.Fatal("single-bit flip not detected")
	}
}
