package zstdpool

import (
	"bytes"
	"testing"
)

func TestCompressDecompressRoundTrip(t *testing.T) {
	orig := bytes.Repeat([]byte("grainfs-compressible-payload "), 4096)
	comp, err := Compress(orig)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	if len(comp) >= len(orig) {
		t.Fatalf("expected compression to shrink: comp=%d orig=%d", len(comp), len(orig))
	}
	got, err := Decompress(comp)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(got, orig) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestDecompressRejectsGarbage(t *testing.T) {
	if _, err := Decompress([]byte("not a zstd frame")); err == nil {
		t.Fatalf("expected error decompressing garbage")
	}
}
