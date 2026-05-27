package wal

import (
	"bytes"
	"testing"
)

func TestHeaderV4CarriesDekGen(t *testing.T) {
	var buf bytes.Buffer
	if err := writeHeader(&buf, fileVersionV4, 5); err != nil {
		t.Fatalf("writeHeader: %v", err)
	}
	ver, gen, err := readHeader(&buf)
	if err != nil {
		t.Fatalf("readHeader: %v", err)
	}
	if ver != fileVersionV4 || gen != 5 {
		t.Fatalf("got ver=%d gen=%d, want 4/5", ver, gen)
	}
	// v2 plain header stays 8 bytes, gen reported as 0.
	var buf2 bytes.Buffer
	if err := writeHeader(&buf2, fileVersionV2, 0); err != nil {
		t.Fatal(err)
	}
	if buf2.Len() != 8 {
		t.Fatalf("v2 header len = %d, want 8", buf2.Len())
	}
	ver2, gen2, err := readHeader(&buf2)
	if err != nil || ver2 != fileVersionV2 || gen2 != 0 {
		t.Fatalf("v2 readHeader: ver=%d gen=%d err=%v", ver2, gen2, err)
	}
}
