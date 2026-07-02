package cluster

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// buildECShards encodes payload into a cfg-shaped shard set and returns the
// full shard streams (8-byte header + body) as byte slices, plus the padded
// per-shard body size. ECSplit is the same encoder the buffered write path
// uses, so tests exercise real reconstructable data.
func buildECShards(t *testing.T, cfg ECConfig, payload []byte) ([][]byte, int) {
	t.Helper()
	full, err := ECSplit(cfg, payload)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	return full, len(full[0]) - shardHeaderSize
}

// TestECStreamRead_TruncatedShardFailsTyped pins the truncation contract on
// the all-data-shards prefetch path: a data shard whose body ends cleanly
// short must surface *ecShardTruncatedError to the consumer (never io.EOF,
// never the next shard's bytes at the wrong offset), and the fault hook must
// fire with the truncated shard's index.
func TestECStreamRead_TruncatedShardFailsTyped(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("abcdefgh"), 512) // 4 KiB
	full, bodyLen := buildECShards(t, cfg, payload)

	cut := shardHeaderSize + bodyLen/2
	shards := []io.Reader{
		bytes.NewReader(full[0][:cut]),
		bytes.NewReader(full[1]),
		nil, // parity absent — all-data-shards prefetch path
	}

	var faults []int
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, nil, func(i int) { faults = append(faults, i) })
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err == nil {
		t.Fatalf("expected truncation error, got clean read of %d bytes", len(got))
	}
	var terr *ecShardTruncatedError
	if !errors.As(err, &terr) {
		t.Fatalf("expected *ecShardTruncatedError, got %v", err)
	}
	if terr.Idx != 0 {
		t.Fatalf("truncated shard idx = %d, want 0", terr.Idx)
	}
	if !bytes.Equal(got, payload[:len(got)]) {
		t.Fatalf("mis-spliced output: delivered %d bytes that are not a payload prefix", len(got))
	}
	if len(faults) != 1 || faults[0] != 0 {
		t.Fatalf("onShardFault calls = %v, want [0]", faults)
	}
	_ = rc.Close()
}

// TestECStreamRead_MissingDataPathTruncationFailsTyped — same contract on the
// io.Pipe reconstruct branch (a data shard missing, another present shard truncated).
func TestECStreamRead_MissingDataPathTruncationFailsTyped(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("zyxwvuts"), 512)
	full, bodyLen := buildECShards(t, cfg, payload)

	cut := shardHeaderSize + bodyLen/2
	shards := []io.Reader{
		bytes.NewReader(full[0][:cut]),
		nil,
		bytes.NewReader(full[2]),
	}

	var faults []int
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, nil, func(i int) { faults = append(faults, i) })
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err = io.ReadAll(rc)
	var terr *ecShardTruncatedError
	if !errors.As(err, &terr) {
		t.Fatalf("expected *ecShardTruncatedError through the pipe path, got %v", err)
	}
	if terr.Idx != 0 {
		t.Fatalf("truncated shard idx = %d, want 0", terr.Idx)
	}
	if len(faults) != 1 || faults[0] != 0 {
		t.Fatalf("onShardFault calls = %v, want [0]", faults)
	}
	_ = rc.Close()
}

// TestECStreamRead_ExactAndOverlongShardsStillRoundTrip — regression guard:
// exact-length shards round-trip; trailing garbage past expected length is
// capped (no error, correct bytes).
func TestECStreamRead_ExactAndOverlongShardsStillRoundTrip(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("12345678"), 512)
	full, _ := buildECShards(t, cfg, payload)

	t.Run("exact", func(t *testing.T) {
		shards := []io.Reader{bytes.NewReader(full[0]), bytes.NewReader(full[1]), nil}
		rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, nil, nil)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		got, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("round-trip mismatch: got %d bytes", len(got))
		}
		_ = rc.Close()
	})

	t.Run("overlong capped", func(t *testing.T) {
		long0 := append(append([]byte{}, full[0]...), []byte("GARBAGE")...)
		shards := []io.Reader{bytes.NewReader(long0), bytes.NewReader(full[1]), nil}
		rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, nil, nil)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		got, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("overlong shard corrupted output: got %d bytes", len(got))
		}
		_ = rc.Close()
	})
}

// TestECStreamBodies_EmptyBodyHeaderIsTypedTruncation — empty body (clean EOF
// at header read) must return *ecShardTruncatedError carrying the shard index.
func TestECStreamBodies_EmptyBodyHeaderIsTypedTruncation(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("qwertyui"), 64)
	full, _ := buildECShards(t, cfg, payload)

	shards := []io.Reader{
		bytes.NewReader(nil),
		bytes.NewReader(full[1]),
		nil,
	}
	_, _, err := ecReconstructStreamBodies(cfg, shards)
	var terr *ecShardTruncatedError
	if !errors.As(err, &terr) {
		t.Fatalf("expected *ecShardTruncatedError for empty-body header, got %v", err)
	}
	if terr.Idx != 0 {
		t.Fatalf("idx = %d, want 0", terr.Idx)
	}
}
