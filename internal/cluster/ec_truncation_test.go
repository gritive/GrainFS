package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
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
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, nil, func(i int) { faults = append(faults, i) }, nil)
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
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, nil, func(i int) { faults = append(faults, i) }, nil)
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
		rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, nil, nil, nil)
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
		rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, nil, nil, nil)
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

// errAfterBytesReader yields data then a fixed non-EOF error, modeling a
// transport failure mid-body.
type errAfterBytesReader struct {
	data []byte
	err  error
}

func (r *errAfterBytesReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}

// TestECStreamRead_TransportErrorPassesThroughUntyped pins the guard's
// division of labor: a non-EOF (transport) error mid-body must pass through
// unwrapped — no *ecShardTruncatedError, no onFault — because the
// endpoint-layer healthTrackingReadCloser already owns transport-error
// marking. A regression here would double-mark peers.
func TestECStreamRead_TransportErrorPassesThroughUntyped(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("transprt"), 512)
	full, bodyLen := buildECShards(t, cfg, payload)

	connReset := errors.New("connection reset by peer")
	shards := []io.Reader{
		&errAfterBytesReader{data: full[0][:shardHeaderSize+bodyLen/2], err: connReset},
		bytes.NewReader(full[1]),
		nil,
	}

	var faults []int
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, nil, func(i int) { faults = append(faults, i) }, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err = io.ReadAll(rc)
	if err == nil {
		t.Fatal("expected transport error")
	}
	var terr *ecShardTruncatedError
	if errors.As(err, &terr) {
		t.Fatalf("transport error must not be typed as truncation, got %v", err)
	}
	if !errors.Is(err, connReset) {
		t.Fatalf("transport error must pass through unwrapped, got %v", err)
	}
	if len(faults) != 0 {
		t.Fatalf("onShardFault must not fire for transport errors, got %v", faults)
	}
	_ = rc.Close()
}

// TestECStreamBodies_PartialHeaderIsTypedTruncation covers the
// io.ErrUnexpectedEOF half of the header guard: 1-7 header bytes then clean
// close must be typed with the bytes actually read.
func TestECStreamBodies_PartialHeaderIsTypedTruncation(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("qwertyui"), 64)
	full, _ := buildECShards(t, cfg, payload)

	shards := []io.Reader{
		bytes.NewReader(full[0][:4]), // 4 of 8 header bytes
		bytes.NewReader(full[1]),
		nil,
	}
	_, _, err := ecReconstructStreamBodies(cfg, shards)
	var terr *ecShardTruncatedError
	if !errors.As(err, &terr) {
		t.Fatalf("expected *ecShardTruncatedError for partial header, got %v", err)
	}
	if terr.Idx != 0 || terr.Got != 4 || terr.Want != shardHeaderSize {
		t.Fatalf("got Idx=%d Got=%d Want=%d, want Idx=0 Got=4 Want=%d", terr.Idx, terr.Got, terr.Want, shardHeaderSize)
	}
}

// TestECStreamBodies_HostileHeaderSizeRejected pins the untrusted-header
// bounds check: a header size that would overflow the ceil(origSize/K) math
// (or a negative size from uint64 wrap) must fail the open with an explicit
// error — not silently disable the guard via negative shardBodySize.
func TestECStreamBodies_HostileHeaderSizeRejected(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}

	for _, size := range []uint64{
		math.MaxInt64,             // ceil math would wrap negative
		math.MaxInt64 + 1,         // int64 cast goes negative
		uint64(math.MaxInt64) - 1, // still overflows origSize + K - 1
	} {
		var h [shardHeaderSize]byte
		binary.BigEndian.PutUint64(h[:], size)
		body := append(append([]byte{}, h[:]...), []byte("xx")...)
		shards := []io.Reader{bytes.NewReader(body), bytes.NewReader(body), nil}

		_, _, err := ecReconstructStreamBodies(cfg, shards)
		if err == nil {
			t.Fatalf("size %d: expected out-of-range rejection", size)
		}
		var terr *ecShardTruncatedError
		if errors.As(err, &terr) {
			t.Fatalf("size %d: hostile header must not be typed as truncation (no health marking for unattributable corruption), got %v", size, err)
		}
	}
}

// TestECStreamRead_ZeroSizeObjectRoundTrips pins the origSize==0 boundary:
// header-only shard streams (empty object) read as an empty stream with no
// error and no fault.
func TestECStreamRead_ZeroSizeObjectRoundTrips(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	h := encodeShardHeader(0)
	mk := func() io.Reader { return bytes.NewReader(append([]byte{}, h[:]...)) }
	shards := []io.Reader{mk(), mk(), mk()}

	var faults []int
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, nil, func(i int) { faults = append(faults, i) }, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 0 || len(faults) != 0 {
		t.Fatalf("zero-size object: got %d bytes, faults %v", len(got), faults)
	}
	_ = rc.Close()
}

// TestECStreamRead_TruncatedParityShardOnPipePathIsTyped pins parity-shard
// attribution (Idx >= DataShards) through the missing-data reconstruct branch.
func TestECStreamRead_TruncatedParityShardOnPipePathIsTyped(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("parityyy"), 512)
	full, bodyLen := buildECShards(t, cfg, payload)

	cut := shardHeaderSize + bodyLen/2
	shards := []io.Reader{
		bytes.NewReader(full[0]),
		nil,                            // missing data shard → pipe branch
		bytes.NewReader(full[2][:cut]), // truncated parity shard
	}

	var faults []int
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, nil, func(i int) { faults = append(faults, i) }, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err = io.ReadAll(rc)
	var terr *ecShardTruncatedError
	if !errors.As(err, &terr) {
		t.Fatalf("expected *ecShardTruncatedError, got %v", err)
	}
	if terr.Idx != 2 {
		t.Fatalf("truncated shard idx = %d, want 2 (parity)", terr.Idx)
	}
	if len(faults) != 1 || faults[0] != 2 {
		t.Fatalf("onShardFault calls = %v, want [2]", faults)
	}
	_ = rc.Close()
}
