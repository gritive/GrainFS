package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// truncStreamStore serves canned whole-shard streams per shard index via both
// the remote streaming RPC surface and the local open, so
// ecObjectReader.OpenObject exercises the real endpoint → header-parse →
// guard chain for either endpoint kind.
type truncStreamStore struct {
	recordingShardStore
	streams [][]byte
}

func (s *truncStreamStore) ReadShardStream(_ context.Context, _ string, _ string, _ string, shardIdx int) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.streams[shardIdx])), nil
}

func (s *truncStreamStore) OpenLocalShard(_ string, _ string, shardIdx int) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.streams[shardIdx])), nil
}

// TestOpenObject_TruncatedRemoteShardMarksPeerUnhealthy pins end-to-end
// attribution: a remote peer serving a cleanly-truncated shard body fails the
// GET with *ecShardTruncatedError AND lands in peerHealth.unhealthy.
func TestOpenObject_TruncatedRemoteShardMarksPeerUnhealthy(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("abcdefgh"), 512)
	full, bodyLen := buildECShards(t, cfg, payload)

	streams := [][]byte{full[0][:shardHeaderSize+bodyLen/2], full[1], full[2]}
	ph := &fakeECObjectPeerHealth{}
	store := &truncStreamStore{streams: streams}
	r := ecObjectReader{selfID: "self", shards: store, peerHealth: ph, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"peer-a", "peer-b", "peer-c"}, K: 2, M: 1}

	rc, err := r.OpenObject(context.Background(), "b", "k", rec, int64(len(payload)))
	require.NoError(t, err)
	_, err = io.ReadAll(rc)
	var terr *ecShardTruncatedError
	require.True(t, errors.As(err, &terr), "want typed truncation, got %v", err)
	require.Equal(t, 0, terr.Idx)
	require.Equal(t, []string{"peer-a"}, ph.unhealthy, "truncating peer must be marked")
	_ = rc.Close()
}

// TestOpenObject_HeaderLieMarksLyingPeerOnly pins end-to-end anchored-mode
// attribution: shard 0 (peer-a) lies in its header against the
// metadata-authoritative objectSize; shards 1 and 2 are honest. OpenObject
// must fail typed with *ecShardHeaderMismatchError and mark peer-a — and only
// peer-a — unhealthy.
func TestOpenObject_HeaderLieMarksLyingPeerOnly(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("abcdefgh"), 512)
	full, _ := buildECShards(t, cfg, payload)

	lie := encodeShardHeader(int64(len(payload)) + 8)
	corrupt0 := append(append([]byte{}, lie[:]...), full[0][shardHeaderSize:]...)

	streams := [][]byte{corrupt0, full[1], full[2]}
	ph := &fakeECObjectPeerHealth{}
	store := &truncStreamStore{streams: streams}
	r := ecObjectReader{selfID: "self", shards: store, peerHealth: ph, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"peer-a", "peer-b", "peer-c"}, K: 2, M: 1}

	_, err := r.OpenObject(context.Background(), "b", "k", rec, int64(len(payload)))
	require.Error(t, err)
	var merr *ecShardHeaderMismatchError
	require.True(t, errors.As(err, &merr), "want typed header mismatch, got %v", err)
	require.True(t, merr.AnchorCorroborated, "honest shards 1,2 corroborate the anchor")
	require.Equal(t, []string{"peer-a"}, ph.unhealthy, "only the lying peer must be marked")
}

// TestOpenObject_CorruptAnchorMarksNobody pins the mass-marking guard: when
// every readable shard agrees with the others but disagrees with the metadata
// anchor, the anchor is just as suspect as the shards (metadata corruption or
// full collusion — indistinguishable). The open must still fail typed, but NO
// peer may be marked unhealthy — otherwise one corrupt quorum-meta entry would
// node-level-taint every peer serving the object on every retry.
func TestOpenObject_CorruptAnchorMarksNobody(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("abcdefgh"), 512)
	full, _ := buildECShards(t, cfg, payload)

	ph := &fakeECObjectPeerHealth{}
	store := &truncStreamStore{streams: full}
	r := ecObjectReader{selfID: "self", shards: store, peerHealth: ph, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"peer-a", "peer-b", "peer-c"}, K: 2, M: 1}

	// Honest shards, corrupt anchor: objectSize disagrees with every header.
	_, err := r.OpenObject(context.Background(), "b", "k", rec, int64(len(payload))+8)
	require.Error(t, err)
	var merr *ecShardHeaderMismatchError
	require.True(t, errors.As(err, &merr), "want typed header mismatch, got %v", err)
	require.False(t, merr.AnchorCorroborated, "no shard agreed with the anchor")
	require.Empty(t, ph.unhealthy, "unanimous disagreement must not mark any peer")
}

// TestOpenObject_EmptyBodyHeaderMarksPeerUnhealthy — empty body: the open
// itself fails typed, and the peer is marked.
func TestOpenObject_EmptyBodyHeaderMarksPeerUnhealthy(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("qwertyui"), 64)
	full, _ := buildECShards(t, cfg, payload)

	streams := [][]byte{nil, full[1], full[2]}
	ph := &fakeECObjectPeerHealth{}
	store := &truncStreamStore{streams: streams}
	r := ecObjectReader{selfID: "self", shards: store, peerHealth: ph, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"peer-a", "peer-b", "peer-c"}, K: 2, M: 1}

	_, err := r.OpenObject(context.Background(), "b", "k", rec, int64(len(payload)))
	var terr *ecShardTruncatedError
	require.True(t, errors.As(err, &terr), "want typed truncation from open, got %v", err)
	require.Contains(t, ph.unhealthy, "peer-a")
}

// TestOpenObject_LocalTruncatedShardDoesNotSelfMark — a node never
// health-marks itself, even for its own truncated shard.
func TestOpenObject_LocalTruncatedShardDoesNotSelfMark(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("selftest"), 512)
	full, bodyLen := buildECShards(t, cfg, payload)

	streams := [][]byte{full[0][:shardHeaderSize+bodyLen/2], full[1], full[2]}
	ph := &fakeECObjectPeerHealth{}
	store := &truncStreamStore{streams: streams}
	r := ecObjectReader{selfID: "self", shards: store, peerHealth: ph, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"self", "peer-b", "peer-c"}, K: 2, M: 1}

	rc, err := r.OpenObject(context.Background(), "b", "k", rec, int64(len(payload)))
	require.NoError(t, err)
	_, err = io.ReadAll(rc)
	var terr *ecShardTruncatedError
	require.True(t, errors.As(err, &terr))
	require.Empty(t, ph.unhealthy, "a node must not mark itself")
	_ = rc.Close()
}

// TestStripeStream_TruncatedFragmentIsTypedAndMarks — stripe path: a short
// fragment read surfaces *ecShardTruncatedError and fires the hook with the
// truncated shard's index.
func TestStripeStream_TruncatedFragmentIsTypedAndMarks(t *testing.T) {
	const stripeBytes = 1024
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("stripedd"), 512) // 4 KiB = 4 stripes
	bodies := buildInterleavedShards(t, cfg, payload, stripeBytes)

	// Truncate shard 0 mid-fragment: one full stripe fragment plus half of the
	// next, so fill()'s io.ReadFull on stripe 1 comes up short.
	fragSize := stripeFragSize(stripeBytes, cfg.DataShards)
	bodies[0] = bodies[0][:fragSize+fragSize/2]

	var faults []int
	rc, err := newStripeDeinterleaveStreamReader(cfg, bodyReaders(bodies), stripeBytes, int64(len(payload)), func(i int) { faults = append(faults, i) })
	require.NoError(t, err)
	_, err = io.ReadAll(rc)
	var terr *ecShardTruncatedError
	require.True(t, errors.As(err, &terr), "want typed truncation, got %v", err)
	require.Equal(t, 0, terr.Idx)
	require.Equal(t, []int{0}, faults, "hook must fire once with the truncated shard index")
	_ = rc.Close()
}

// TestOpenObject_StripedTruncatedRemoteShardMarksPeerUnhealthy pins striped
// end-to-end attribution: OpenObject with StripeBytes>0 routes through the
// stripe de-interleave reader (headers skipped lazily), and a remote shard
// whose stream ends cleanly mid-fragment fails typed AND marks the peer.
func TestOpenObject_StripedTruncatedRemoteShardMarksPeerUnhealthy(t *testing.T) {
	const stripeBytes = 1024
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("stripeee"), 512) // 4 KiB = 4 stripes
	bodies := buildInterleavedShards(t, cfg, payload, stripeBytes)

	// Whole-shard streams = 8-byte header + interleaved body; truncate shard 0
	// after one full fragment plus half of the next.
	h := encodeShardHeader(int64(len(payload)))
	fragSize := stripeFragSize(stripeBytes, cfg.DataShards)
	streams := make([][]byte, len(bodies))
	for i, b := range bodies {
		streams[i] = append(append([]byte{}, h[:]...), b...)
	}
	streams[0] = streams[0][:shardHeaderSize+fragSize+fragSize/2]

	ph := &fakeECObjectPeerHealth{}
	store := &truncStreamStore{streams: streams}
	r := ecObjectReader{selfID: "self", shards: store, peerHealth: ph, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"peer-a", "peer-b", "peer-c"}, K: 2, M: 1, StripeBytes: stripeBytes}

	rc, err := r.OpenObject(context.Background(), "b", "k", rec, int64(len(payload)))
	require.NoError(t, err)
	_, err = io.ReadAll(rc)
	var terr *ecShardTruncatedError
	require.True(t, errors.As(err, &terr), "want typed truncation, got %v", err)
	require.Equal(t, 0, terr.Idx)
	require.Equal(t, []string{"peer-a"}, ph.unhealthy, "striped truncating peer must be marked")
	_ = rc.Close()
}

// TestOpenObject_StripedHeaderLieMarksLyingPeerOnly pins striped end-to-end
// header validation (the board acceptance gap: the stripe branch used to skip
// the header BLIND via skipReader, so a lying header there was never
// validated). A lying shard-0 header on an otherwise-honest striped object
// must surface *ecShardHeaderMismatchError with that shard's idx and mark
// exactly that peer unhealthy.
func TestOpenObject_StripedHeaderLieMarksLyingPeerOnly(t *testing.T) {
	const stripeBytes = 1024
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("stripelie"), 512)
	bodies := buildInterleavedShards(t, cfg, payload, stripeBytes)

	h := encodeShardHeader(int64(len(payload)))
	streams := make([][]byte, len(bodies))
	for i, b := range bodies {
		streams[i] = append(append([]byte{}, h[:]...), b...)
	}
	// Corrupt shard 0's header only; its interleaved body is untouched.
	lie := encodeShardHeader(int64(len(payload)) + 8)
	streams[0] = append(append([]byte{}, lie[:]...), bodies[0]...)

	ph := &fakeECObjectPeerHealth{}
	store := &truncStreamStore{streams: streams}
	r := ecObjectReader{selfID: "self", shards: store, peerHealth: ph, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"peer-a", "peer-b", "peer-c"}, K: 2, M: 1, StripeBytes: stripeBytes}

	rc, err := r.OpenObject(context.Background(), "b", "k", rec, int64(len(payload)))
	require.NoError(t, err) // stripe header check is lazy: open succeeds, first Read fails
	_, err = io.ReadAll(rc)
	var merr *ecShardHeaderMismatchError
	require.True(t, errors.As(err, &merr), "want typed header mismatch, got %v", err)
	require.Equal(t, []int{0}, merr.Idxs)
	require.Equal(t, []string{"peer-a"}, ph.unhealthy, "only the lying peer must be marked")
	_ = rc.Close()
}
