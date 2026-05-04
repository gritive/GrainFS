package cluster

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakePeerResponse is the canned ReadShard outcome for a single peer.
type fakePeerResponse struct {
	data []byte
	err  error
}

// fakePeerCall records one ReadShard invocation for assertion-by-call-log.
type fakePeerCall struct {
	peer     string
	bucket   string
	key      string
	shardIdx int
}

// fakePeerReader is an in-memory peerReader that returns canned responses
// keyed by peer address and records every call. Concurrency-safe.
type fakePeerReader struct {
	mu        sync.Mutex
	responses map[string]fakePeerResponse
	calls     []fakePeerCall
}

func (f *fakePeerReader) ReadShard(_ context.Context, peer, bucket, key string, shardIdx int) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakePeerCall{peer: peer, bucket: bucket, key: key, shardIdx: shardIdx})
	r, ok := f.responses[peer]
	if !ok {
		return nil, fmt.Errorf("fakePeerReader: no response configured for %s", peer)
	}
	return r.data, r.err
}

func (f *fakePeerReader) calledPeers() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.calls))
	for _, c := range f.calls {
		out = append(out, c.peer)
	}
	return out
}

// newTestBackendForRepair constructs a minimal *DistributedBackend that
// supports repairReplicaWith without raft / badger / QUIC. liveNodes() falls
// back to b.allNodes when b.node is nil; we exploit that here.
//
// Cooldown is set to 1h so peers marked unhealthy mid-test never spontaneously
// recover. The peerHealth Gauge metric registration uses a global registry —
// reusing the same peer name across tests is fine; the Set call is idempotent.
func newTestBackendForRepair(t *testing.T, peers []string, selfAddr string) *DistributedBackend {
	t.Helper()
	return &DistributedBackend{
		root:       t.TempDir(),
		selfAddr:   selfAddr,
		allNodes:   append([]string{}, peers...),
		peerHealth: NewPeerHealth(peers, time.Hour),
	}
}

func md5hex(b []byte) string {
	h := md5.Sum(b)
	return hex.EncodeToString(h[:])
}

func TestRepairReplicaWith_HappyPath(t *testing.T) {
	payload := []byte("hello-world-payload")
	etag := md5hex(payload)
	peers := []string{"self", "peer-a", "peer-b"}
	b := newTestBackendForRepair(t, peers, "self")
	reader := &fakePeerReader{responses: map[string]fakePeerResponse{
		"peer-a": {data: payload},
	}}

	err := b.repairReplicaWith(context.Background(), reader, "bkt", "k", "v1", etag)
	require.NoError(t, err)

	got, err := os.ReadFile(b.objectPathV("bkt", "k", "v1"))
	require.NoError(t, err)
	require.Equal(t, payload, got)

	require.Equal(t, []string{"peer-a"}, reader.calledPeers(),
		"first healthy peer matched; should short-circuit")
	require.True(t, b.peerHealth.IsHealthy("peer-a"))
}

func TestRepairReplicaWith_ETagMismatchFallback(t *testing.T) {
	payload := []byte("real-payload")
	etag := md5hex(payload)
	peers := []string{"self", "peer-a", "peer-b"}
	b := newTestBackendForRepair(t, peers, "self")
	reader := &fakePeerReader{responses: map[string]fakePeerResponse{
		"peer-a": {data: []byte("wrong-bytes")},
		"peer-b": {data: payload},
	}}

	err := b.repairReplicaWith(context.Background(), reader, "bkt", "k", "v1", etag)
	require.NoError(t, err)
	require.Equal(t, []string{"peer-a", "peer-b"}, reader.calledPeers())

	// ETag mismatch must NOT mark unhealthy — only ReadShard error does.
	// This protects us from quarantining a peer that simply happens to hold a
	// stale or corrupt copy of one specific object.
	require.True(t, b.peerHealth.IsHealthy("peer-a"))
	require.True(t, b.peerHealth.IsHealthy("peer-b"))
}

func TestRepairReplicaWith_ReadErrorMarksUnhealthy(t *testing.T) {
	payload := []byte("real-payload")
	etag := md5hex(payload)
	peers := []string{"self", "peer-a", "peer-b"}
	b := newTestBackendForRepair(t, peers, "self")
	reader := &fakePeerReader{responses: map[string]fakePeerResponse{
		"peer-a": {err: fmt.Errorf("connection refused")},
		"peer-b": {data: payload},
	}}

	err := b.repairReplicaWith(context.Background(), reader, "bkt", "k", "v1", etag)
	require.NoError(t, err)

	require.False(t, b.peerHealth.IsHealthy("peer-a"),
		"ReadShard error must trigger MarkUnhealthy")
	require.True(t, b.peerHealth.IsHealthy("peer-b"))
}

func TestRepairReplicaWith_FallbackToUnhealthy(t *testing.T) {
	payload := []byte("real-payload")
	etag := md5hex(payload)
	peers := []string{"self", "peer-a", "peer-b"}
	b := newTestBackendForRepair(t, peers, "self")
	// peer-a is pre-marked unhealthy (e.g. from a prior failed write); it
	// happens to still hold the correct bytes. peer-b is healthy but its copy
	// drifted.
	b.peerHealth.MarkUnhealthy("peer-a")
	reader := &fakePeerReader{responses: map[string]fakePeerResponse{
		"peer-a": {data: payload},
		"peer-b": {data: []byte("wrong")},
	}}

	err := b.repairReplicaWith(context.Background(), reader, "bkt", "k", "v1", etag)
	require.NoError(t, err)

	// Healthy loop visits peer-b only (peer-a skipped, mismatch).
	// Unhealthy fallback visits peer-a → matches → success + MarkHealthy.
	require.Equal(t, []string{"peer-b", "peer-a"}, reader.calledPeers())
	require.True(t, b.peerHealth.IsHealthy("peer-a"),
		"successful tryRepairFromPeer must MarkHealthy")
}

func TestRepairReplicaWith_AllPeersFail(t *testing.T) {
	etag := md5hex([]byte("real-payload"))
	peers := []string{"self", "peer-a", "peer-b"}
	b := newTestBackendForRepair(t, peers, "self")
	reader := &fakePeerReader{responses: map[string]fakePeerResponse{
		"peer-a": {err: fmt.Errorf("oops")},
		"peer-b": {data: []byte("wrong")},
	}}

	err := b.repairReplicaWith(context.Background(), reader, "bkt", "k", "v1", etag)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no peer returned matching bytes")

	// Healthy loop: peer-a (err → MarkUnhealthy, tried++), peer-b (mismatch, tried++).
	// Unhealthy fallback loop: peer-a is now unhealthy → revisited (still err, tried++);
	// peer-b still healthy → skipped.
	// This documents the current behavior — peer-a is queried twice. Future
	// optimization that dedupes per-run should update this assertion intentionally.
	require.Contains(t, err.Error(), "tried 3")
	require.Equal(t, []string{"peer-a", "peer-b", "peer-a"}, reader.calledPeers())
}

func TestRepairReplicaWith_SelfAddrFiltered(t *testing.T) {
	payload := []byte("payload")
	etag := md5hex(payload)
	peers := []string{"self", "peer-a"}
	b := newTestBackendForRepair(t, peers, "self")
	reader := &fakePeerReader{responses: map[string]fakePeerResponse{
		// "self" deliberately configured to match — if iterated, would succeed
		// without ever needing peer-a. The filter must skip it regardless.
		"self":   {data: payload},
		"peer-a": {data: payload},
	}}

	err := b.repairReplicaWith(context.Background(), reader, "bkt", "k", "v1", etag)
	require.NoError(t, err)
	require.Equal(t, []string{"peer-a"}, reader.calledPeers(),
		"self address must never be queried")
}

func TestRepairReplicaWith_NilReader(t *testing.T) {
	b := newTestBackendForRepair(t, []string{"a"}, "")
	err := b.repairReplicaWith(context.Background(), nil, "bkt", "k", "v1", "etag")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no shard service")
}

func TestRepairReplicaWith_EmptyExpectedETag(t *testing.T) {
	b := newTestBackendForRepair(t, []string{"self", "peer-a"}, "self")
	reader := &fakePeerReader{}

	err := b.repairReplicaWith(context.Background(), reader, "bkt", "k", "v1", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing expected ETag")
	require.Empty(t, reader.calls,
		"ETag guard must run before any peer query")
}

func TestRepairReplicaWith_EmptyVersionIDDefaultsCurrent(t *testing.T) {
	payload := []byte("payload")
	etag := md5hex(payload)
	peers := []string{"self", "peer-a"}
	b := newTestBackendForRepair(t, peers, "self")
	reader := &fakePeerReader{responses: map[string]fakePeerResponse{
		"peer-a": {data: payload},
	}}

	err := b.repairReplicaWith(context.Background(), reader, "bkt", "k", "", etag)
	require.NoError(t, err)

	require.Len(t, reader.calls, 1)
	require.Equal(t, "k/current", reader.calls[0].key,
		"empty versionID must default to 'current' in shardKey")

	_, err = os.Stat(b.objectPathV("bkt", "k", "current"))
	require.NoError(t, err, "file must land at objectPathV(...current)")
}
