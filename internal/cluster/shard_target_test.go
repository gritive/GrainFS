package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// recordingShardStore records which ShardService surface (local vs remote) each
// endpoint method delegates to, so the dispatcher's local/remote routing can be
// asserted without a real ShardService.
type recordingShardStore struct {
	calls []string
	// stagedStagingKey / stagedFinalKey capture the last staged-write args so the
	// endpoint-delegation tests can assert PR1 staging passes (path=staging,
	// AAD=final) correctly across the local and remote seams.
	stagedStagingKey string
	stagedFinalKey   string
	stagedSize       int64
}

func (s *recordingShardStore) record(name string) { s.calls = append(s.calls, name) }

func (s *recordingShardStore) WriteLocalShardStreamContext(_ context.Context, _ string, _ string, _ int, body io.Reader) error {
	s.record("WriteLocalShardStreamContext")
	_, _ = io.Copy(io.Discard, body)
	return nil
}

func (s *recordingShardStore) WriteLocalShardStreamSizedContext(_ context.Context, _ string, _ string, _ int, body io.Reader, _ int64) error {
	s.record("WriteLocalShardStreamSizedContext")
	_, _ = io.Copy(io.Discard, body)
	return nil
}

func (s *recordingShardStore) WriteLocalShardStreamStagedContext(_ context.Context, _ string, stagingKey, finalKey string, _ int, body io.Reader) error {
	s.record("WriteLocalShardStreamStagedContext")
	s.stagedStagingKey, s.stagedFinalKey = stagingKey, finalKey
	_, _ = io.Copy(io.Discard, body)
	return nil
}

func (s *recordingShardStore) WriteLocalShardStreamStagedSizedContext(_ context.Context, _ string, stagingKey, finalKey string, _ int, body io.Reader, streamSize, _ int64) error {
	s.record("WriteLocalShardStreamStagedSizedContext")
	s.stagedStagingKey, s.stagedFinalKey = stagingKey, finalKey
	s.stagedSize = streamSize
	_, _ = io.Copy(io.Discard, body)
	return nil
}

func (s *recordingShardStore) DeleteLocalShards(string, string) error {
	s.record("DeleteLocalShards")
	return nil
}

func (s *recordingShardStore) ReadLocalShard(string, string, int) ([]byte, error) {
	s.record("ReadLocalShard")
	return []byte("local"), nil
}

func (s *recordingShardStore) OpenLocalShard(string, string, int) (io.ReadCloser, error) {
	s.record("OpenLocalShard")
	return io.NopCloser(strings.NewReader("local")), nil
}

func (s *recordingShardStore) ReadLocalShardAt(_ string, _ string, _ int, _ int64, buf []byte) (int, error) {
	s.record("ReadLocalShardAt")
	return copy(buf, "local"), nil
}

func (s *recordingShardStore) WriteShard(context.Context, string, string, string, int, []byte) error {
	s.record("WriteShard")
	return nil
}

func (s *recordingShardStore) WriteShardStream(_ context.Context, _ string, _ string, _ string, _ int, body io.Reader) error {
	s.record("WriteShardStream")
	_, _ = io.Copy(io.Discard, body)
	return nil
}

func (s *recordingShardStore) WriteShardStreamStaged(_ context.Context, _ string, _ string, stagingKey, finalKey string, _ int, body io.Reader) error {
	s.record("WriteShardStreamStaged")
	s.stagedStagingKey, s.stagedFinalKey = stagingKey, finalKey
	_, _ = io.Copy(io.Discard, body)
	return nil
}

func (s *recordingShardStore) WriteShardStreamStagedSized(_ context.Context, _ string, _ string, stagingKey, finalKey string, _ int, body io.Reader, streamSize int64) error {
	s.record("WriteShardStreamStagedSized")
	s.stagedStagingKey, s.stagedFinalKey = stagingKey, finalKey
	s.stagedSize = streamSize
	_, _ = io.Copy(io.Discard, body)
	return nil
}

func (s *recordingShardStore) WriteShardStreamSized(_ context.Context, _ string, _ string, _ string, _ int, body io.Reader, streamSize int64) error {
	s.record("WriteShardStreamSized")
	s.stagedSize = streamSize
	_, _ = io.Copy(io.Discard, body)
	return nil
}

func (s *recordingShardStore) DeleteShards(context.Context, string, string, string) error {
	s.record("DeleteShards")
	return nil
}

func (s *recordingShardStore) ReadShard(context.Context, string, string, string, int) ([]byte, error) {
	s.record("ReadShard")
	return []byte("remote"), nil
}

func (s *recordingShardStore) ReadShardStream(context.Context, string, string, string, int) (io.ReadCloser, error) {
	s.record("ReadShardStream")
	return io.NopCloser(strings.NewReader("remote")), nil
}

func (s *recordingShardStore) ReadShardRangeStream(_ context.Context, _ string, _ string, _ string, _ int, _ int64, length int64) (io.ReadCloser, error) {
	s.record("ReadShardRangeStream")
	return io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("r"), int(length)))), nil
}

var _ ecShardStore = (*recordingShardStore)(nil)

// TestShardTargetEndpointForResolvesLocality pins the SOLE local-vs-remote
// decision: a slot equal to selfID resolves local; any other slot resolves
// remote. Both the writer's and reader's endpointFor share the same rule.
func TestShardTargetEndpointForResolvesLocality(t *testing.T) {
	store := &recordingShardStore{}
	writer := newECObjectWriter("self", store, nil)
	reader := ecObjectReader{selfID: "self", shards: store}

	tests := []struct {
		name      string
		node      string
		wantLocal bool
	}{
		{name: "self resolves local", node: "self", wantLocal: true},
		{name: "peer resolves remote", node: "peer", wantLocal: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wep := writer.endpointFor(tc.node)
			require.Equal(t, tc.wantLocal, wep.IsLocal())
			require.Equal(t, tc.node, wep.Node())

			rep := reader.endpointFor(tc.node)
			require.Equal(t, tc.wantLocal, rep.IsLocal())
			require.Equal(t, tc.node, rep.Node())
		})
	}
}

// TestShardTargetLocalEndpointDelegatesToLocalMethods asserts the local endpoint
// drives the ShardService *Local* surface for every operation.
func TestShardTargetLocalEndpointDelegatesToLocalMethods(t *testing.T) {
	tests := []struct {
		name string
		call func(t *testing.T, ep shardEndpoint)
		want string
	}{
		{
			name: "known-size write -> WriteLocalShardStreamSizedContext",
			call: func(t *testing.T, ep shardEndpoint) {
				err := ep.WriteShardReader(context.Background(), "b", "k", "", 0, -1,
					func(int) (io.Reader, error) { return strings.NewReader("x"), nil },
					func(int) (int64, error) { return 1, nil })
				require.NoError(t, err)
			},
			want: "WriteLocalShardStreamSizedContext",
		},
		{
			name: "unknown-size write -> WriteLocalShardStreamContext",
			call: func(t *testing.T, ep shardEndpoint) {
				err := ep.WriteShardReader(context.Background(), "b", "k", "", 0, -1,
					func(int) (io.Reader, error) { return strings.NewReader("x"), nil }, nil)
				require.NoError(t, err)
			},
			want: "WriteLocalShardStreamContext",
		},
		{
			name: "delete -> DeleteLocalShards",
			call: func(t *testing.T, ep shardEndpoint) {
				require.NoError(t, ep.DeleteShards(context.Background(), "b", "k"))
			},
			want: "DeleteLocalShards",
		},
		{
			name: "open -> OpenLocalShard",
			call: func(t *testing.T, ep shardEndpoint) {
				rc, err := ep.OpenShardStream(context.Background(), "b", "k", 0)
				require.NoError(t, err)
				_ = rc.Close()
			},
			want: "OpenLocalShard",
		},
		{
			name: "readat -> ReadLocalShardAt",
			call: func(t *testing.T, ep shardEndpoint) {
				_, err := ep.ReadShardAt(context.Background(), "b", "k", 0, 0, make([]byte, 4))
				require.NoError(t, err)
			},
			want: "ReadLocalShardAt",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &recordingShardStore{}
			ep := newECObjectWriter("self", store, nil).endpointFor("self")
			require.True(t, ep.IsLocal())
			tc.call(t, ep)
			require.Contains(t, store.calls, tc.want)
		})
	}
}

// TestShardTargetRemoteEndpointDelegatesToRemoteMethods asserts the remote
// endpoint drives the ShardService RPC surface for every operation.
func TestShardTargetRemoteEndpointDelegatesToRemoteMethods(t *testing.T) {
	tests := []struct {
		name string
		call func(t *testing.T, ep shardEndpoint)
		want string
	}{
		{
			name: "known-size write -> WriteShardStreamSized",
			call: func(t *testing.T, ep shardEndpoint) {
				err := ep.WriteShardReader(context.Background(), "b", "k", "", 0, -1,
					func(int) (io.Reader, error) { return strings.NewReader("x"), nil },
					func(int) (int64, error) { return 1, nil })
				require.NoError(t, err)
			},
			want: "WriteShardStreamSized",
		},
		{
			name: "unknown-size write -> WriteShardStream",
			call: func(t *testing.T, ep shardEndpoint) {
				err := ep.WriteShardReader(context.Background(), "b", "k", "", 0, -1,
					func(int) (io.Reader, error) { return strings.NewReader("x"), nil }, nil)
				require.NoError(t, err)
			},
			want: "WriteShardStream",
		},
		{
			name: "delete -> DeleteShards",
			call: func(t *testing.T, ep shardEndpoint) {
				require.NoError(t, ep.DeleteShards(context.Background(), "b", "k"))
			},
			want: "DeleteShards",
		},
		{
			name: "open -> ReadShardStream",
			call: func(t *testing.T, ep shardEndpoint) {
				rc, err := ep.OpenShardStream(context.Background(), "b", "k", 0)
				require.NoError(t, err)
				_ = rc.Close()
			},
			want: "ReadShardStream",
		},
		{
			name: "small readat -> ReadShardRangeStream",
			call: func(t *testing.T, ep shardEndpoint) {
				_, err := ep.ReadShardAt(context.Background(), "b", "k", 0, 0, make([]byte, 4))
				require.NoError(t, err)
			},
			want: "ReadShardRangeStream",
		},
		{
			name: "large readat -> ReadShardRangeStream",
			call: func(t *testing.T, ep shardEndpoint) {
				_, err := ep.ReadShardAt(context.Background(), "b", "k", 0, 0, make([]byte, 64<<10+1))
				require.NoError(t, err)
			},
			want: "ReadShardRangeStream",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &recordingShardStore{}
			ep := newECObjectWriter("self", store, nil).endpointFor("peer")
			require.False(t, ep.IsLocal())
			tc.call(t, ep)
			require.Contains(t, store.calls, tc.want)
		})
	}
}

// TestShardTargetStagedWriteRoutesAndPreservesAADKey pins the PR1 segment-staging
// contract at the endpoint seam: a non-empty stagingShardKey must route to the
// STAGED write method (never the legacy buffered/stream methods), and it must pass
// the staging key as the PHYSICAL path while the shardKey stays the FINAL key used
// as AAD. A swapped pair (staging used as AAD) would corrupt every staged read —
// this is the swap guard.
func TestShardTargetStagedWriteRoutesAndPreservesAADKey(t *testing.T) {
	const (
		finalKey   = "obj/segments/blob-1"
		stagingKey = ".segstaging/txn-9/blob-1"
	)

	t.Run("local endpoint -> WriteLocalShardStreamStagedSizedContext(staging, final)", func(t *testing.T) {
		store := &recordingShardStore{}
		ep := newECObjectWriter("self", store, nil).endpointFor("self")
		require.True(t, ep.IsLocal())
		// shardSize is non-nil: staged local writes should preserve the streaming
		// path while still using the known-size optimization.
		err := ep.WriteShardReader(context.Background(), "b", finalKey, stagingKey, 0, -1,
			func(int) (io.Reader, error) { return strings.NewReader("payload"), nil },
			func(int) (int64, error) { return int64(len("payload")), nil })
		require.NoError(t, err)
		require.Contains(t, store.calls, "WriteLocalShardStreamStagedSizedContext")
		require.NotContains(t, store.calls, "WriteLocalShardStreamStagedContext")
		require.Equal(t, stagingKey, store.stagedStagingKey, "staging key must be the physical path")
		require.Equal(t, finalKey, store.stagedFinalKey, "shardKey must remain the final AAD key")
		require.Equal(t, int64(len("payload")), store.stagedSize)
	})

	t.Run("local endpoint unknown size -> WriteLocalShardStreamStagedContext(staging, final)", func(t *testing.T) {
		store := &recordingShardStore{}
		ep := newECObjectWriter("self", store, nil).endpointFor("self")
		require.True(t, ep.IsLocal())
		err := ep.WriteShardReader(context.Background(), "b", finalKey, stagingKey, 0, -1,
			func(int) (io.Reader, error) { return strings.NewReader("payload"), nil },
			func(int) (int64, error) { return 0, errors.New("unknown shard size") })
		require.NoError(t, err)
		require.Contains(t, store.calls, "WriteLocalShardStreamStagedContext")
		require.NotContains(t, store.calls, "WriteLocalShardStreamStagedSizedContext")
		require.Equal(t, stagingKey, store.stagedStagingKey, "staging key must be the physical path")
		require.Equal(t, finalKey, store.stagedFinalKey, "shardKey must remain the final AAD key")
	})

	t.Run("remote endpoint -> WriteShardStreamStagedSized(staging, final)", func(t *testing.T) {
		store := &recordingShardStore{}
		ep := remoteShardEndpoint{node: "peer", shards: store, writeAttempts: 1}
		require.False(t, ep.IsLocal())
		err := ep.WriteShardReader(context.Background(), "b", finalKey, stagingKey, 0, -1,
			func(int) (io.Reader, error) { return strings.NewReader("payload"), nil },
			func(int) (int64, error) { return int64(len("payload")), nil })
		require.NoError(t, err)
		require.Contains(t, store.calls, "WriteShardStreamStagedSized")
		require.NotContains(t, store.calls, "WriteShardStreamStaged")
		require.NotContains(t, store.calls, "WriteShard")
		require.NotContains(t, store.calls, "WriteShardStream")
		require.Equal(t, stagingKey, store.stagedStagingKey, "staging key must be the physical path")
		require.Equal(t, finalKey, store.stagedFinalKey, "finalKey must be the AAD key")
		require.Equal(t, int64(len("payload")), store.stagedSize)
	})

	t.Run("remote endpoint unknown size -> WriteShardStreamStaged(staging, final)", func(t *testing.T) {
		store := &recordingShardStore{}
		ep := remoteShardEndpoint{node: "peer", shards: store, writeAttempts: 1}
		require.False(t, ep.IsLocal())
		err := ep.WriteShardReader(context.Background(), "b", finalKey, stagingKey, 0, -1,
			func(int) (io.Reader, error) { return strings.NewReader("payload"), nil },
			func(int) (int64, error) { return 0, errors.New("unknown shard size") })
		require.NoError(t, err)
		require.Contains(t, store.calls, "WriteShardStreamStaged")
		require.NotContains(t, store.calls, "WriteShardStreamStagedSized")
		require.NotContains(t, store.calls, "WriteShard")
		require.NotContains(t, store.calls, "WriteShardStream")
		require.Equal(t, stagingKey, store.stagedStagingKey, "staging key must be the physical path")
		require.Equal(t, finalKey, store.stagedFinalKey, "finalKey must be the AAD key")
	})
}

// TestShardTargetRemoteEndpointMarksPeerHealth pins the peerHealth contract:
// remote success marks healthy, remote failure marks unhealthy, and the local
// endpoint never marks (a node does not health-check itself).
func TestShardTargetRemoteEndpointMarksPeerHealth(t *testing.T) {
	t.Run("remote write success marks healthy", func(t *testing.T) {
		ph := &fakeECObjectPeerHealth{}
		ep := remoteShardEndpoint{node: "peer", shards: &recordingShardStore{}, peerHealth: ph, writeAttempts: 1}
		err := ep.WriteShardReader(context.Background(), "b", "k", "", 0, -1,
			func(int) (io.Reader, error) { return strings.NewReader("x"), nil },
			func(int) (int64, error) { return 1, nil })
		require.NoError(t, err)
		require.Equal(t, []string{"peer"}, ph.healthy)
		require.Empty(t, ph.unhealthy)
	})

	t.Run("local write never marks peerHealth", func(t *testing.T) {
		ph := &fakeECObjectPeerHealth{}
		writer := newECObjectWriter("self", &recordingShardStore{}, ph)
		ep := writer.endpointFor("self")
		require.True(t, ep.IsLocal())
		err := ep.WriteShardReader(context.Background(), "b", "k", "", 0, -1,
			func(int) (io.Reader, error) { return strings.NewReader("x"), nil },
			func(int) (int64, error) { return 1, nil })
		require.NoError(t, err)
		require.Empty(t, ph.healthy)
		require.Empty(t, ph.unhealthy)
	})
}

// TestWriteShardReader_SmallShard_StreamsNotBuffers pins the streaming-only
// invariant: a small (≤ old 256KiB) shard with a known size must go through the
// streaming path (WriteLocalShardStreamSizedContext). The buffered path
// (WriteLocalShardContext) was removed; its re-introduction would fail compilation
// because it no longer exists on the localShardStore interface.
func TestWriteShardReader_SmallShard_StreamsNotBuffers(t *testing.T) {
	spy := &recordingShardStore{}
	e := localShardEndpoint{node: "self", shards: spy}
	data := bytes.Repeat([]byte("x"), 4<<10) // 4KiB, under old 256KiB limit
	err := e.WriteShardReader(context.Background(), "b", "k", "", 0, int64(len(data)),
		func(int) (io.Reader, error) { return bytes.NewReader(data), nil },
		func(int) (int64, error) { return int64(len(data)), nil })
	require.NoError(t, err)
	require.Contains(t, spy.calls, "WriteLocalShardStreamSizedContext", "small shard must use streaming path")
}

// TestReadShardAt_SmallRange_StreamsNotBuffers is a regression guard: all range
// reads (small or large) must use the streaming RPC (ReadShardRangeStream).
func TestReadShardAt_SmallRange_StreamsNotBuffers(t *testing.T) {
	spy := &recordingShardStore{}
	e := remoteShardEndpoint{node: "peer", shards: spy}
	buf := make([]byte, 8<<10) // 8KiB
	n, err := e.ReadShardAt(context.Background(), "b", "k", 0, 0, buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Contains(t, spy.calls, "ReadShardRangeStream", "small range must use streaming RPC")
}

// errAfterReader yields data, then a mid-body error (models a peer that
// serves 200 + partial body, then resets the connection).
type errAfterReader struct {
	data []byte
	err  error
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}

func (r *errAfterReader) Close() error { return nil }

// streamShardStore returns a canned body from the streaming read RPCs.
type streamShardStore struct {
	recordingShardStore
	body io.ReadCloser
}

func (s *streamShardStore) ReadShardStream(ctx context.Context, peer, bucket, key string, shardIdx int) (io.ReadCloser, error) {
	s.record("ReadShardStream")
	return s.body, nil
}

func (s *streamShardStore) ReadShardRangeStream(ctx context.Context, peer, bucket, key string, shardIdx int, offset, length int64) (io.ReadCloser, error) {
	s.record("ReadShardRangeStream")
	return s.body, nil
}

// TestOpenShardStreamMarksPeerUnhealthyOnMidBodyFailure pins the post-open
// health contract: a shard stream that opens OK (peer marked healthy) but
// fails mid-body must flip the peer unhealthy, exactly once. Normal EOF and
// caller-driven context cancellation must NOT flip health.
func TestOpenShardStreamMarksPeerUnhealthyOnMidBodyFailure(t *testing.T) {
	t.Run("mid-body transport error marks unhealthy once", func(t *testing.T) {
		ph := &fakeECObjectPeerHealth{}
		store := &streamShardStore{body: &errAfterReader{data: []byte("partial"), err: errors.New("connection reset by peer")}}
		ep := remoteShardEndpoint{node: "peer", shards: store, peerHealth: ph}

		rc, err := ep.OpenShardStream(context.Background(), "b", "k", 0)
		require.NoError(t, err)
		require.Equal(t, []string{"peer"}, ph.healthy, "open success marks healthy")

		buf := make([]byte, 16)
		_, err = io.ReadFull(rc, buf)
		require.Error(t, err)
		_, _ = rc.Read(buf) // second read after error must not double-mark
		require.NoError(t, rc.Close())

		require.Equal(t, []string{"peer"}, ph.unhealthy, "mid-body failure marks unhealthy exactly once")
	})

	t.Run("clean EOF does not mark unhealthy", func(t *testing.T) {
		ph := &fakeECObjectPeerHealth{}
		store := &streamShardStore{body: io.NopCloser(strings.NewReader("full body"))}
		ep := remoteShardEndpoint{node: "peer", shards: store, peerHealth: ph}

		rc, err := ep.OpenShardStream(context.Background(), "b", "k", 0)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())

		require.Empty(t, ph.unhealthy)
	})

	t.Run("caller context cancellation does not mark unhealthy", func(t *testing.T) {
		ph := &fakeECObjectPeerHealth{}
		store := &streamShardStore{body: &errAfterReader{data: []byte("partial"), err: fmt.Errorf("read frame: %w", context.Canceled)}}
		ep := remoteShardEndpoint{node: "peer", shards: store, peerHealth: ph}

		rc, err := ep.OpenShardStream(context.Background(), "b", "k", 0)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, rc)
		require.Error(t, err)
		require.NoError(t, rc.Close())

		require.Empty(t, ph.unhealthy, "consumer abort is not peer fault")
	})

	t.Run("shardRPCTimeout deadline does not mark unhealthy", func(t *testing.T) {
		// openShardReaders bounds the WHOLE body read with shardRPCTimeout, so a
		// legitimately large/slow streaming GET can hit the deadline mid-body on a
		// healthy peer — that must not poison peerHealth.
		ph := &fakeECObjectPeerHealth{}
		store := &streamShardStore{body: &errAfterReader{data: []byte("partial"), err: fmt.Errorf("read frame: %w", context.DeadlineExceeded)}}
		ep := remoteShardEndpoint{node: "peer", shards: store, peerHealth: ph}

		rc, err := ep.OpenShardStream(context.Background(), "b", "k", 0)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, rc)
		require.Error(t, err)
		require.NoError(t, rc.Close())

		require.Empty(t, ph.unhealthy, "deadline on a large read is not peer fault")
	})

	t.Run("ReadShardAt mid-body transport error marks unhealthy", func(t *testing.T) {
		ph := &fakeECObjectPeerHealth{}
		store := &streamShardStore{body: &errAfterReader{data: []byte("par"), err: errors.New("connection reset by peer")}}
		ep := remoteShardEndpoint{node: "peer", shards: store, peerHealth: ph}

		buf := make([]byte, 16)
		_, err := ep.ReadShardAt(context.Background(), "b", "k", 0, 0, buf)
		require.Error(t, err)
		require.Equal(t, []string{"peer"}, ph.unhealthy, "ranged read mid-body transport failure marks unhealthy")
	})

	t.Run("ReadShardAt short read (ErrUnexpectedEOF) does not mark unhealthy", func(t *testing.T) {
		// Pins the existing documented semantic: post-RPC short-read returns
		// ErrUnexpectedEOF but does NOT flip the peer unhealthy.
		ph := &fakeECObjectPeerHealth{}
		store := &streamShardStore{body: io.NopCloser(strings.NewReader("par"))}
		ep := remoteShardEndpoint{node: "peer", shards: store, peerHealth: ph}

		buf := make([]byte, 16)
		_, err := ep.ReadShardAt(context.Background(), "b", "k", 0, 0, buf)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.Empty(t, ph.unhealthy)
	})
}
