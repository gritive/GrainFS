package cluster

import (
	"bytes"
	"context"
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
}

func (s *recordingShardStore) record(name string) { s.calls = append(s.calls, name) }

func (s *recordingShardStore) WriteLocalShardContext(context.Context, string, string, int, []byte) error {
	s.record("WriteLocalShardContext")
	return nil
}

func (s *recordingShardStore) WriteLocalShardStreamContext(_ context.Context, _ string, _ string, _ int, body io.Reader) error {
	s.record("WriteLocalShardStreamContext")
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

func (s *recordingShardStore) ReadShardRange(_ context.Context, _ string, _ string, _ string, _ int, _ int64, length int64) ([]byte, error) {
	s.record("ReadShardRange")
	return bytes.Repeat([]byte("r"), int(length)), nil
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
			name: "buffered write -> WriteLocalShardContext",
			call: func(t *testing.T, ep shardEndpoint) {
				err := ep.WriteShardReader(context.Background(), "b", "k", 0,
					func(int) (io.Reader, error) { return strings.NewReader("x"), nil },
					func(int) (int64, error) { return 1, nil })
				require.NoError(t, err)
			},
			want: "WriteLocalShardContext",
		},
		{
			name: "unknown-size write -> WriteLocalShardStreamContext",
			call: func(t *testing.T, ep shardEndpoint) {
				err := ep.WriteShardReader(context.Background(), "b", "k", 0,
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
			name: "read -> ReadLocalShard",
			call: func(t *testing.T, ep shardEndpoint) {
				_, err := ep.ReadShard(context.Background(), "b", "k", 0)
				require.NoError(t, err)
			},
			want: "ReadLocalShard",
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
			name: "buffered write -> WriteShard",
			call: func(t *testing.T, ep shardEndpoint) {
				err := ep.WriteShardReader(context.Background(), "b", "k", 0,
					func(int) (io.Reader, error) { return strings.NewReader("x"), nil },
					func(int) (int64, error) { return 1, nil })
				require.NoError(t, err)
			},
			want: "WriteShard",
		},
		{
			name: "unknown-size write -> WriteShardStream",
			call: func(t *testing.T, ep shardEndpoint) {
				err := ep.WriteShardReader(context.Background(), "b", "k", 0,
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
			name: "read -> ReadShard",
			call: func(t *testing.T, ep shardEndpoint) {
				_, err := ep.ReadShard(context.Background(), "b", "k", 0)
				require.NoError(t, err)
			},
			want: "ReadShard",
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
			name: "small readat -> ReadShardRange",
			call: func(t *testing.T, ep shardEndpoint) {
				_, err := ep.ReadShardAt(context.Background(), "b", "k", 0, 0, make([]byte, 4))
				require.NoError(t, err)
			},
			want: "ReadShardRange",
		},
		{
			name: "large readat -> ReadShardRangeStream",
			call: func(t *testing.T, ep shardEndpoint) {
				_, err := ep.ReadShardAt(context.Background(), "b", "k", 0, 0, make([]byte, maxShardRangeReplyBytes+1))
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

// TestShardTargetRemoteEndpointMarksPeerHealth pins the peerHealth contract:
// remote success marks healthy, remote failure marks unhealthy, and the local
// endpoint never marks (a node does not health-check itself).
func TestShardTargetRemoteEndpointMarksPeerHealth(t *testing.T) {
	t.Run("remote write success marks healthy", func(t *testing.T) {
		ph := &fakeECObjectPeerHealth{}
		ep := remoteShardEndpoint{node: "peer", shards: &recordingShardStore{}, peerHealth: ph, writeAttempts: 1}
		err := ep.WriteShardReader(context.Background(), "b", "k", 0,
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
		err := ep.WriteShardReader(context.Background(), "b", "k", 0,
			func(int) (io.Reader, error) { return strings.NewReader("x"), nil },
			func(int) (int64, error) { return 1, nil })
		require.NoError(t, err)
		require.Empty(t, ph.healthy)
		require.Empty(t, ph.unhealthy)
	})
}
