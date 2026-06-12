package cluster

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeShadowTransport embeds the shardTransport interface (nil) and overrides
// only CallBuffered — the sole method the quorum-meta-shadow remote path uses
// (the native /shard/rpc route, Phase 8 N7-3). Any other method call panics,
// which keeps the fake honest about its surface.
type fakeShadowTransport struct {
	shardTransport
	mu       sync.Mutex
	addrs    []string
	failAddr map[string]bool
}

func (f *fakeShadowTransport) CallBuffered(_ context.Context, addr, _ string, _ []byte) ([]byte, error) {
	f.mu.Lock()
	f.addrs = append(f.addrs, addr)
	fail := f.failAddr[addr]
	f.mu.Unlock()
	if fail {
		return marshalResponseDirect("Error", []byte("forced")), nil
	}
	return marshalResponseDirect("OK", nil), nil
}

func (f *fakeShadowTransport) calledAddrs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.addrs...)
}

func newShadowTestShardService(t *testing.T, tr shardTransport) *ShardService {
	t.Helper()
	keeper, clusterID := testDEKKeeper(t)
	return NewShardService(t.TempDir(), tr, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
}

func TestWriteShadowMetaLocal_WritesFile(t *testing.T) {
	s := newShadowTestShardService(t, &fakeShadowTransport{})

	require.NoError(t, s.writeShadowMetaLocal("bucket", "obj/key", []byte("meta-payload")))

	path := filepath.Join(s.dataDirs[0], ".shadow_meta", "bucket", "obj/key")
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "meta-payload", string(got))
}

func TestWriteShadowMetaLocal_RejectsTraversal(t *testing.T) {
	s := newShadowTestShardService(t, &fakeShadowTransport{})
	require.Error(t, s.writeShadowMetaLocal("bucket", "../../escape", []byte("x")))
}

func TestHandleShadowMeta_RoutesThroughRPCAndWrites(t *testing.T) {
	s := newShadowTestShardService(t, &fakeShadowTransport{})

	fw := buildShardEnvelope("WriteShadowMeta", "bucket", "k1", 0, []byte("blob"))
	payload := fw.Builder.FinishedBytes()
	resp := s.handleRPC(&transport.Message{Type: transport.StreamData, Payload: payload})

	rpcType, _, err := unmarshalEnvelope(resp.Payload)
	require.NoError(t, err)
	require.Equal(t, "OK", rpcType)

	got, err := os.ReadFile(filepath.Join(s.dataDirs[0], ".shadow_meta", "bucket", "k1"))
	require.NoError(t, err)
	require.Equal(t, "blob", string(got))
}

// fanOut must return as soon as K acks arrive — NOT wait for all N. Two nodes
// block forever; the call must still return nil promptly. A wait-all
// implementation would hang here (RED).
func TestFanOutQuorumMetaShadow_ReturnsAtKAcks(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	nodes := []string{"a", "b", "c", "d", "e", "f"}

	done := make(chan error, 1)
	go func() {
		done <- fanOutQuorumMetaShadow(context.Background(), nodes, 4, func(_ context.Context, node string) error {
			if node == "e" || node == "f" {
				<-block // straggler: never acks within the test window
			}
			return nil
		})
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("fanOut did not return at K acks — waited for all N (fire-and-forget regression or wait-all)")
	}
}

func TestFanOutQuorumMetaShadow_QuorumUnreachable(t *testing.T) {
	nodes := []string{"a", "b", "c", "d", "e", "f"}
	// 3 failures out of 6 with K=4 → only 3 can succeed → quorum unreachable.
	err := fanOutQuorumMetaShadow(context.Background(), nodes, 4, func(_ context.Context, node string) error {
		if node == "a" || node == "b" || node == "c" {
			return assertErr
		}
		return nil
	})
	require.Error(t, err)
}

var assertErr = &shadowTestErr{}

type shadowTestErr struct{}

func (*shadowTestErr) Error() string { return "forced failure" }

// Real-transport carrier proof: the remote WriteShadowMeta RPC must actually
// round-trip through the TCP transport, inbound dispatch, handleRPC, the new
// case, handleShadowMeta, and land a file on the remote node. The fake-transport
// unit tests only prove the coordinator's fan-out logic — this proves the wire
// path the GCP run depends on (neuter-verify: deleting the "WriteShadowMeta"
// case in handleRPC makes this RED).
func TestWriteShadowMeta_RealTransportRoundTrip(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	tr1 := transport.MustNewHTTPTransport("test-cluster-psk")
	tr2 := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()
	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	dir1, dir2 := t.TempDir(), t.TempDir()
	svc1 := NewShardService(dir1, tr1, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svc2 := NewShardService(dir2, tr2, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	tr2.SetStreamHandler(svc2.HandleRPC())
	tr2.RegisterBufferedRoute(transport.RouteShardRPC, svc2.NativeRPCHandler())

	blob := []byte("shadow-meta-over-the-wire")
	require.NoError(t, svc1.WriteShadowMeta(ctx, tr2.LocalAddr(), "bkt", "key", blob))

	// The remote node must have durably written the shadow meta.
	got, err := os.ReadFile(filepath.Join(dir2, "shards", ".shadow_meta", "bkt", "key"))
	require.NoError(t, err)
	require.Equal(t, blob, got)
}

func TestQuorumMetaShadow_DisabledIsInert(t *testing.T) {
	prev := quorumMetaShadowEnabled
	quorumMetaShadowEnabled = false
	defer func() { quorumMetaShadowEnabled = prev }()

	ft := &fakeShadowTransport{}
	s := newShadowTestShardService(t, ft)
	b := &DistributedBackend{selfAddr: "node-a", shardSvc: s}

	b.quorumMetaShadow(context.Background(), PutObjectMetaCmd{
		Bucket:  "bucket",
		Key:     "k",
		ECData:  4,
		NodeIDs: []string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-f"},
	})

	require.Empty(t, ft.calledAddrs(), "disabled shadow must not dispatch")
	_, err := os.Stat(filepath.Join(s.dataDirs[0], ".shadow_meta"))
	require.True(t, os.IsNotExist(err), "disabled shadow must not write")
}

func TestQuorumMetaShadow_SelfLocalRemoteRemoteAndTraces(t *testing.T) {
	prev := quorumMetaShadowEnabled
	quorumMetaShadowEnabled = true
	defer func() { quorumMetaShadowEnabled = prev }()

	tracePath := filepath.Join(t.TempDir(), "trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", tracePath)
	reloadPutTraceSinkForTest()
	defer func() {
		os.Unsetenv("GRAINFS_PUT_TRACE_FILE")
		reloadPutTraceSinkForTest()
	}()

	ft := &fakeShadowTransport{}
	s := newShadowTestShardService(t, ft)
	b := &DistributedBackend{selfAddr: "node-a", shardSvc: s}

	// K = N here so the fan-out waits for every node (incl. the slower self
	// local fsync write) before returning — makes the self-local file check
	// deterministic. Quorum early-return (K < N) is covered by
	// TestFanOutQuorumMetaShadow_ReturnsAtKAcks.
	ctx := ContextWithPutTrace(context.Background(), PutTraceRequest{Bucket: "bucket", Key: "k", Ingress: PutTraceIngressLocalLeader})
	b.quorumMetaShadow(ctx, PutObjectMetaCmd{
		Bucket:  "bucket",
		Key:     "k",
		ECData:  6,
		NodeIDs: []string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-f"},
	})

	// self (node-a) must be a LOCAL write — never dispatched over the transport.
	for _, addr := range ft.calledAddrs() {
		require.NotEqual(t, "node-a", addr, "self node must be written locally, not over loopback")
	}
	// self local file present.
	_, err := os.Stat(filepath.Join(s.dataDirs[0], ".shadow_meta", "bucket", "k"))
	require.NoError(t, err)

	// trace event emitted for the quorum_meta_write stage.
	data, err := os.ReadFile(tracePath)
	require.NoError(t, err)
	found := false
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var ev PutTraceEvent
		require.NoError(t, json.Unmarshal([]byte(line), &ev))
		if ev.Stage == PutTraceStageQuorumMetaWrite {
			found = true
			assert.Equal(t, 6, ev.MetaProposeCount)
			assert.Empty(t, ev.Error)
		}
	}
	require.True(t, found, "expected a quorum_meta_write trace event")
}
