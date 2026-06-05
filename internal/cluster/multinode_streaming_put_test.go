package cluster_test

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/cluster/putpipeline"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/datawal"
	"github.com/gritive/GrainFS/internal/transport"

	"github.com/stretchr/testify/require"
)

// Task 5 acceptance harness: one full coord DistributedBackend (raft + meta +
// pipeline) plus N standalone peer ShardServices, all reachable over real
// loopback TCP transports. The coord is allNodes[0], so PlaceShards spreads a
// 3+2 object's five shards across the five distinct node addresses: one shard
// lands on the coord (read/written locally) and the other four stream to peers
// via the verbatim WriteSealedShard RPC (PUT) and ReadShard*/HandleRPC (GET).
//
// This exercises the dormant multi-node streaming-EC PUT path activated by
// Tasks 1-4 (per-shard RPC deadline, StripeBytes stamp on the multi-node meta,
// K>=2 guard lift, env opt-in — enabled here programmatically via
// SetPutPipelineMultiNode). It is a Go test (not Ginkgo) because the
// per-test fault injection and runtime placement discovery read cleaner as
// table-free top-level funcs; the existing all-local pipeline_integration_test.go
// covers the Ginkgo round-trip suite.

// inProcessCluster is a coord backend + N peer Shardservices over loopback TCP.
type inProcessCluster struct {
	t         *testing.T
	coord     *cluster.DistributedBackend
	coordAddr string
	// peerAddr[i] is the loopback address of peer i (i in [0, nPeers)); peer
	// transports are indexed by address, not shard index.
	peerAddrs []string
	// callWithBodyFails, when an address is present (value true), makes the
	// coord pipeline's remote sealed-shard write RPC to that peer fail. Read by
	// the fault-injecting transport wrapping pipeline.Config.Transport.
	failWrite map[string]bool
}

// failingShardTransport wraps the coord's TCP transport for the put pipeline
// only: CallWithBody (the verbatim WriteSealedShard body RPC) errors for any
// peer address in failWrite, and delegates everything else. The coord
// ShardService keeps the raw transport, so GET reads are never disturbed by the
// injected write failure — which is exactly what the parity-best-effort test
// needs (the parity write fails, the surviving data shards still serve GET).
type failingShardTransport struct {
	inner     *transport.TCPTransport
	failWrite map[string]bool
}

func (f *failingShardTransport) CallWithBody(ctx context.Context, addr string, req *transport.Message, body io.Reader) (*transport.Message, error) {
	if f.failWrite[addr] {
		// Drain the body so the pipeline's pipe writer unblocks, then reject,
		// mirroring a peer that accepts the stream but rejects the shard.
		_, _ = io.Copy(io.Discard, body)
		return nil, context.Canceled
	}
	return f.inner.CallWithBody(ctx, addr, req, body)
}

// newInProcessCluster stands up `nodes` distinct nodes (coord + nodes-1 peers)
// wired for streaming-EC PUT and GET at ec. The shared keeper + clusterID
// threads through the coord pipeline, the coord ShardService, and every peer
// ShardService so PUT-seal and GET-open agree on one DEK.
func newInProcessCluster(t *testing.T, nodes int, ec cluster.ECConfig) *inProcessCluster {
	t.Helper()
	require.GreaterOrEqual(t, nodes, ec.NumShards(), "need one node per shard for a clean multi-node placement")

	clusterID := bytes.Repeat([]byte{0x42}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x91}, encrypt.KEKSize), clusterID)
	require.NoError(t, err)

	coord := cluster.NewTestDistributedBackend(t)
	coord.SetECConfig(ec)

	// Coord transport + ShardService. The coord pipeline writes coord-local
	// shards to DataDirs (multi-root, one per shard like the all-local helper),
	// so the coord-local shard read aligns with where the pipeline wrote it.
	coordTr := transport.MustNewTCPTransport("test-cluster-psk")
	require.NoError(t, coordTr.Listen(context.Background(), "127.0.0.1:0"))
	t.Cleanup(func() { _ = coordTr.Close() })

	coordDWAL, err := datawal.Open(filepath.Join(coord.Root(), "datawal"),
		storage.NewDEKKeeperAdapter(keeper, clusterID), datawal.NamespaceShard)
	require.NoError(t, err)
	t.Cleanup(func() { _ = coordDWAL.Close() })

	coordDataRoots := make([]string, ec.NumShards())
	for i := range coordDataRoots {
		coordDataRoots[i] = t.TempDir()
	}
	coordSvc := cluster.NewMultiRootShardService(coordDataRoots, coordTr,
		cluster.WithShardDEKKeeper(keeper, clusterID), cluster.WithDataWAL(coordDWAL))

	// Peer ShardServices: single-root each, indexed by real shard idx, on their
	// own loopback transport. Each registers ALL handlers — HandleWriteBody for
	// PUT receive AND HandleRPC + HandleReadBody for GET serve (ReadShard /
	// ReadShardStream). Registering only the write handler passes PUT and fails
	// GET, so all three are required.
	cl := &inProcessCluster{t: t, coordAddr: coordTr.LocalAddr(), failWrite: map[string]bool{}}
	for i := 0; i < nodes-1; i++ {
		peerTr := transport.MustNewTCPTransport("test-cluster-psk")
		require.NoError(t, peerTr.Listen(context.Background(), "127.0.0.1:0"))
		t.Cleanup(func() { _ = peerTr.Close() })

		peerDir := t.TempDir()
		peerDWAL, err := datawal.Open(filepath.Join(peerDir, "datawal"),
			storage.NewDEKKeeperAdapter(keeper, clusterID), datawal.NamespaceShard)
		require.NoError(t, err)
		t.Cleanup(func() { _ = peerDWAL.Close() })

		peerSvc := cluster.NewShardService(peerDir, peerTr,
			cluster.WithShardDEKKeeper(keeper, clusterID), cluster.WithDataWAL(peerDWAL))
		peerTr.SetStreamHandler(peerSvc.HandleRPC())
		peerTr.HandleBody(transport.StreamShardWriteBody, peerSvc.HandleWriteBody())
		peerTr.HandleRead(transport.StreamShardReadBody, peerSvc.HandleReadBody())
		t.Cleanup(func() { _ = peerSvc.Close() })

		cl.peerAddrs = append(cl.peerAddrs, peerTr.LocalAddr())
	}

	// allNodes = [coord, peer0, peer1, ...]; coord = allNodes[0] = selfAddr.
	allNodes := append([]string{coordTr.LocalAddr()}, cl.peerAddrs...)
	coord.SetShardService(coordSvc, allNodes)

	// Coord pipeline. Config.Transport is the fault-injecting wrapper (write-path
	// only); DataDirs == coordSvc.DataDirs() so coord-local shards land where the
	// coord reader expects.
	pipeline := putpipeline.New(putpipeline.Config{
		DataDirs:        coordSvc.DataDirs(),
		DEKKeeper:       keeper,
		ClusterID:       clusterID,
		ECConfig:        ec,
		Transport:       &failingShardTransport{inner: coordTr, failWrite: cl.failWrite},
		ShardRPCTimeout: 30 * time.Second,
	})
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pipeline.Shutdown(shutdownCtx)
	})
	coord.SetPutPipeline(pipeline, true)

	cl.coord = coord
	return cl
}

// failSealedShardWritesOn marks a peer address so the coord pipeline's
// WriteSealedShard RPC to it fails. Targeting is by address (resolved at
// runtime from the placement plan), never by shard index — with random
// loopback ports the index->node mapping varies per run, and one shard is
// always coord-local (no RPC), so an index-based failpoint could be vacuous.
func (cl *inProcessCluster) failSealedShardWritesOn(addr string) {
	cl.t.Helper()
	require.NotEqual(cl.t, cl.coordAddr, addr, "fault target must be a REMOTE peer; the coord-local shard takes no write RPC")
	cl.failWrite[addr] = true
}

func putReq(bucket, key string, body io.Reader, size int64) storage.PutObjectRequest {
	return storage.PutObjectRequest{Bucket: bucket, Key: key, Body: body, SizeHint: &size}
}

func getAll(t *testing.T, b *cluster.DistributedBackend, bucket, key string) []byte {
	t.Helper()
	rc, _, err := b.GetObject(context.Background(), bucket, key)
	require.NoError(t, err)
	got, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	require.NoError(t, readErr)
	require.NoError(t, closeErr)
	return got
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	// Deterministic per run is fine; the point is non-trivial, non-repeating
	// content so a contiguous (mis-de-interleaved) read cannot coincidentally
	// match the original.
	r := rand.New(rand.NewSource(0x5eed))
	_, err := r.Read(b)
	require.NoError(t, err)
	return b
}

// remoteDataIdx / remoteParityIdx pick, from the runtime placement, a shard
// index that is (a) the right kind (data: i < DataShards; parity: i >=
// DataShards) and (b) on a REMOTE peer (node != coordAddr). The coord-local
// shard is skipped because the injected fault is on the remote write RPC.
func (cl *inProcessCluster) remoteShardIdx(nodeIDs []string, ec cluster.ECConfig, parity bool) (idx int, addr string, ok bool) {
	for i, node := range nodeIDs {
		isParity := i >= ec.DataShards
		if isParity != parity {
			continue
		}
		if node == cl.coordAddr {
			continue
		}
		return i, node, true
	}
	return 0, "", false
}

func ctxBg() context.Context { return context.Background() }

// requireSpansNodes asserts the resolved placement genuinely spans nodes (at
// least K-1 remote entries on a 5-node 3+2 layout), so a green round-trip
// cannot be a false all-local collapse (task Step 3).
func requireSpansNodes(t *testing.T, cl *inProcessCluster, nodeIDs []string) {
	t.Helper()
	remote := 0
	for _, n := range nodeIDs {
		if n != cl.coordAddr {
			remote++
		}
	}
	require.Greater(t, remote, 0, "placement %v must span nodes (have remote shards), not collapse to all-local", nodeIDs)
}

// TestMultiNodeStreamingPUT_K3_RoundTrip: a 3+2 (K=3) object PUT through the
// multi-node streaming pipeline round-trips through GET.
//
// RED-on-revert proof (Task 5 Step 4 — the load-bearing check): temporarily
// remove `StripeBytes: stripeBytes,` from the multi-node PutObjectMetaCmd
// propose in object_put.go (the multi-node branch, ~line 179) and re-run — this
// test goes RED. The reader (#716) treats a StripeBytes==0 object as contiguous,
// but the pipeline wrote a stripe-INTERLEAVED K=3 layout, so GET reconstructs
// garbage and bytes.Equal fails. Re-applying the stamp returns it to GREEN.
// K>=3 is essential: at K==2 the interleaved fragment length coincidentally
// equals ceil(size/2) so a contiguous read happens to match and the revert
// would NOT fire (the #716 lesson).
func TestMultiNodeStreamingPUT_K3_RoundTrip(t *testing.T) {
	ec := cluster.ECConfig{DataShards: 3, ParityShards: 2}
	cl := newInProcessCluster(t, 5, ec)
	coord := cl.coord
	coord.SetPutPipelineMultiNode(true)
	require.NoError(t, coord.CreateBucket(ctxBg(), "bucket"))

	nodeIDs, cfg, err := coord.PlanPlacementForTest(ctxBg(), "multi.bin")
	require.NoError(t, err)
	require.Equal(t, ec, cfg, "EC config must not be clamped on a 5-node cluster")
	requireSpansNodes(t, cl, nodeIDs)

	payload := randBytes(t, 3<<20+123) // multi-stripe, not a clean multiple
	obj, err := coord.PutObjectWithRequest(ctxBg(), putReq("bucket", "multi.bin", bytes.NewReader(payload), int64(len(payload))))
	require.NoError(t, err)
	require.Greater(t, obj.StripeBytes, uint32(0), "streaming PUT must stamp StripeBytes (the de-interleave key)")

	got := getAll(t, coord, "bucket", "multi.bin")
	require.True(t, bytes.Equal(payload, got), "GET must reconstruct the streamed multi-node object")
}

// TestMultiNodeStreamingPUT_ParityShardFailure_CommitsAndReads: failing a
// REMOTE parity-shard write must NOT fail the PUT (parity best-effort,
// commit.go:132-133), and GET must still reconstruct — de-interleave needs only
// the K data shards. Strongest direct proof of the inherited commit semantics.
func TestMultiNodeStreamingPUT_ParityShardFailure_CommitsAndReads(t *testing.T) {
	ec := cluster.ECConfig{DataShards: 3, ParityShards: 2}
	cl := newInProcessCluster(t, 5, ec)
	coord := cl.coord
	coord.SetPutPipelineMultiNode(true)
	require.NoError(t, coord.CreateBucket(ctxBg(), "bucket"))

	nodeIDs, _, err := coord.PlanPlacementForTest(ctxBg(), "parityfail.bin")
	require.NoError(t, err)
	idx, addr, ok := cl.remoteShardIdx(nodeIDs, ec, true /*parity*/)
	require.True(t, ok, "need a remote PARITY shard to inject into; placement=%v coord=%s", nodeIDs, cl.coordAddr)
	t.Logf("failing remote parity shard idx=%d on %s (placement=%v)", idx, addr, nodeIDs)
	cl.failSealedShardWritesOn(addr)

	payload := randBytes(t, 3<<20+123)
	_, err = coord.PutObjectWithRequest(ctxBg(), putReq("bucket", "parityfail.bin", bytes.NewReader(payload), int64(len(payload))))
	require.NoError(t, err, "parity-shard failure must NOT fail the PUT (parity best-effort)")
	require.True(t, bytes.Equal(payload, getAll(t, coord, "bucket", "parityfail.bin")),
		"GET de-interleaves from the K data shards alone")
}

// TestMultiNodeStreamingPUT_DataShardFailure_NoCommit: failing a REMOTE
// data-shard write drops dataShardsOK below DataShards, so the PUT fails and no
// metadata is committed. Orphan sealed shards on the peers that DID succeed are
// expected (matches all-local pipeline-error behavior) — scrubber reap is a
// Task 6 follow-up, not fixed here.
func TestMultiNodeStreamingPUT_DataShardFailure_NoCommit(t *testing.T) {
	ec := cluster.ECConfig{DataShards: 3, ParityShards: 2}
	cl := newInProcessCluster(t, 5, ec)
	coord := cl.coord
	coord.SetPutPipelineMultiNode(true)
	require.NoError(t, coord.CreateBucket(ctxBg(), "bucket"))

	nodeIDs, _, err := coord.PlanPlacementForTest(ctxBg(), "datafail.bin")
	require.NoError(t, err)
	idx, addr, ok := cl.remoteShardIdx(nodeIDs, ec, false /*data*/)
	require.True(t, ok, "need a remote DATA shard to inject into; placement=%v coord=%s", nodeIDs, cl.coordAddr)
	t.Logf("failing remote data shard idx=%d on %s (placement=%v)", idx, addr, nodeIDs)
	cl.failSealedShardWritesOn(addr)

	_, err = coord.PutObjectWithRequest(ctxBg(), putReq("bucket", "datafail.bin", bytes.NewReader(randBytes(t, 3<<20)), 3<<20))
	require.Error(t, err, "data-shard failure drops dataShardsOK below DataShards -> PUT fails")

	_, gerr := coord.HeadObject(ctxBg(), "bucket", "datafail.bin")
	require.Error(t, gerr, "no metadata committed")
}

// TestMultiNodeStreamingPUT_FlagOff_FallsBackToSpool is the cluster-layer
// reproduction of the #717 wiring bug: on multi-node placement (allLocal=false),
// if putPipelineMultiNode is OFF on the SERVING backend, the multi-node streaming
// branch (object_put.go:137) is skipped and the PUT falls back off the streaming
// path — which does NOT stamp StripeBytes. Paired with the K3 test (flag ON ->
// streaming + StripeBytes>0), this proves the flag on the serving backend is the
// dispatch gate, i.e. the missing boot wiring (#717) silently disabled streaming.
func TestMultiNodeStreamingPUT_FlagOff_FallsBackToSpool(t *testing.T) {
	ec := cluster.ECConfig{DataShards: 3, ParityShards: 2}
	cl := newInProcessCluster(t, 5, ec)
	coord := cl.coord
	// Flag intentionally NOT set (default OFF) — mirrors the production bug
	// where boot wired SetPutPipelineMultiNode on group-0 only.
	require.NoError(t, coord.CreateBucket(ctxBg(), "bucket"))

	nodeIDs, _, err := coord.PlanPlacementForTest(ctxBg(), "spool.bin")
	require.NoError(t, err)
	requireSpansNodes(t, cl, nodeIDs) // genuinely multi-node (allLocal=false)

	payload := randBytes(t, 3<<20+123)
	obj, err := coord.PutObjectWithRequest(ctxBg(), putReq("bucket", "spool.bin", bytes.NewReader(payload), int64(len(payload))))
	require.NoError(t, err)
	require.Equal(t, uint32(0), obj.StripeBytes,
		"flag OFF on multi-node placement must NOT take the streaming path (no StripeBytes stamp = spool fallback)")

	got := getAll(t, coord, "bucket", "spool.bin")
	require.True(t, bytes.Equal(payload, got), "spool fallback must still round-trip")
}
