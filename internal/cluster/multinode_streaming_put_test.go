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
	// nodeTransports holds every node's transport (coord first, then peers) so
	// tests can read the per-node native-dispatch counter
	// (InboundNativeShardWrites — a concrete *transport.HTTPTransport accessor,
	// deliberately NOT part of ClusterTransport).
	nodeTransports []transport.ClusterTransport
	// callWithBodyFails, when an address is present (value true), makes the
	// coord pipeline's remote sealed-shard write RPC to that peer fail. Read by
	// the fault-injecting transport wrapping pipeline.Config.Transport.
	failWrite map[string]bool
}

// failingShardTransport wraps the coord's transport for the put pipeline
// only: ShardWrite (the native sealed-shard write route) errors for any
// peer address in failWrite, and delegates everything else. The coord
// ShardService keeps the raw transport, so GET reads are never disturbed by the
// injected write failure — which is exactly what the parity-best-effort test
// needs (the parity write fails, the surviving data shards still serve GET).
type failingShardTransport struct {
	inner     transport.ClusterTransport
	failWrite map[string]bool
}

func (f *failingShardTransport) ShardWrite(ctx context.Context, addr string, req transport.ShardWriteRequest, body io.Reader) error {
	if f.failWrite[addr] {
		// Drain the body so the pipeline's pipe writer unblocks, then reject,
		// mirroring a peer that accepts the stream but rejects the shard.
		_, _ = io.Copy(io.Discard, body)
		return context.Canceled
	}
	return f.inner.ShardWrite(ctx, addr, req, body)
}

// mkTransport builds a cluster transport for one node from the shared cluster
// PSK. The Phase 8 HTTP transport is the only cluster transport; the factory
// indirection is retained so the harness stays transport-agnostic.
type mkTransport func(psk string) transport.ClusterTransport

func httpTransport(psk string) transport.ClusterTransport { return transport.MustNewHTTPTransport(psk) }

// newInProcessCluster stands up `nodes` distinct nodes over the loopback HTTP
// cluster transport.
func newInProcessCluster(t *testing.T, nodes int, ec cluster.ECConfig) *inProcessCluster {
	return newInProcessClusterT(t, nodes, ec, httpTransport)
}

// newInProcessClusterT stands up `nodes` distinct nodes (coord + nodes-1 peers)
// wired for streaming-EC PUT and GET at ec, each on a transport built by mk. The
// shared keeper + clusterID threads through the coord pipeline, the coord
// ShardService, and every peer ShardService so PUT-seal and GET-open agree on one
// DEK.
func newInProcessClusterT(t *testing.T, nodes int, ec cluster.ECConfig, mk mkTransport) *inProcessCluster {
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
	coordTr := mk("test-cluster-psk")
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
	cl.nodeTransports = append(cl.nodeTransports, coordTr)
	for i := 0; i < nodes-1; i++ {
		peerTr := mk("test-cluster-psk")
		require.NoError(t, peerTr.Listen(context.Background(), "127.0.0.1:0"))
		t.Cleanup(func() { _ = peerTr.Close() })

		peerDir := t.TempDir()
		peerDWAL, err := datawal.Open(filepath.Join(peerDir, "datawal"),
			storage.NewDEKKeeperAdapter(keeper, clusterID), datawal.NamespaceShard)
		require.NoError(t, err)
		t.Cleanup(func() { _ = peerDWAL.Close() })

		peerSvc := cluster.NewShardService(peerDir, peerTr,
			cluster.WithShardDEKKeeper(keeper, clusterID), cluster.WithDataWAL(peerDWAL))
		peerTr.RegisterBufferedRoute(transport.RouteShardRPC, peerSvc.NativeRPCHandler())
		peerTr.RegisterBufferedRoute(transport.RouteShardRPC, peerSvc.NativeRPCHandler())
		peerTr.RegisterShardWriteHandler(peerSvc.NativeWriteHandler()) // native /shard/write route (Phase 8 N6)
		peerTr.RegisterShardReadHandler(peerSvc.NativeReadHandler())   // native /shard/read route (Phase 8 N7-1)
		peerTr.RegisterShardWriteHandler(peerSvc.NativeWriteHandler())
		peerTr.RegisterShardReadHandler(peerSvc.NativeReadHandler())
		t.Cleanup(func() { _ = peerSvc.Close() })

		cl.peerAddrs = append(cl.peerAddrs, peerTr.LocalAddr())
		cl.nodeTransports = append(cl.nodeTransports, peerTr)
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

// requireNativeShardWriteDispatch sums InboundNativeShardWrites across every
// node transport and asserts the PUT's remote shard writes traveled the native
// /shard/write route, not the tunnel (Phase 8 N6 positive dispatch proof —
// S5b lesson: prove dispatch, don't infer it from a green round-trip).
func requireNativeShardWriteDispatch(t *testing.T, cl *inProcessCluster) {
	t.Helper()
	var nativeWrites uint64
	for _, tr := range cl.nodeTransports {
		ht, ok := tr.(*transport.HTTPTransport)
		require.True(t, ok, "node transport must be the concrete *transport.HTTPTransport to expose the native counter")
		nativeWrites += ht.InboundNativeShardWrites()
	}
	require.Positive(t, nativeWrites, "multi-node PUT must dispatch shard writes through the native route")
}

// requireNativeShardReadDispatch sums InboundNativeShardReads across every node
// transport and asserts the GET's remote shard reads traveled the native
// /shard/read route, not the tunnel (Phase 8 N7-1 positive dispatch proof).
// Only meaningful after a GET of a >4MiB object: smaller objects take the
// BUFFERED ReadShard path (bufferedShardReaders, ec_object_reader.go) — a
// different family that never touches the native route — so the counter would
// legitimately read 0 for them.
func requireNativeShardReadDispatch(t *testing.T, cl *inProcessCluster) {
	t.Helper()
	var nativeReads uint64
	for _, tr := range cl.nodeTransports {
		ht, ok := tr.(*transport.HTTPTransport)
		require.True(t, ok, "node transport must be the concrete *transport.HTTPTransport to expose the native counter")
		nativeReads += ht.InboundNativeShardReads()
	}
	require.Positive(t, nativeReads, "multi-node GET of a >4MiB object must stream shard reads through the native route")
}

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
	requireNativeShardWriteDispatch(t, cl)
}

// TestMultiNodeStreamingPUT_HTTP_K3_RoundTrip is the S8-5 Phase A data-plane
// proof: the SAME multi-node streaming-EC PUT/GET round-trip, but every node runs
// the Phase 8 HTTPTransport instead of TCP. A 3+2 object's five shards spread
// across five distinct nodes (requireSpansNodes guards against an all-local
// collapse), so the PUT streams ≥K-1 sealed shards to remote peers over HTTP
// CallWithBody and the GET reconstructs by fetching remote shards over HTTP
// CallRead/ReadShard. This is the first REAL data-plane-over-HTTP exercise (S8-2
// was method-isolated, S8-3 was raft) — the path the production flip activates.
// macOS functional-only (no throughput bench; eyes-open, like the QUIC→TCP flip).
func TestMultiNodeStreamingPUT_HTTP_K3_RoundTrip(t *testing.T) {
	ec := cluster.ECConfig{DataShards: 3, ParityShards: 2}
	cl := newInProcessClusterT(t, 5, ec, httpTransport)
	coord := cl.coord
	coord.SetPutPipelineMultiNode(true)
	require.NoError(t, coord.CreateBucket(ctxBg(), "bucket"))

	nodeIDs, cfg, err := coord.PlanPlacementForTest(ctxBg(), "http.bin")
	require.NoError(t, err)
	require.Equal(t, ec, cfg, "EC config must not be clamped on a 5-node cluster")
	requireSpansNodes(t, cl, nodeIDs)

	payload := randBytes(t, 3<<20+123) // multi-stripe, not a clean multiple
	obj, err := coord.PutObjectWithRequest(ctxBg(), putReq("bucket", "http.bin", bytes.NewReader(payload), int64(len(payload))))
	require.NoError(t, err, "multi-node streaming PUT over HTTP must succeed")
	require.Greater(t, obj.StripeBytes, uint32(0), "streaming PUT must stamp StripeBytes (the de-interleave key)")

	got := getAll(t, coord, "bucket", "http.bin")
	require.True(t, bytes.Equal(payload, got), "GET must reconstruct the streamed multi-node object over HTTP")
	requireNativeShardWriteDispatch(t, cl)

	// Second object, >4MiB: openShardReaders buffers objects <=
	// maxECPooledReadObjectSize (4<<20) through the BUFFERED ReadShard tunnel op,
	// so the 3MiB object above cannot exercise the native read route. A 5MiB+77
	// payload forces the STREAMING reconstruction branch (ReadShardStream →
	// native GET /shard/read) — the byte-identical GET plus the positive counter
	// below is the behavior-neutral proof for the streaming read path under real
	// EC reconstruction (Phase 8 N7-1; RED if the consumer switch is reverted).
	bigPayload := randBytes(t, 5<<20+77)
	bigObj, err := coord.PutObjectWithRequest(ctxBg(), putReq("bucket", "http-big.bin", bytes.NewReader(bigPayload), int64(len(bigPayload))))
	require.NoError(t, err, "multi-node streaming PUT of a >4MiB object over HTTP must succeed")
	require.Greater(t, bigObj.StripeBytes, uint32(0), "streaming PUT must stamp StripeBytes (the de-interleave key)")
	bigGot := getAll(t, coord, "bucket", "http-big.bin")
	require.True(t, bytes.Equal(bigPayload, bigGot), "GET must reconstruct the >4MiB streamed multi-node object over HTTP")
	requireNativeShardReadDispatch(t, cl)
}

// TestMultiNodeStreamingPUT_HTTP_ParityShardFailure_CommitsAndReads runs the
// parity-best-effort path over HTTP: a failed REMOTE parity-shard write (injected
// via the failingShardTransport wrapping the HTTP CallWithBody) must NOT fail the
// PUT, and GET must still reconstruct from the K data shards over HTTP — proving
// the fault/commit semantics hold on the HTTP data plane, not just TCP.
func TestMultiNodeStreamingPUT_HTTP_ParityShardFailure_CommitsAndReads(t *testing.T) {
	ec := cluster.ECConfig{DataShards: 3, ParityShards: 2}
	cl := newInProcessClusterT(t, 5, ec, httpTransport)
	coord := cl.coord
	coord.SetPutPipelineMultiNode(true)
	require.NoError(t, coord.CreateBucket(ctxBg(), "bucket"))

	nodeIDs, _, err := coord.PlanPlacementForTest(ctxBg(), "httpparity.bin")
	require.NoError(t, err)
	idx, addr, ok := cl.remoteShardIdx(nodeIDs, ec, true /*parity*/)
	require.True(t, ok, "need a remote PARITY shard to inject into; placement=%v coord=%s", nodeIDs, cl.coordAddr)
	t.Logf("failing remote parity shard idx=%d on %s (placement=%v)", idx, addr, nodeIDs)
	cl.failSealedShardWritesOn(addr)

	payload := randBytes(t, 3<<20+123)
	_, err = coord.PutObjectWithRequest(ctxBg(), putReq("bucket", "httpparity.bin", bytes.NewReader(payload), int64(len(payload))))
	require.NoError(t, err, "parity-shard failure must NOT fail the PUT (parity best-effort) over HTTP")
	require.True(t, bytes.Equal(payload, getAll(t, coord, "bucket", "httpparity.bin")),
		"GET de-interleaves from the K data shards alone over HTTP")
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
