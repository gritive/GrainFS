package putpipeline

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// recordingWAL captures the shard indices handed to AppendBatch so a test can
// assert which shards this node records for local recovery.
type recordingWAL struct {
	mu      sync.Mutex
	indices []int
}

func (w *recordingWAL) AppendBatch(_ context.Context, records []ShardWALRecord) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range records {
		w.indices = append(w.indices, r.ShardIdx)
	}
	return true, nil
}

func (w *recordingWAL) recorded() []int {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := append([]int(nil), w.indices...)
	sort.Ints(out)
	return out
}

// TestPipeline_MixedPlacementRoundTrip is the S2-sender-b-2 acceptance test:
// a PUT whose shards are split across nodes (some local, some streamed to a
// peer's verbatim WriteSealedShard RPC) must reconstruct byte-identically
// through the production GET path (cluster.ECReconstruct over the per-shard
// reads). This is deliberately a round-trip GET, not a "shard files landed"
// check: a remote shard sealed or stored under a key the reader can't validate
// would still land a file, but ReadLocalShard's AEAD decrypt (AAD = bucket,
// shardKey, idx) would fail and reconstruction would not recover the object.
// It is the only test that catches seal-identity != envelope-identity drift.
func TestPipeline_MixedPlacementRoundTrip(t *testing.T) {
	ctx := context.Background()
	keeper := testDEKKeeper(t)
	clusterID := testClusterID()

	tr1 := transport.MustNewTCPTransport("test-cluster-psk")
	tr2 := transport.MustNewTCPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()
	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	// node1 owns shards 0,1 locally; node2 receives shards 2,3 verbatim.
	// The pipeline keys its registry by PutID, so each local shard needs its
	// own drive (one root per shard, matching production's DataDirs>=numShards).
	ss1 := cluster.NewMultiRootShardService(
		[]string{t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()}, tr1,
		cluster.WithShardDEKKeeper(keeper, clusterID),
		cluster.WithDataWAL(fakeShardWAL{}))
	ss2 := newSinkTestShardService(t, tr2, keeper, clusterID)
	tr2.HandleBody(transport.StreamShardWriteBody, ss2.HandleWriteBody())

	p := New(Config{
		DataDirs:    ss1.DataDirs(), // local shards land where ss1 reads them
		DEKKeeper:   keeper,
		ClusterID:   clusterID,
		ECConfig:    cluster.ECConfig{DataShards: 2, ParityShards: 2},
		StripeBytes: 1 << 20,
		Transport:   tr1,
	})
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, p.Shutdown(sctx))
	}()

	// Single-stripe body (< StripeBytes). cluster.ECReconstruct expects the
	// spool path's whole-buffer shard layout; the pipeline instead appends a
	// per-stripe fragment to each shard, so ECReconstruct only matches when
	// there is a single stripe. (Production GET reads multi-stripe pipeline
	// objects through a stripe-aware reader, not this function — R7 warp GETs
	// 10 MiB all-local objects with content intact.) The real multi-MiB
	// PUT->GET is S3's named acceptance criterion (through the backend reader);
	// here a single stripe still spans many encrypted chunks, exercising the
	// remote sink's chunk streaming + backpressure.
	body := make([]byte, 800*1024)
	for i := range body {
		body[i] = byte((i * 7) % 251)
	}
	wantSum := md5.Sum(body)
	wantETag := hex.EncodeToString(wantSum[:])

	bucket := "external"
	shardKey := "obj-mixed/v1"
	size := int64(len(body))
	obj, err := p.Put(ctx, PutRequest{
		Bucket:    bucket,
		Key:       shardKey,
		Body:      bytes.NewReader(body),
		SizeHint:  &size,
		Placement: []string{"", "", tr2.LocalAddr(), tr2.LocalAddr()},
	})
	require.NoError(t, err)
	require.NotNil(t, obj)
	require.Equal(t, size, obj.Size)
	require.Equal(t, wantETag, obj.ETag)

	// Round-trip GET: gather every shard from its owning node through the same
	// ReadLocalShard the production GET / repair path uses (decrypts with
	// AAD = bucket,shardKey,idx). A remote shard sealed or stored under a key
	// the reader can't validate would AEAD-fail here.
	recCfg := cluster.ECConfig{DataShards: 2, ParityShards: 2}
	shards := make([][]byte, recCfg.NumShards())
	for i := 0; i < 2; i++ {
		s, rerr := ss1.ReadLocalShard(bucket, shardKey, i)
		require.NoError(t, rerr, "local shard %d must read back from node1", i)
		shards[i] = s
	}
	for i := 2; i < 4; i++ {
		s, rerr := ss2.ReadLocalShard(bucket, shardKey, i)
		require.NoError(t, rerr, "remote shard %d must read back from node2 (catches seal/envelope drift)", i)
		shards[i] = s
	}

	got, err := cluster.ECReconstruct(recCfg, shards)
	require.NoError(t, err)
	require.Equal(t, body, got, "object must reconstruct byte-identically from mixed-placement shards")

	// Force the REMOTE parity shards into the EC decode: drop a local data
	// shard and reconstruct from {data1, parity2, parity3}. This proves the
	// remote shards are not merely decryptable but byte-correct EC parity —
	// a drifted remote shard would yield a wrong object, not just a read error.
	recovered, err := cluster.ECReconstruct(recCfg, [][]byte{nil, shards[1], shards[2], shards[3]})
	require.NoError(t, err)
	require.Equal(t, body, recovered, "object must recover from data1 + remote parity (remote shards are valid EC parity)")
}

// TestPipeline_MixedPlacementWALSkipsRemote proves this node's data WAL records
// only the LOCAL shards of a mixed-placement PUT. A remote shard is durable in
// the peer's own WAL (its WriteSealedShard receiver appends it there); recording
// it here would point local recovery at a file that lives on another node.
func TestPipeline_MixedPlacementWALSkipsRemote(t *testing.T) {
	ctx := context.Background()
	keeper := testDEKKeeper(t)
	clusterID := testClusterID()

	tr1 := transport.MustNewTCPTransport("test-cluster-psk")
	tr2 := transport.MustNewTCPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()
	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	ss2 := newSinkTestShardService(t, tr2, keeper, clusterID)
	tr2.HandleBody(transport.StreamShardWriteBody, ss2.HandleWriteBody())

	wal := &recordingWAL{}
	p := New(Config{
		DataDirs:    []string{t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()},
		DEKKeeper:   keeper,
		ClusterID:   clusterID,
		ECConfig:    cluster.ECConfig{DataShards: 2, ParityShards: 2},
		StripeBytes: 1 << 20,
		Transport:   tr1,
		WAL:         wal,
	})
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, p.Shutdown(sctx))
	}()

	body := make([]byte, 400*1024)
	for i := range body {
		body[i] = byte(i % 251)
	}
	size := int64(len(body))
	_, err := p.Put(ctx, PutRequest{
		Bucket:    "external",
		Key:       "obj-wal/v1",
		Body:      bytes.NewReader(body),
		SizeHint:  &size,
		Placement: []string{"", "", tr2.LocalAddr(), tr2.LocalAddr()},
	})
	require.NoError(t, err)

	require.Equal(t, []int{0, 1}, wal.recorded(),
		"only local shards 0,1 may be WAL-recorded here; remote shards 2,3 live in the peer's WAL")
}

// TestPipeline_MixedPlacementDenseLocalDrives proves mixed placement does NOT
// require one drive per shard: with fewer drives than shards (a real multi-node
// coordinator stores only a subset), shards 0,2 both map to drive 0 and 1,3 to
// drive 1. The PutID-keyed DriveActor registry would alias these on a shared
// long-lived drive, but the mixed path gives each shard its own ephemeral pump,
// so the PUT completes and every shard reconstructs.
func TestPipeline_MixedPlacementDenseLocalDrives(t *testing.T) {
	ctx := context.Background()
	keeper := testDEKKeeper(t)
	clusterID := testClusterID()

	tr := transport.MustNewTCPTransport("test-cluster-psk")
	require.NoError(t, tr.Listen(ctx, "127.0.0.1:0"))
	defer tr.Close()
	ss := cluster.NewMultiRootShardService(
		[]string{t.TempDir(), t.TempDir()}, tr, // 2 drives, 4 shards
		cluster.WithShardDEKKeeper(keeper, clusterID),
		cluster.WithDataWAL(fakeShardWAL{}))

	p := New(Config{
		DataDirs:    ss.DataDirs(),
		DEKKeeper:   keeper,
		ClusterID:   clusterID,
		ECConfig:    cluster.ECConfig{DataShards: 2, ParityShards: 2},
		StripeBytes: 1 << 20,
	})
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, p.Shutdown(sctx))
	}()

	body := make([]byte, 64*1024)
	for i := range body {
		body[i] = byte((i * 5) % 251)
	}
	size := int64(len(body))
	bucket, shardKey := "external", "obj-dense/v1"
	// Placement non-nil + all-local: forces every shard through an ephemeral
	// local pump; shards 0,2 share drive dir 0, shards 1,3 share dir 1.
	_, err := p.Put(ctx, PutRequest{
		Bucket:    bucket,
		Key:       shardKey,
		Body:      bytes.NewReader(body),
		SizeHint:  &size,
		Placement: []string{"", "", "", ""},
	})
	require.NoError(t, err, "dense local placement (shards aliasing a drive dir) must complete via ephemeral pumps")

	shards := make([][]byte, 4)
	for i := 0; i < 4; i++ {
		s, rerr := ss.ReadLocalShard(bucket, shardKey, i)
		require.NoError(t, rerr, "shard %d must read back", i)
		shards[i] = s
	}
	got, err := cluster.ECReconstruct(cluster.ECConfig{DataShards: 2, ParityShards: 2}, shards)
	require.NoError(t, err)
	require.Equal(t, body, got)
}
