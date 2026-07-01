package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/metrics"
)

// fakeECObjectShardFetcher records calls and returns configurable data/errors.
type fakeECObjectShardFetcher struct {
	localShards               map[string][]byte // "bucket/key/idx" → raw shard bytes (with header)
	remoteErr                 map[string]error  // node → error to return
	mu                        sync.Mutex
	readShardCalls            int
	readShardStreamCalls      int
	readShardRangeStreamCalls int
}

func (f *fakeECObjectShardFetcher) key(bucket, key string, idx int) string {
	return shardCacheKey(bucket, key, idx)
}

func (f *fakeECObjectShardFetcher) ReadLocalShard(bucket, key string, shardIdx int) ([]byte, error) {
	data, ok := f.localShards[f.key(bucket, key, shardIdx)]
	if !ok {
		return nil, errors.New("local shard not found")
	}
	return append([]byte(nil), data...), nil
}

func (f *fakeECObjectShardFetcher) ReadShard(ctx context.Context, peer, bucket, key string, shardIdx int) ([]byte, error) {
	f.mu.Lock()
	f.readShardCalls++
	f.mu.Unlock()
	return f.readShardData(peer, bucket, key, shardIdx)
}

func (f *fakeECObjectShardFetcher) readShardData(peer, bucket, key string, shardIdx int) ([]byte, error) {
	if err := f.remoteErr[peer]; err != nil {
		return nil, err
	}
	data, ok := f.localShards[f.key(bucket, key, shardIdx)]
	if !ok {
		return nil, errors.New("shard not found")
	}
	return append([]byte(nil), data...), nil
}

func (f *fakeECObjectShardFetcher) OpenLocalShard(bucket, key string, shardIdx int) (io.ReadCloser, error) {
	data, err := f.ReadLocalShard(bucket, key, shardIdx)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeECObjectShardFetcher) ReadShardStream(ctx context.Context, peer, bucket, key string, shardIdx int) (io.ReadCloser, error) {
	f.mu.Lock()
	f.readShardStreamCalls++
	f.mu.Unlock()
	data, err := f.readShardData(peer, bucket, key, shardIdx)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeECObjectShardFetcher) ReadLocalShardAt(bucket, key string, shardIdx int, offset int64, buf []byte) (int, error) {
	data, err := f.ReadLocalShard(bucket, key, shardIdx)
	if err != nil {
		return 0, err
	}
	if offset >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(buf, data[offset:])
	return n, nil
}

func (f *fakeECObjectShardFetcher) ReadShardRangeStream(_ context.Context, peer, bucket, key string, shardIdx int, offset, length int64) (io.ReadCloser, error) {
	f.mu.Lock()
	f.readShardRangeStreamCalls++
	f.mu.Unlock()
	data, err := f.readShardData(peer, bucket, key, shardIdx)
	if err != nil {
		return nil, err
	}
	if offset >= int64(len(data)) {
		return nil, io.EOF
	}
	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return io.NopCloser(bytes.NewReader(data[offset:end])), nil
}

// Write-side methods satisfy the unified ecShardStore interface. The EC reader
// never invokes them; they are unreachable stubs.
func (f *fakeECObjectShardFetcher) WriteLocalShardStreamContext(context.Context, string, string, int, io.Reader) error {
	panic("WriteLocalShardStreamContext: not used by ecObjectReader")
}

func (f *fakeECObjectShardFetcher) DeleteLocalShards(string, string) error {
	panic("DeleteLocalShards: not used by ecObjectReader")
}

func (f *fakeECObjectShardFetcher) WriteShard(context.Context, string, string, string, int, []byte) error {
	panic("WriteShard: not used by ecObjectReader")
}

func (f *fakeECObjectShardFetcher) WriteShardStream(context.Context, string, string, string, int, io.Reader) error {
	panic("WriteShardStream: not used by ecObjectReader")
}

func (f *fakeECObjectShardFetcher) WriteLocalShardStreamStagedContext(context.Context, string, string, string, int, io.Reader) error {
	panic("WriteLocalShardStreamStagedContext: not used by ecObjectReader")
}

func (f *fakeECObjectShardFetcher) WriteShardStreamStaged(context.Context, string, string, string, string, int, io.Reader) error {
	panic("WriteShardStreamStaged: not used by ecObjectReader")
}

func (f *fakeECObjectShardFetcher) DeleteShards(context.Context, string, string, string) error {
	panic("DeleteShards: not used by ecObjectReader")
}

// fakeECObjectPeerHealth records mark calls.
type fakeECObjectPeerHealth struct {
	mu        sync.Mutex
	healthy   []string
	unhealthy []string
}

func (f *fakeECObjectPeerHealth) MarkHealthy(peer string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.healthy = append(f.healthy, peer)
	return true
}

func (f *fakeECObjectPeerHealth) MarkUnhealthy(peer string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unhealthy = append(f.unhealthy, peer)
	return true
}

// fakeECObjectShardCache is a minimal in-memory cache for tests.
type fakeECObjectShardCache struct {
	data     map[string][]byte
	capacity int64
}

func newFakeCache(capacity int64) *fakeECObjectShardCache {
	return &fakeECObjectShardCache{data: make(map[string][]byte), capacity: capacity}
}

func (c *fakeECObjectShardCache) Peek(key string) ([]byte, bool) {
	v, ok := c.data[key]
	return v, ok
}

func (c *fakeECObjectShardCache) Get(key string) ([]byte, bool) {
	v, ok := c.data[key]
	return v, ok
}

func (c *fakeECObjectShardCache) Put(key string, data []byte) {
	c.data[key] = append([]byte(nil), data...)
}

func (c *fakeECObjectShardCache) CanStore(key string, size int64) bool {
	return size <= c.capacity
}

func (c *fakeECObjectShardCache) Stats() shardcache.Stats {
	return shardcache.Stats{CapacityByte: c.capacity}
}

// buildFakeShards encodes data with the given ECConfig and stores each shard
// (with header) in the fetcher's localShards map under bucket/shardKey.
func buildFakeShards(t testing.TB, fetcher *fakeECObjectShardFetcher, bucket, shardKey string, cfg ECConfig, data []byte) {
	t.Helper()
	shards, err := ECSplit(cfg, data)
	require.NoError(t, err)
	if fetcher.localShards == nil {
		fetcher.localShards = make(map[string][]byte)
	}
	for i, s := range shards {
		fetcher.localShards[shardCacheKey(bucket, shardKey, i)] = append([]byte(nil), s...)
	}
}

// readObjectViaOpen reads the whole object through the production streaming
// path (OpenObject), the drop-in replacement for the retired buffered ReadObject.
func readObjectViaOpen(t testing.TB, r ecObjectReader, bucket, shardKey string, rec PlacementRecord, size int64) ([]byte, error) {
	t.Helper()
	rc, err := r.OpenObject(context.Background(), bucket, shardKey, rec, size)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// TestECObjectReader_OpenObject_PrefersDataShardsBeforeParity guards that a
// healthy read opens exactly DataShards (K) shards — never fanning out to parity.
// openShardReaders iterates the primary (data-shard) list first, then checks
// `available >= recCfg.DataShards` at the top of the fallback loop (break-guard),
// which prevents any parity shard from being opened when data shards satisfy K.
// The count assertion is exactly K=4; if the fallback break-guard were removed the
// count would climb to K+M=6, which would fail this test.
func TestECObjectReader_OpenObject_PrefersDataShardsBeforeParity(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := bytes.Repeat([]byte("x"), 64<<10) // 64 KiB
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "key", cfg, data)

	// selfID is not in rec.Nodes → all shards are remote, so openShardReaders
	// calls remoteShardEndpoint.OpenShardStream → fetcher.ReadShardStream for each
	// open (incrementing readShardStreamCalls).
	r := ecObjectReader{
		selfID:   "node-self",
		shards:   fetcher,
		ecConfig: cfg,
		// bl is nil: computeAttemptOrder uses plain data-first order, no BL swap.
	}
	rec := PlacementRecord{
		Nodes: []string{"n0", "n1", "n2", "n3", "p0", "p1"},
	}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	got, err := readObjectViaOpen(t, r, "bucket", "key", rec, int64(len(data)))
	require.NoError(t, err)
	require.Equal(t, data, got)

	// Exactly K=4 stream-opens must occur — no parity shard opened.
	// This enforces the fallback-loop break-guard (available >= K → break before parity).
	require.Equal(t, cfg.DataShards, fetcher.readShardStreamCalls,
		"healthy read must open exactly DataShards=%d shards (got %d: parity fan-out regression)",
		cfg.DataShards, fetcher.readShardStreamCalls)
	require.Zero(t, fetcher.readShardCalls, "healthy read must not use retired buffered ReadShard path")
}

func TestECObjectReader_ReadObject_AllLocal(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	data := []byte("hello world from EC")
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "key/v1", cfg, data)

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	got, err := readObjectViaOpen(t, r, "bucket", "key/v1", rec, int64(len(data)))
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestECObjectReader_ReadObject_FallsBackToParityShard(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	data := []byte("reconstruct from parity")
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "obj", cfg, data)

	// Delete data shard 0 so the reader must use the parity shard.
	delete(fetcher.localShards, shardCacheKey("bucket", "obj", 0))

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	got, err := readObjectViaOpen(t, r, "bucket", "obj", rec, int64(len(data)))
	require.NoError(t, err)
	require.Equal(t, data, got)
}

// TestECObjectReader_OpenObject_MarksUnhealthyPeerOnFetchError guards that the
// streaming open path (OpenObject → openShardReaders → remoteShardEndpoint.OpenShardStream)
// marks a failing remote shard's node as unhealthy. Shard 0 is on node-b (remote,
// returns rpc timeout); shards 1 and 2 (parity) come from node-a. The open fails
// at the RPC boundary, which triggers remoteShardEndpoint.markHealth(false) before
// falling back to parity. Reconstruction via shards 1+2 must still yield the
// original data.
func TestECObjectReader_OpenObject_MarksUnhealthyPeerOnFetchError(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	data := []byte("peer health test")
	fetcher := &fakeECObjectShardFetcher{
		remoteErr: map[string]error{"node-b": errors.New("rpc timeout")},
	}
	buildFakeShards(t, fetcher, "bucket", "key", cfg, data)
	// Redirect shard 0 to failing node-b; shards 1 and 2 (parity) come from node-a.
	health := &fakeECObjectPeerHealth{}
	r := ecObjectReader{
		selfID:     "node-a",
		shards:     fetcher,
		peerHealth: health,
		ecConfig:   cfg,
	}
	rec := PlacementRecord{Nodes: []string{"node-b", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	got, err := readObjectViaOpen(t, r, "bucket", "key", rec, int64(len(data)))
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.Contains(t, health.unhealthy, "node-b")
}

// TestECObjectReader_OpenObject_MarksHealthyPeerOnSuccess guards that the
// streaming open path (OpenObject → openShardReaders → remoteShardEndpoint.OpenShardStream)
// marks a successful remote shard's node as healthy. Shard 0 is on node-b (remote,
// succeeds); shards 1 and 2 come from node-a (local). The open succeeds at the
// RPC boundary, which triggers remoteShardEndpoint.markHealth(true).
func TestECObjectReader_OpenObject_MarksHealthyPeerOnSuccess(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	data := []byte("peer health happy path")
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "key", cfg, data)

	health := &fakeECObjectPeerHealth{}
	r := ecObjectReader{
		selfID:     "node-a",
		shards:     fetcher,
		peerHealth: health,
		ecConfig:   cfg,
	}
	// shard 0 = node-b (remote, succeeds); shards 1 and 2 = node-a (local)
	rec := PlacementRecord{Nodes: []string{"node-b", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	got, err := readObjectViaOpen(t, r, "bucket", "key", rec, int64(len(data)))
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.Contains(t, health.healthy, "node-b")
	require.Empty(t, health.unhealthy)
}

func TestECObjectReader_ReadObject_DeinterleavesStripedObject(t *testing.T) {
	const stripeBytes = 1 << 20
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	payload := make([]byte, 3*1024*1024+5)
	for i := range payload {
		payload[i] = byte((i*7 + 3) % 251)
	}

	// CPUPool stores each shard with an 8-byte full-object-size header (same
	// convention as ECSplit). Prepend it to the stripe-interleaved bodies.
	bodies := buildInterleavedShards(t, cfg, payload, stripeBytes)
	header := ShardHeader(int64(len(payload)))
	fetcher := &fakeECObjectShardFetcher{localShards: make(map[string][]byte)}
	for i, body := range bodies {
		shard := append(append([]byte(nil), header[:]...), body...)
		fetcher.localShards[shardCacheKey("bucket", "key", i)] = shard
	}

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards
	rec.StripeBytes = stripeBytes

	got, err := readObjectViaOpen(t, r, "bucket", "key", rec, int64(len(payload)))
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

// buildStripedShardsWithHeader returns K+M shard byte slices, each = 8-byte
// full-object-size header + stripe-interleaved body, matching the on-disk
// convention used by CPUPool/ECSplit.
func buildStripedShardsWithHeader(t testing.TB, cfg ECConfig, payload []byte, stripeBytes int) [][]byte {
	t.Helper()
	bodies := buildInterleavedShards(t.(*testing.T), cfg, payload, stripeBytes)
	header := ShardHeader(int64(len(payload)))
	out := make([][]byte, len(bodies))
	for i, body := range bodies {
		out[i] = append(append([]byte(nil), header[:]...), body...)
	}
	return out
}

func TestECObjectReader_OpenObject_StreamsStripedObject(t *testing.T) {
	const stripeBytes = 1 << 20
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	payload := make([]byte, 5*1024*1024+321)
	for i := range payload {
		payload[i] = byte((i*7 + 3) % 251)
	}

	shards := buildStripedShardsWithHeader(t, cfg, payload, stripeBytes)
	fetcher := &fakeECObjectShardFetcher{localShards: make(map[string][]byte)}
	for i, shard := range shards {
		fetcher.localShards[shardCacheKey("bucket", "key", i)] = shard
	}

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	// Non-self nodes for data shards 0,1 force the ReadShardStream path so we can
	// assert streaming (OpenLocalShard does not bump readShardStreamCalls).
	rec := PlacementRecord{Nodes: []string{"node-b", "node-c", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards
	rec.StripeBytes = stripeBytes

	rc, err := r.OpenObject(context.Background(), "bucket", "key", rec, int64(len(payload)))
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	// Off-by-8 (skip-0 or double-skip) guard: leading bytes must match exactly.
	require.Equal(t, payload[:16], got[:16])
	require.Zero(t, fetcher.readShardCalls)
	require.Greater(t, fetcher.readShardStreamCalls, 0)
}

func TestECObjectReader_OpenObject_StreamsStripedObjectDegraded(t *testing.T) {
	const stripeBytes = 1 << 20
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	payload := make([]byte, 5*1024*1024+321)
	for i := range payload {
		payload[i] = byte((i*7 + 3) % 251)
	}

	shards := buildStripedShardsWithHeader(t, cfg, payload, stripeBytes)
	fetcher := &fakeECObjectShardFetcher{localShards: make(map[string][]byte)}
	for i, shard := range shards {
		fetcher.localShards[shardCacheKey("bucket", "key", i)] = shard
	}
	// Drop data shard 0: openShardReaders falls back to parity → RS reconstruct
	// per stripe in the de-interleave stream reader.
	delete(fetcher.localShards, shardCacheKey("bucket", "key", 0))

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-b", "node-c", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards
	rec.StripeBytes = stripeBytes

	rc, err := r.OpenObject(context.Background(), "bucket", "key", rec, int64(len(payload)))
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

// TestECObjectReader_OpenObject_StreamsSmallStripedObject guards the striped
// code path: a small redundant EC object with StripeBytes>0 and objectSize <
// 4MiB must be reconstructed correctly on the streaming path. All object sizes
// now use the streaming open loop + skipReader header-strip + stripe
// de-interleave reader; the old size-based fork has been removed.
func TestECObjectReader_OpenObject_StreamsSmallStripedObject(t *testing.T) {
	const stripeBytes = 512 << 10 // 512KiB stripes
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	// 1.5MiB — well under old 4MiB limit; spans 3 stripes per data shard.
	payload := make([]byte, 1536*1024+77)
	for i := range payload {
		payload[i] = byte((i*13 + 5) % 251)
	}

	shards := buildStripedShardsWithHeader(t, cfg, payload, stripeBytes)
	fetcher := &fakeECObjectShardFetcher{localShards: make(map[string][]byte)}
	for i, shard := range shards {
		fetcher.localShards[shardCacheKey("bucket", "key", i)] = shard
	}

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	// Non-self nodes for data shards 0,1 force ReadShardStream (not OpenLocalShard).
	rec := PlacementRecord{Nodes: []string{"node-b", "node-c", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards
	rec.StripeBytes = stripeBytes

	rc, err := r.OpenObject(context.Background(), "bucket", "key", rec, int64(len(payload)))
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, payload, got, "small striped object must reconstruct to original bytes")
	// Off-by-8 guard: first bytes must exactly match (catches double-skip or no-skip header bugs).
	require.Equal(t, payload[:16], got[:16])
	require.Zero(t, fetcher.readShardCalls, "small striped object must NOT use buffered (ReadShard) path")
	require.Greater(t, fetcher.readShardStreamCalls, 0, "small striped object must stream (ReadShardStream)")
}

func TestECObjectReader_ReadObject_ErrorsWhenNotEnoughShards(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	fetcher := &fakeECObjectShardFetcher{} // empty — no shards available

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	_, err := readObjectViaOpen(t, r, "bucket", "key", rec, 0)
	require.Error(t, err)
}

func TestECObjectReader_ReadAt_ReturnsCorrectRange(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 0}
	data := []byte("0123456789abcdef") // 16 bytes
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "key", cfg, data)

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a"}}
	rec.K = cfg.DataShards

	buf := make([]byte, 4)
	n, err := r.ReadAt(context.Background(), "bucket", "key", rec, int64(len(data)), 4, buf)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, []byte("4567"), buf)
}

func TestECObjectReader_ReadAt_StripedRange(t *testing.T) {
	const stripeBytes = 1 << 20
	const off, n = 2_500_000, 4096
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	payload := make([]byte, 4*1024*1024+10)
	for i := range payload {
		payload[i] = byte((i*7 + 3) % 251)
	}

	shards := buildStripedShardsWithHeader(t, cfg, payload, stripeBytes)
	fetcher := &fakeECObjectShardFetcher{localShards: make(map[string][]byte)}
	for i, shard := range shards {
		fetcher.localShards[shardCacheKey("bucket", "key", i)] = shard
	}

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-b", "node-c", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards
	rec.StripeBytes = stripeBytes

	buf := make([]byte, n)
	got, err := r.ReadAt(context.Background(), "bucket", "key", rec, int64(len(payload)), off, buf)
	require.NoError(t, err)
	require.Equal(t, n, got)
	require.Equal(t, payload[off:off+n], buf)
}

func TestECObjectReader_ReadAt_PastEOFReturnsEOF(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 0}
	data := []byte("short")
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "key", cfg, data)

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a"}}
	rec.K = cfg.DataShards

	buf := make([]byte, 4)
	_, err := r.ReadAt(context.Background(), "bucket", "key", rec, int64(len(data)), 100, buf)
	require.ErrorIs(t, err, io.EOF)
}

func TestECObjectReader_OpenObject_AllLocal(t *testing.T) {
	// ParityShards=0: Redundant()==false and no cache → exercises streaming path.
	cfg := ECConfig{DataShards: 2, ParityShards: 0}
	data := []byte("hello world open")
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "key", cfg, data)

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a"}}
	rec.K = cfg.DataShards

	rc, err := r.OpenObject(context.Background(), "bucket", "key", rec, int64(len(data)))
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestECObjectReader_OpenObject_StreamsMultipartSizedObject(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	data := bytes.Repeat([]byte("x"), 5<<20)
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "key", cfg, data)

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-b", "node-c", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	rc, err := r.OpenObject(context.Background(), "bucket", "key", rec, int64(len(data)))
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.Zero(t, fetcher.readShardCalls)
	require.Greater(t, fetcher.readShardStreamCalls, 0)
}

func BenchmarkECObjectReaderOpenObject5MiB(b *testing.B) {
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	data := bytes.Repeat([]byte("x"), 5<<20)
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(b, fetcher, "bucket", "key", cfg, data)

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-b", "node-c", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc, err := r.OpenObject(context.Background(), "bucket", "key", rec, int64(len(data)))
		require.NoError(b, err)
		if _, err := io.Copy(io.Discard, rc); err != nil {
			_ = rc.Close()
			require.NoError(b, err)
		}
		require.NoError(b, rc.Close())
	}
}

func TestECObjectReader_OpenObject_NilShardService_ReturnsError(t *testing.T) {
	r := ecObjectReader{selfID: "node-a", shards: nil, ecConfig: ECConfig{DataShards: 2, ParityShards: 0}}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a"}}
	rec.K = 2

	_, err := r.OpenObject(context.Background(), "bucket", "key", rec, 100)
	require.Error(t, err)
}

func TestECObjectReader_ReadAt_PlacementMismatch(t *testing.T) {
	// rec.Nodes length (2) != NumShards for DataShards=2 ParityShards=1 (3).
	r := ecObjectReader{selfID: "node-a", ecConfig: ECConfig{DataShards: 2, ParityShards: 1}}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a"}}
	rec.K = 2
	rec.M = 1

	buf := make([]byte, 4)
	_, err := r.ReadAt(context.Background(), "bucket", "key", rec, 100, 0, buf)
	require.Error(t, err)
}

func TestECObjectReader_ReadAt_InvalidDataShards(t *testing.T) {
	r := ecObjectReader{selfID: "node-a", ecConfig: ECConfig{DataShards: 0, ParityShards: 0}}
	rec := PlacementRecord{Nodes: []string{}}
	rec.K = 0

	buf := make([]byte, 4)
	_, err := r.ReadAt(context.Background(), "bucket", "key", rec, 100, 0, buf)
	require.Error(t, err)
}

func TestECObjectReader_ReadAt_MultiShardSpan(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 0}
	data := []byte("0123456789abcdef") // 16 bytes, 4 bytes per shard
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "key", cfg, data)

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a", "node-a", "node-a"}}
	rec.K = cfg.DataShards

	// offset=2, len=8 spans shards 0→1→2: "23456789"
	buf := make([]byte, 8)
	n, err := r.ReadAt(context.Background(), "bucket", "key", rec, int64(len(data)), 2, buf)
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.Equal(t, []byte("23456789"), buf)
}

func TestECObjectReader_ReadAt_CachesRemoteRange(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 0}
	data := bytes.Repeat([]byte("x"), 256<<10)
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "key", cfg, data)
	cache := shardcache.New(16 << 20)

	r := ecObjectReader{selfID: "node-a", shards: fetcher, cache: cache, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-b", "node-c"}}
	rec.K = cfg.DataShards

	first := make([]byte, 128<<10)
	n, err := r.ReadAt(context.Background(), "bucket", "key", rec, int64(len(data)), 0, first)
	require.NoError(t, err)
	require.Equal(t, len(first), n)
	require.Equal(t, data[:len(first)], first)
	require.Equal(t, 1, fetcher.readShardRangeStreamCalls)

	second := make([]byte, len(first))
	n, err = r.ReadAt(context.Background(), "bucket", "key", rec, int64(len(data)), 0, second)
	require.NoError(t, err)
	require.Equal(t, len(second), n)
	require.Equal(t, first, second)
	require.Equal(t, 1, fetcher.readShardRangeStreamCalls)
}

// TestECObjectReader_ReadAt_CachesSmallRemoteRange is a regression guard: the
// range-result cache must be populated on first read and hit on second, so no
// extra RPC is issued. Both small and large ranges use ReadShardRangeStream.
func TestECObjectReader_ReadAt_CachesSmallRemoteRange(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 0}
	data := bytes.Repeat([]byte("z"), 256<<10)
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "key", cfg, data)
	cache := shardcache.New(16 << 20)

	r := ecObjectReader{selfID: "node-a", shards: fetcher, cache: cache, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-b", "node-c"}}
	rec.K = cfg.DataShards

	first := make([]byte, 4<<10)
	n, err := r.ReadAt(context.Background(), "bucket", "key", rec, int64(len(data)), 0, first)
	require.NoError(t, err)
	require.Equal(t, len(first), n)
	require.Equal(t, data[:len(first)], first)
	firstStreamCalls := fetcher.readShardRangeStreamCalls

	// Second identical read must be served from the range cache — no new RPC.
	second := make([]byte, len(first))
	n, err = r.ReadAt(context.Background(), "bucket", "key", rec, int64(len(data)), 0, second)
	require.NoError(t, err)
	require.Equal(t, len(second), n)
	require.Equal(t, first, second)
	require.Equal(t, firstStreamCalls, fetcher.readShardRangeStreamCalls, "range cache must prevent duplicate RPC on second call")
}

func TestECObjectReader_ReadAt_PartialFillAtEnd(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 0}
	data := []byte("hello") // 5 bytes
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "key", cfg, data)

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a"}}
	rec.K = cfg.DataShards

	// offset=3, buf=10 → only 2 bytes remain ("lo"), no error
	buf := make([]byte, 10)
	n, err := r.ReadAt(context.Background(), "bucket", "key", rec, int64(len(data)), 3, buf)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []byte("lo"), buf[:n])
}

// fakeHotChecker for unit tests.
type fakeHotChecker struct {
	hot map[string]struct{}
}

func newFakeHot(nodes ...string) *fakeHotChecker {
	m := map[string]struct{}{}
	for _, n := range nodes {
		m[n] = struct{}{}
	}
	return &fakeHotChecker{hot: m}
}

func (f *fakeHotChecker) IsHot(nodeID string) bool {
	_, ok := f.hot[nodeID]
	return ok
}

func containsInt(s []int, x int) bool {
	for _, v := range s {
		if v == x {
			return true
		}
	}
	return false
}

func TestComputeAttemptOrder_SwapInHotDataToParity(t *testing.T) {
	rec := PlacementRecord{Nodes: []string{"n0", "n1", "n2", "n3", "n4", "n5"}}
	rec.K = 4
	rec.M = 2
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	r := ecObjectReader{bl: newFakeHot("n0")}
	primary, fallback := r.computeAttemptOrder(rec, cfg)

	require.NotContains(t, primary, 0, "primary must not contain hot data idx 0")
	require.True(t, containsInt(primary, 4) || containsInt(primary, 5), "primary must contain at least one parity idx")
	require.Contains(t, fallback, 0, "fallback must contain hot data idx 0")
	require.Len(t, primary, 4, "primary len must equal k=4")
}

func TestComputeAttemptOrder_NoBlNoSwap(t *testing.T) {
	rec := PlacementRecord{Nodes: []string{"n0", "n1", "n2", "n3", "n4", "n5"}}
	rec.K = 4
	rec.M = 2
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	r := ecObjectReader{} // bl nil
	primary, fallback := r.computeAttemptOrder(rec, cfg)
	require.Equal(t, []int{0, 1, 2, 3}, primary)
	require.Equal(t, []int{4, 5}, fallback)
}

func TestComputeAttemptOrder_NoColdNoSwap(t *testing.T) {
	// All data shards cool → no swap
	rec := PlacementRecord{Nodes: []string{"n0", "n1", "n2", "n3", "n4", "n5"}}
	rec.K = 4
	rec.M = 2
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	r := ecObjectReader{bl: newFakeHot()} // nothing hot
	primary, fallback := r.computeAttemptOrder(rec, cfg)
	require.Equal(t, []int{0, 1, 2, 3}, primary)
	require.Equal(t, []int{4, 5}, fallback)
}

// Step 1: hot > m bypass branch — safety valve coverage.
func TestComputeAttemptOrder_HotExceedsM_Bypass(t *testing.T) {
	rec := PlacementRecord{Nodes: []string{"n0", "n1", "n2", "n3", "n4", "n5"}}
	rec.K = 4
	rec.M = 2
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	r := ecObjectReader{bl: newFakeHot("n0", "n1", "n2")} // 3 hot > m=2

	beforeBypass := testutil.ToFloat64(metrics.ClusterBLBypassedReads)
	primary, fallback := r.computeAttemptOrder(rec, cfg)
	afterBypass := testutil.ToFloat64(metrics.ClusterBLBypassedReads)

	assert.Equal(t, []int{0, 1, 2, 3}, primary)
	assert.Equal(t, []int{4, 5}, fallback)
	assert.Equal(t, 1.0, afterBypass-beforeBypass, "bypass counter should increment by 1")
}

// spyHotChecker records how many times IsHot was called.
type spyHotChecker struct {
	calls atomic.Int64
}

func (s *spyHotChecker) IsHot(string) bool {
	s.calls.Add(1)
	return false
}

// Step 2: contract test — computeAttemptOrder calls IsHot exactly k times (data shards only).
// computeAttemptOrder only probes data-shard nodes, so it is called exactly k times
// regardless of how many parity shards are present in the placement.
func TestComputeAttemptOrder_CacheBypassSpy(t *testing.T) {
	rec := PlacementRecord{Nodes: []string{"n0", "n1", "n2", "n3", "n4", "n5"}}
	rec.K = 4
	rec.M = 2
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	spy := &spyHotChecker{}
	r := ecObjectReader{bl: spy}
	primary, fallback := r.computeAttemptOrder(rec, cfg)
	assert.Equal(t, []int{0, 1, 2, 3}, primary)
	assert.Equal(t, []int{4, 5}, fallback)
	// IsHot called exactly k=4 times (data shards probed, none hot → early exit).
	assert.Equal(t, int64(4), spy.calls.Load())
}

// Step 4: streaming path uses computeAttemptOrder — hot data shard is not opened
// when a parity shard satisfies k=1.
func TestOpenShardReaders_BLSwap_StreamingPath(t *testing.T) {
	// ParityShards=1 ensures Redundant()=true; all object sizes now stream.
	cfg := ECConfig{DataShards: 1, ParityShards: 1}
	objectSize := int64(4<<20 + 1) // 4MiB+1 — arbitrary size for a meaningful payload

	// Build fake shards: shard 0 (data) and shard 1 (parity).
	fetcher := &fakeECObjectShardFetcher{}
	payload := bytes.Repeat([]byte("z"), int(objectSize))
	buildFakeShards(t, fetcher, "bucket", "key", cfg, payload)

	// n0 is hot → BL should reroute the primary read to parity (shard 1, node n1).
	r := ecObjectReader{
		selfID:   "node-self",
		shards:   fetcher,
		ecConfig: cfg,
		bl:       newFakeHot("n0"),
	}
	// shard 0 → node n0 (hot); shard 1 → node n1 (parity, cool)
	rec := PlacementRecord{Nodes: []string{"n0", "n1"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	recCfg, readers, err := r.openShardReaders(context.Background(), "bucket", "key", rec)
	require.NoError(t, err)
	defer closeECShardReaders(readers)

	assert.Equal(t, cfg, recCfg)
	// primary=[1] (parity swap), fallback=[0] (hot data).
	// k=1 satisfied by shard 1 → shard 0 must not be opened.
	assert.Nil(t, readers[0], "hot data shard should not be opened when parity satisfies k")
	assert.NotNil(t, readers[1], "parity shard should be opened as primary")
}

// TestOpenShardReaders_LargeObject_StreamsEvenWhenCacheFits guards that a
// redundant EC object uses the streaming open path (ReadShardStream) and NOT the
// removed buffered path (ReadShard), even when a shard cache is present.
func TestOpenShardReaders_LargeObject_StreamsEvenWhenCacheFits(t *testing.T) {
	cfg := ECConfig{DataShards: 1, ParityShards: 1}
	objectSize := int64(4<<20 + 1) // 4MiB+1; size no longer affects path choice

	fetcher := &fakeECObjectShardFetcher{}
	payload := bytes.Repeat([]byte("z"), int(objectSize))
	buildFakeShards(t, fetcher, "bucket", "key", cfg, payload)

	r := ecObjectReader{
		selfID:   "node-self", // not in rec.Nodes → all shards are remote
		shards:   fetcher,
		ecConfig: cfg,
		// Cache large enough to store the object: the old gate buffered here.
		cache: newFakeCache(1 << 30),
	}
	rec := PlacementRecord{Nodes: []string{"n0", "n1"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	_, readers, err := r.openShardReaders(context.Background(), "bucket", "key", rec)
	require.NoError(t, err)
	defer closeECShardReaders(readers)

	// Streaming uses ReadShardStream; buffering uses ReadShard. A large object
	// must stream even when the cache could store it.
	require.Zero(t, fetcher.readShardCalls, "large object must NOT use the buffered (ReadShard) path")
	require.Greater(t, fetcher.readShardStreamCalls, 0, "large object must stream (ReadShardStream)")
}

// TestOpenShardReaders_SmallObject_StreamsNotBuffers guards that a small
// redundant-EC object takes the streaming open path (OpenShardStream → ReadShardStream)
// and not the retired buffered path (ReadShard). The streaming path handles all
// object sizes unconditionally; the old size-based fork has been removed.
func TestOpenShardReaders_SmallObject_StreamsNotBuffers(t *testing.T) {
	cfg := ECConfig{DataShards: 1, ParityShards: 1}
	objectSize := int64(4 << 10) // 4KiB — well under old 4MiB limit

	fetcher := &fakeECObjectShardFetcher{}
	payload := bytes.Repeat([]byte("s"), int(objectSize))
	buildFakeShards(t, fetcher, "bucket", "key", cfg, payload)

	r := ecObjectReader{
		selfID:   "node-self", // not in rec.Nodes → all shards are remote
		shards:   fetcher,
		ecConfig: cfg,
	}
	rec := PlacementRecord{Nodes: []string{"n0", "n1"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	_, readers, err := r.openShardReaders(context.Background(), "bucket", "key", rec)
	require.NoError(t, err)
	defer closeECShardReaders(readers)

	// The streaming path (ReadShardStream) handles all object sizes unconditionally.
	require.Zero(t, fetcher.readShardCalls, "small redundant object must NOT use the retired buffered (ReadShard) path")
	require.Greater(t, fetcher.readShardStreamCalls, 0, "small redundant object must stream (ReadShardStream)")
}

// TestOpenShardReaders_SmallDegradedObject_StreamsAndRecovers guards that a
// small redundant EC object with one missing shard is reconstructed correctly
// on the streaming path (which handles degraded reads via RS reconstruction).
// The streaming path (ReadShardStream) handles all object sizes; small objects no
// longer bifurcate to the retired buffered path (readShardCalls > 0 would regress).
// The test reads through OpenObject to verify parity reconstruction yields the
// original bytes, not just that streaming dispatch occurred.
func TestOpenShardReaders_SmallDegradedObject_StreamsAndRecovers(t *testing.T) {
	cfg := ECConfig{DataShards: 1, ParityShards: 1}
	objectSize := int64(4 << 10) // 4KiB — under old 4MiB limit

	fetcher := &fakeECObjectShardFetcher{
		remoteErr: map[string]error{
			"n0": errors.New("shard not available"), // data shard missing
		},
	}
	payload := bytes.Repeat([]byte("d"), int(objectSize))
	buildFakeShards(t, fetcher, "bucket", "key", cfg, payload)

	r := ecObjectReader{
		selfID:   "node-self",
		shards:   fetcher,
		ecConfig: cfg,
	}
	rec := PlacementRecord{Nodes: []string{"n0", "n1"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	// Read through OpenObject (not just openShardReaders) to verify that parity
	// reconstruction actually yields the original bytes, not just that dispatch happened.
	rc, err := r.OpenObject(context.Background(), "bucket", "key", rec, objectSize)
	require.NoError(t, err, "degraded read must succeed when parity is available")
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, payload, got, "degraded small object must reconstruct to original bytes via parity")
	require.Zero(t, fetcher.readShardCalls, "degraded small object must NOT use the buffered path")
	require.Greater(t, fetcher.readShardStreamCalls, 0, "degraded small object must use ReadShardStream")
}

// TestOpenShardReaders_ZeroSizeObject_Streams guards that a zero-length redundant
// EC object is handled correctly on the streaming path. The streaming path handles all
// object sizes including zero; the old size-based fork to buffered reads has been removed.
// The test reads through OpenObject to verify 0-length output — a dispatch-only check
// cannot catch zero-size reconstruct bugs (e.g. header parsing, origSize=0 early-returns).
func TestOpenShardReaders_ZeroSizeObject_Streams(t *testing.T) {
	cfg := ECConfig{DataShards: 1, ParityShards: 1}

	fetcher := &fakeECObjectShardFetcher{}
	// Build zero-size shards (header only, origSize=0).
	buildFakeShards(t, fetcher, "bucket", "key", cfg, []byte{})

	r := ecObjectReader{
		selfID:   "node-self",
		shards:   fetcher,
		ecConfig: cfg,
	}
	rec := PlacementRecord{Nodes: []string{"n0", "n1"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	// Read through OpenObject (not just openShardReaders) to exercise the full
	// reconstruct path and verify 0-length output.
	rc, err := r.OpenObject(context.Background(), "bucket", "key", rec, 0)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Empty(t, got, "zero-size object must produce empty output")
	require.Zero(t, fetcher.readShardCalls, "zero-size object must NOT use the buffered path")
	require.Greater(t, fetcher.readShardStreamCalls, 0, "zero-size object must use ReadShardStream")
}
