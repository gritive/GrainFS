package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cache/shardcache"
)

// fakeECObjectShardFetcher records calls and returns configurable data/errors.
type fakeECObjectShardFetcher struct {
	localShards map[string][]byte // "bucket/key/idx" → raw shard bytes (with header)
	remoteErr   map[string]error  // node → error to return
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
	data, err := f.ReadShard(ctx, peer, bucket, key, shardIdx)
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

func (f *fakeECObjectShardFetcher) ReadShardRange(ctx context.Context, peer, bucket, key string, shardIdx int, offset, length int64) ([]byte, error) {
	data, err := f.ReadShard(ctx, peer, bucket, key, shardIdx)
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
	return append([]byte(nil), data[offset:end]...), nil
}

func (f *fakeECObjectShardFetcher) ReadShardRangeStream(ctx context.Context, peer, bucket, key string, shardIdx int, offset, length int64) (io.ReadCloser, error) {
	data, err := f.ReadShardRange(ctx, peer, bucket, key, shardIdx, offset, length)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// fakeECObjectPeerHealth records mark calls.
type fakeECObjectPeerHealth struct {
	healthy   []string
	unhealthy []string
}

func (f *fakeECObjectPeerHealth) MarkHealthy(peer string) bool {
	f.healthy = append(f.healthy, peer)
	return true
}

func (f *fakeECObjectPeerHealth) MarkUnhealthy(peer string) bool {
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
func buildFakeShards(t *testing.T, fetcher *fakeECObjectShardFetcher, bucket, shardKey string, cfg ECConfig, data []byte) {
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

func TestECObjectReader_ReadObject_AllLocal(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	data := []byte("hello world from EC")
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "key/v1", cfg, data)

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	got, err := r.ReadObject(context.Background(), "bucket", "key/v1", rec)
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

	got, err := r.ReadObject(context.Background(), "bucket", "obj", rec)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestECObjectReader_ReadObject_MarksUnhealthyPeerOnFetchError(t *testing.T) {
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

	got, err := r.ReadObject(context.Background(), "bucket", "key", rec)
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.Contains(t, health.unhealthy, "node-b")
}

func TestECObjectReader_ReadObject_ErrorsWhenNotEnoughShards(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	fetcher := &fakeECObjectShardFetcher{} // empty — no shards available

	r := ecObjectReader{selfID: "node-a", shards: fetcher, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	_, err := r.ReadObject(context.Background(), "bucket", "key", rec)
	require.Error(t, err)
}

func TestECObjectReader_ReadObject_UsesCache(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	data := []byte("cached read")
	fetcher := &fakeECObjectShardFetcher{} // fetcher has no shards
	cache := newFakeCache(1 << 20)

	// Pre-populate the cache with the shards.
	shards, err := ECSplit(cfg, data)
	require.NoError(t, err)
	for i, s := range shards {
		cache.Put(shardCacheKey("bucket", "key", i), s)
	}

	r := ecObjectReader{selfID: "node-a", shards: fetcher, cache: cache, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a", "node-a"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	got, err := r.ReadObject(context.Background(), "bucket", "key", rec)
	require.NoError(t, err)
	require.Equal(t, data, got)
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

func TestECObjectReader_NilShardService_ReturnsError(t *testing.T) {
	r := ecObjectReader{
		selfID:   "node-a",
		shards:   nil,
		ecConfig: ECConfig{DataShards: 2, ParityShards: 1},
	}
	rec := PlacementRecord{Nodes: []string{"node-a", "node-a", "node-a"}}
	rec.K = 2
	rec.M = 1

	_, err := r.ReadObject(context.Background(), "bucket", "key", rec)
	require.Error(t, err)
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

func TestECObjectReader_CacheCanStore_NilCache(t *testing.T) {
	r := ecObjectReader{cache: nil}
	require.False(t, r.cacheCanStore("b", "k", ECConfig{DataShards: 2, ParityShards: 1}, 100))
}

func TestECObjectReader_CacheCanStore_ZeroCapacity(t *testing.T) {
	r := ecObjectReader{cache: newFakeCache(0)}
	require.False(t, r.cacheCanStore("b", "k", ECConfig{DataShards: 2, ParityShards: 1}, 100))
}

func TestECObjectReader_CacheCanStore_FitsInCache(t *testing.T) {
	r := ecObjectReader{cache: newFakeCache(1 << 20)}
	require.True(t, r.cacheCanStore("b", "k", ECConfig{DataShards: 2, ParityShards: 1}, 100))
}
