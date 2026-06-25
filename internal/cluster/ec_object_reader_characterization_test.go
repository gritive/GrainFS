package cluster

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cache/shardcache"
)

// These are CHARACTERIZATION tests: they pin the current observable behavior of
// (ecObjectReader).readShards on two subtle paths existing coverage is thin on
// (drain-under-cancel peer-health, and cache partial-promote). They are green on
// the current production code and MUST stay green through the readShards
// decomposition refactor. They are not bug-finding tests.

// blockingShardFetcher is a ctx-aware shard fetcher. Each shard index either
// returns immediately (from localShards) or blocks on a per-index release
// channel until released OR the read context is canceled. This makes the
// k-of-n early-exit cancel/drain path deterministic: the K winners are released
// first, then the slow shard observes the cancel (context.Canceled) rather than
// racing.
//
// It is a separate type from fakeECObjectShardFetcher (and not an embedding of
// it) deliberately: that fake's ReadShard ignores ctx, so it cannot honor the
// cancellation that the drain path depends on, and a race-dependent repro would
// be flaky. The per-index release/cancel semantics are specific to this
// characterization test, so a distinct minimal fake is clearer than bolting
// optional blocking onto the shared fake used by ~20 other tests.
type blockingShardFetcher struct {
	localShards map[string][]byte // "bucket/key/idx" → raw shard bytes (with header)
	remoteErr   map[string]error  // node → immediate error (fail-fast shards)
	release     map[int]chan struct{}

	mu             sync.Mutex
	readShardCalls int
}

func (f *blockingShardFetcher) key(bucket, key string, idx int) string {
	return shardCacheKey(bucket, key, idx)
}

func (f *blockingShardFetcher) ReadShard(ctx context.Context, peer, bucket, key string, shardIdx int) ([]byte, error) {
	f.mu.Lock()
	f.readShardCalls++
	f.mu.Unlock()
	if err := f.remoteErr[peer]; err != nil {
		return nil, err
	}
	if ch, ok := f.release[shardIdx]; ok {
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	data, ok := f.localShards[f.key(bucket, key, shardIdx)]
	if !ok {
		return nil, errors.New("shard not found")
	}
	return append([]byte(nil), data...), nil
}

// Read-side methods the EC reader may touch; only ReadShard is exercised here.
func (f *blockingShardFetcher) ReadLocalShard(bucket, key string, shardIdx int) ([]byte, error) {
	data, ok := f.localShards[f.key(bucket, key, shardIdx)]
	if !ok {
		return nil, errors.New("local shard not found")
	}
	return append([]byte(nil), data...), nil
}

func (f *blockingShardFetcher) OpenLocalShard(string, string, int) (io.ReadCloser, error) {
	panic("OpenLocalShard: not used by this characterization test")
}

func (f *blockingShardFetcher) ReadShardStream(context.Context, string, string, string, int) (io.ReadCloser, error) {
	panic("ReadShardStream: not used by this characterization test")
}

func (f *blockingShardFetcher) ReadLocalShardAt(string, string, int, int64, []byte) (int, error) {
	panic("ReadLocalShardAt: not used by this characterization test")
}

func (f *blockingShardFetcher) ReadShardRange(context.Context, string, string, string, int, int64, int64) ([]byte, error) {
	panic("ReadShardRange: not used by this characterization test")
}

func (f *blockingShardFetcher) ReadShardRangeStream(context.Context, string, string, string, int, int64, int64) (io.ReadCloser, error) {
	panic("ReadShardRangeStream: not used by this characterization test")
}

// Write-side stubs satisfy ecShardStore; the reader never invokes them.
func (f *blockingShardFetcher) WriteLocalShardContext(context.Context, string, string, int, []byte) error {
	panic("WriteLocalShardContext: not used by ecObjectReader")
}

func (f *blockingShardFetcher) WriteLocalShardStreamContext(context.Context, string, string, int, io.Reader) error {
	panic("WriteLocalShardStreamContext: not used by ecObjectReader")
}

func (f *blockingShardFetcher) DeleteLocalShards(string, string) error {
	panic("DeleteLocalShards: not used by ecObjectReader")
}

func (f *blockingShardFetcher) WriteShard(context.Context, string, string, string, int, []byte) error {
	panic("WriteShard: not used by ecObjectReader")
}

func (f *blockingShardFetcher) WriteShardStream(context.Context, string, string, string, int, io.Reader) error {
	panic("WriteShardStream: not used by ecObjectReader")
}

func (f *blockingShardFetcher) WriteLocalShardStreamStagedContext(context.Context, string, string, string, int, io.Reader) error {
	panic("WriteLocalShardStreamStagedContext: not used by ecObjectReader")
}

func (f *blockingShardFetcher) WriteShardStreamStaged(context.Context, string, string, string, string, int, io.Reader) error {
	panic("WriteShardStreamStaged: not used by ecObjectReader")
}

func (f *blockingShardFetcher) DeleteShards(context.Context, string, string, string) error {
	panic("DeleteShards: not used by ecObjectReader")
}

// TestECObjectReader_ReadShards_DrainUnderCancel_DoesNotMarkCanceledPeerUnhealthy
// pins invariant (5): when k-of-n early-exit (stopAtK fallback) reaches K and
// cancels the remaining in-flight remote read, the canceled peer must NOT be
// marked unhealthy (canceled==true skips the mark), while the K winners are
// marked healthy.
func TestECObjectReader_ReadShards_DrainUnderCancel_DoesNotMarkCanceledPeerUnhealthy(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 3}
	data := []byte("drain-under-cancel characterization payload")

	fetcher := &blockingShardFetcher{
		localShards: make(map[string][]byte),
		remoteErr: map[string]error{
			// Both data shards fail fast → primary yields 0, forcing the
			// stopAtK fallback fan-out across the parity shards.
			"d0": errors.New("data shard 0 down"),
			"d1": errors.New("data shard 1 down"),
		},
		release: map[int]chan struct{}{
			// Parity winners released immediately; the laggard (idx 4) blocks
			// until our own k-of-n cancel fires.
			2: closedCh(),
			3: closedCh(),
			4: make(chan struct{}), // never released → observes ctx.Canceled
		},
	}
	buildBlockingShards(t, fetcher, "bucket", "key", cfg, data)
	// Remove the data-shard bytes so even if dispatched they cannot satisfy.
	delete(fetcher.localShards, shardCacheKey("bucket", "key", 0))
	delete(fetcher.localShards, shardCacheKey("bucket", "key", 1))

	health := &fakeECObjectPeerHealth{}
	r := ecObjectReader{
		selfID:     "node-self",
		shards:     fetcher,
		peerHealth: health,
		ecConfig:   cfg,
	}
	rec := PlacementRecord{Nodes: []string{"d0", "d1", "p2", "p3", "p4"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	got, err := r.ReadObject(context.Background(), "bucket", "key", rec)
	require.NoError(t, err)
	require.Equal(t, data, got)

	health.mu.Lock()
	defer health.mu.Unlock()
	// The two released parity peers are the K winners → marked healthy.
	require.Contains(t, health.healthy, "p2")
	require.Contains(t, health.healthy, "p3")
	// The canceled laggard p4 must NOT be marked unhealthy (its error is our
	// own k-of-n cancellation, not a genuine peer failure).
	require.NotContains(t, health.unhealthy, "p4", "canceled peer must not be marked unhealthy")
	require.NotContains(t, health.healthy, "p4", "canceled peer must not be marked healthy either")
	// The genuinely-failed data peers ARE marked unhealthy.
	require.Contains(t, health.unhealthy, "d0")
	require.Contains(t, health.unhealthy, "d1")
}

// spyShardCache records Peek/Get call counts so the cache partial-promote
// accounting (only the first K of cachedIdx get Get-promoted when Peek already
// satisfies K) is assertable. fakeECObjectShardCache has no call instrumentation
// and is used unmodified by other tests; adding per-key counters here keeps that
// shared fake untouched. (The spec's note to reuse the CacheBypassSpy spy was a
// mis-pointer — that is a spyHotChecker, not a cache spy; none existed.)
type spyShardCache struct {
	data      map[string][]byte
	capacity  int64
	peekCalls map[string]int
	getCalls  map[string]int
}

func newSpyCache(capacity int64) *spyShardCache {
	return &spyShardCache{
		data:      make(map[string][]byte),
		capacity:  capacity,
		peekCalls: make(map[string]int),
		getCalls:  make(map[string]int),
	}
}

func (c *spyShardCache) Peek(key string) ([]byte, bool) {
	c.peekCalls[key]++
	v, ok := c.data[key]
	return v, ok
}

func (c *spyShardCache) Get(key string) ([]byte, bool) {
	c.getCalls[key]++
	v, ok := c.data[key]
	return v, ok
}

func (c *spyShardCache) Put(key string, data []byte) {
	c.data[key] = append([]byte(nil), data...)
}

func (c *spyShardCache) CanStore(_ string, size int64) bool { return size <= c.capacity }

func (c *spyShardCache) Stats() shardcache.Stats { return shardcache.Stats{CapacityByte: c.capacity} }

// TestECObjectReader_ReadShards_CachePartialPromote pins invariant (2): when
// Peek satisfies fewer than K cached shards, the Get-loop promotes non-cached
// entries until exactly K are available, and Get is called only as far as
// needed (not for shards past the K-th). With this 2+1 config and 1 pre-cached
// shard, Peek finds 1 (< K=2), then the Get-promote loop fetches the next
// non-cached shard from the cache to reach K.
func TestECObjectReader_ReadShards_CachePartialPromote(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	data := []byte("cache partial promote characterization")

	shards, err := ECSplit(cfg, data)
	require.NoError(t, err)

	cache := newSpyCache(1 << 20)
	// Pre-populate ALL shards into the cache, but the fetcher has none: the read
	// must be fully served from the cache. Peek finds all 3, available(3) >= K,
	// so the satisfied-from-Peek branch Get-promotes only the first K of
	// cachedIdx (idx 0 and 1).
	for i, s := range shards {
		cache.Put(shardCacheKey("bucket", "key", i), s)
	}

	fetcher := &fakeECObjectShardFetcher{} // no disk/remote shards
	r := ecObjectReader{selfID: "node-self", shards: fetcher, cache: cache, ecConfig: cfg}
	rec := PlacementRecord{Nodes: []string{"node-self", "node-self", "node-self"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	got, err := r.ReadObject(context.Background(), "bucket", "key", rec)
	require.NoError(t, err)
	require.Equal(t, data, got)

	// Peek is called once per node (all 3 probed).
	require.Equal(t, 1, cache.peekCalls[shardCacheKey("bucket", "key", 0)])
	require.Equal(t, 1, cache.peekCalls[shardCacheKey("bucket", "key", 1)])
	require.Equal(t, 1, cache.peekCalls[shardCacheKey("bucket", "key", 2)])
	// Peek satisfied K=2 → only the FIRST K of cachedIdx are Get-promoted.
	require.Equal(t, 1, cache.getCalls[shardCacheKey("bucket", "key", 0)])
	require.Equal(t, 1, cache.getCalls[shardCacheKey("bucket", "key", 1)])
	require.Equal(t, 0, cache.getCalls[shardCacheKey("bucket", "key", 2)], "shard past the K-th must NOT be Get-promoted")
	// No disk/remote fetch occurred.
	require.Zero(t, fetcher.readShardCalls)
}

// TestECObjectReader_ReadShards_CacheUnderK_PromotesUntilK pins the OTHER cache
// accounting path: when Peek satisfies fewer than K, every Peeked shard is
// Get-promoted, then non-cached entries are Get-promoted (from the cache) until
// exactly K are available.
func TestECObjectReader_ReadShards_CacheUnderK_PromotesUntilK(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	data := []byte("cache under-K promote until K")

	shards, err := ECSplit(cfg, data)
	require.NoError(t, err)

	cache := newSpyCache(1 << 20)
	// Pre-cache only shard 0 so Peek finds 1 (< K=2) and the under-K branch
	// runs: every Peeked shard is Get-promoted, then non-cached entries are
	// Get-probed in index order until exactly K are available. Shards 1 and 2
	// are not in the cache, so their Get returns miss and the read falls through
	// to the fetcher (local) for the remaining data shard.
	cache.Put(shardCacheKey("bucket", "key", 0), shards[0])

	// The fetcher serves shards 1 and 2 from disk (local).
	fetcher := &fakeECObjectShardFetcher{}
	buildFakeShards(t, fetcher, "bucket", "key", cfg, data)

	r := ecObjectReader{selfID: "node-self", shards: fetcher, cache: cache, ecConfig: cfg}
	// All-local so the fetch falls through to ReadLocalShard for the missing shard.
	rec := PlacementRecord{Nodes: []string{"node-self", "node-self", "node-self"}}
	rec.K = cfg.DataShards
	rec.M = cfg.ParityShards

	got, err := r.ReadObject(context.Background(), "bucket", "key", rec)
	require.NoError(t, err)
	require.Equal(t, data, got)

	// Peek probes all nodes.
	require.Equal(t, 1, cache.peekCalls[shardCacheKey("bucket", "key", 0)])
	// Only shard 0 was in the cache for Peek → it is Get-promoted in the
	// under-K branch.
	require.Equal(t, 1, cache.getCalls[shardCacheKey("bucket", "key", 0)])
	// The under-K branch then Get-probes non-cached entries (1, then stops at K).
	require.GreaterOrEqual(t, cache.getCalls[shardCacheKey("bucket", "key", 1)], 1)
}

func closedCh() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func buildBlockingShards(t testing.TB, fetcher *blockingShardFetcher, bucket, shardKey string, cfg ECConfig, data []byte) {
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
