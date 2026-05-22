package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/metrics/readamp"
)

// hotChecker is the BoundedLoads interface needed for read attempt reordering.
// Defined here so tests can inject mocks without depending on the full BoundedLoads.
type hotChecker interface {
	IsHot(nodeID string) bool
}

// ecObjectShardFetcher abstracts local and remote shard I/O for EC reads.
type ecObjectShardFetcher interface {
	ReadLocalShard(bucket, key string, shardIdx int) ([]byte, error)
	ReadShard(ctx context.Context, peer, bucket, key string, shardIdx int) ([]byte, error)
	OpenLocalShard(bucket, key string, shardIdx int) (io.ReadCloser, error)
	ReadShardStream(ctx context.Context, peer, bucket, key string, shardIdx int) (io.ReadCloser, error)
	ReadLocalShardAt(bucket, key string, shardIdx int, offset int64, buf []byte) (int, error)
	ReadShardRange(ctx context.Context, peer, bucket, key string, shardIdx int, offset, length int64) ([]byte, error)
	ReadShardRangeStream(ctx context.Context, peer, bucket, key string, shardIdx int, offset, length int64) (io.ReadCloser, error)
}

// ecObjectShardCache abstracts the shard LRU cache for EC reads.
type ecObjectShardCache interface {
	Peek(key string) ([]byte, bool)
	Get(key string) ([]byte, bool)
	Put(key string, data []byte)
	CanStore(key string, size int64) bool
	Stats() shardcache.Stats
}

// Verify that concrete types satisfy the interfaces at compile time.
var _ ecObjectShardFetcher = (*ShardService)(nil)
var _ ecObjectShardCache = (*shardcache.Cache)(nil)

// ecObjectReader reconstructs EC-encoded objects from their constituent shards.
// Mirrors ecObjectWriter: the caller creates one per read, injecting the same
// DistributedBackend fields it already holds.
type ecObjectReader struct {
	selfID     string
	shards     ecObjectShardFetcher
	peerHealth ecObjectPeerHealth // nil = disabled
	cache      ecObjectShardCache // nil = disabled
	ecConfig   ECConfig           // cluster-level fallback; per-object rec overrides
	bl         hotChecker         // nil-safe; nil → legacy behavior (no swap)
}

// Verify that *BoundedLoads satisfies hotChecker at compile time.
var _ hotChecker = (*BoundedLoads)(nil)

// ReadObject reconstructs the full object from EC shards and returns it as a
// byte slice. shardKey is the on-disk path (key or key+"/"+versionID).
func (r ecObjectReader) ReadObject(ctx context.Context, bucket, shardKey string, rec PlacementRecord) ([]byte, error) {
	recCfg, shards, err := r.readShards(ctx, bucket, shardKey, rec)
	if err != nil {
		return nil, err
	}
	return ECReconstruct(recCfg, shards)
}

// OpenObject returns a streaming reader that reconstructs the object via EC
// decode on the fly. objectSize is the original pre-encoding size in bytes.
func (r ecObjectReader) OpenObject(ctx context.Context, bucket, shardKey string, rec PlacementRecord, objectSize int64) (io.ReadCloser, error) {
	recCfg, shardReaders, err := r.openShardReaders(ctx, bucket, shardKey, rec, objectSize)
	if err != nil {
		return nil, err
	}
	readers := make([]io.Reader, len(shardReaders))
	for i, shard := range shardReaders {
		if shard != nil {
			readers[i] = shard
		}
	}
	rc, err := newECReconstructStreamReaderWithPrefetch(recCfg, readers, r.hasLocalDataShard(rec, recCfg))
	if err != nil {
		closeECShardReaders(shardReaders)
		return nil, err
	}
	return &multiReadCloser{Reader: rc, close: func() error {
		err := rc.Close()
		closeECShardReaders(shardReaders)
		return err
	}}, nil
}

// ReadAt reads len(buf) bytes at offset within the EC object without
// reconstructing the full object.
func (r ecObjectReader) ReadAt(ctx context.Context, bucket, shardKey string, rec PlacementRecord, objectSize, offset int64, buf []byte) (int, error) {
	if r.shards == nil {
		return 0, fmt.Errorf("shard service unavailable")
	}
	recCfg := rec.ECConfigOrFallback(r.ecConfig)
	if len(rec.Nodes) != recCfg.NumShards() {
		return 0, fmt.Errorf("placement length %d != expected %d", len(rec.Nodes), recCfg.NumShards())
	}
	if recCfg.DataShards <= 0 {
		return 0, fmt.Errorf("ec readat: invalid data shard count %d", recCfg.DataShards)
	}

	dataShardSize := (objectSize + int64(recCfg.DataShards) - 1) / int64(recCfg.DataShards)
	if dataShardSize <= 0 {
		return 0, io.EOF
	}

	done := 0
	for done < len(buf) {
		global := offset + int64(done)
		if global >= objectSize {
			if done > 0 {
				return done, nil
			}
			return 0, io.EOF
		}
		shardIdx := int(global / dataShardSize)
		if shardIdx >= recCfg.DataShards {
			return done, io.EOF
		}
		shardOffset := global - int64(shardIdx)*dataShardSize
		want := len(buf) - done
		if maxInShard := dataShardSize - shardOffset; int64(want) > maxInShard {
			want = int(maxInShard)
		}
		if maxInObject := objectSize - global; int64(want) > maxInObject {
			want = int(maxInObject)
		}

		n, err := r.readDataShardAt(ctx, bucket, shardKey, rec.Nodes[shardIdx], shardIdx, shardOffset, buf[done:done+want])
		done += n
		if err != nil {
			if done > 0 && errors.Is(err, io.EOF) {
				return done, nil
			}
			return done, err
		}
		if n != want {
			return done, io.ErrUnexpectedEOF
		}
	}
	return done, nil
}

// computeAttemptOrder returns primary and fallback shard idx lists.
// Hot data shards are swapped 1:1 with the lowest available parity idx.
// Both lists are sorted ascending for deterministic ordering.
// hot > m → bypass: primary=dataIdx, fallback=parityIdx (metric incremented).
// bl == nil → legacy: primary=dataIdx, fallback=parityIdx.
func (r ecObjectReader) computeAttemptOrder(rec PlacementRecord, cfg ECConfig) (primary, fallback []int) {
	k := cfg.DataShards
	m := cfg.ParityShards
	dataIdx := make([]int, 0, k)
	parityIdx := make([]int, 0, m)
	for i := 0; i < k; i++ {
		dataIdx = append(dataIdx, i)
	}
	for i := k; i < k+m; i++ {
		parityIdx = append(parityIdx, i)
	}

	if r.bl == nil {
		return dataIdx, parityIdx
	}
	var hotData []int
	for _, i := range dataIdx {
		if r.bl.IsHot(rec.Nodes[i]) {
			hotData = append(hotData, i)
		}
	}
	if len(hotData) == 0 {
		return dataIdx, parityIdx
	}
	if len(hotData) > len(parityIdx) {
		metrics.ClusterBLBypassedReads.Inc()
		return dataIdx, parityIdx
	}

	// Swap hotData[:n] with parityIdx[:n] (n = len(hotData)).
	parityIn := parityIdx[:len(hotData)]
	primarySet := map[int]struct{}{}
	for _, i := range dataIdx {
		primarySet[i] = struct{}{}
	}
	for _, i := range hotData {
		delete(primarySet, i)
	}
	for _, i := range parityIn {
		primarySet[i] = struct{}{}
	}
	for i := range primarySet {
		primary = append(primary, i)
	}
	sort.Ints(primary)

	fallbackSet := map[int]struct{}{}
	for _, i := range parityIdx {
		fallbackSet[i] = struct{}{}
	}
	for _, i := range parityIn {
		delete(fallbackSet, i)
	}
	for _, i := range hotData {
		fallbackSet[i] = struct{}{}
	}
	for i := range fallbackSet {
		fallback = append(fallback, i)
	}
	sort.Ints(fallback)

	// Metric: per-hot-data-node rerank counter.
	for _, i := range hotData {
		metrics.ClusterBLRerankedReads.WithLabelValues(rec.Nodes[i]).Inc()
	}
	return primary, fallback
}

// readShards collects the k-of-n data shards needed to reconstruct the object.
//
// Strategy: satisfy data shards first to avoid healthy-path parity fanout; fall
// back to parity shards only when data shards are unavailable.
func (r ecObjectReader) readShards(ctx context.Context, bucket, shardKey string, rec PlacementRecord) (ECConfig, [][]byte, error) {
	recCfg := rec.ECConfigOrFallback(r.ecConfig)
	if len(rec.Nodes) != recCfg.NumShards() {
		return ECConfig{}, nil, fmt.Errorf("placement length %d != expected %d", len(rec.Nodes), recCfg.NumShards())
	}
	if r.shards == nil {
		return ECConfig{}, nil, fmt.Errorf("shard service unavailable")
	}

	shards := make([][]byte, len(rec.Nodes))
	available := 0
	cached := make([]bool, len(rec.Nodes))

	// Cache pre-pass: satisfy from cache to avoid disk/network I/O.
	if r.cache != nil {
		cachedIdx := make([]int, 0, len(rec.Nodes))
		for i := range rec.Nodes {
			if data, ok := r.cache.Peek(shardCacheKey(bucket, shardKey, i)); ok {
				shards[i] = data
				cached[i] = true
				cachedIdx = append(cachedIdx, i)
				available++
			}
		}
		if available >= recCfg.DataShards {
			for _, i := range cachedIdx[:recCfg.DataShards] {
				readamp.RecordECShard(shardCacheKey(bucket, shardKey, i))
				_, _ = r.cache.Get(shardCacheKey(bucket, shardKey, i))
			}
			return recCfg, shards, nil
		}
		for _, i := range cachedIdx {
			readamp.RecordECShard(shardCacheKey(bucket, shardKey, i))
			_, _ = r.cache.Get(shardCacheKey(bucket, shardKey, i))
		}
		for i := range rec.Nodes {
			if !cached[i] {
				// A cache miss means this request will fall through to
				// ReadShard/ReadLocalShard for that shard.
				readamp.RecordECShard(shardCacheKey(bucket, shardKey, i))
				if data, ok := r.cache.Get(shardCacheKey(bucket, shardKey, i)); ok {
					shards[i] = data
					cached[i] = true
					available++
				}
				if available == recCfg.DataShards {
					return recCfg, shards, nil
				}
			}
		}
	}

	type shardResult struct {
		idx      int
		data     []byte
		err      error
		peer     string // non-empty for remote reads; empty for local/selfID reads
		peerOK   bool   // true if remote read succeeded (only valid when peer != "")
		canceled bool   // true if k-of-n early-exit caused the cancellation error
	}

	markPeerHealth := func(res shardResult) {
		if res.peer != "" && r.peerHealth != nil && !res.canceled {
			if res.peerOK {
				r.peerHealth.MarkHealthy(res.peer)
			} else {
				r.peerHealth.MarkUnhealthy(res.peer)
			}
		}
	}

	applyShardResult := func(res shardResult) bool {
		markPeerHealth(res)
		if res.err != nil || res.data == nil {
			return false
		}
		if available < recCfg.DataShards {
			shards[res.idx] = res.data
		}
		if r.cache != nil {
			r.cache.Put(shardCacheKey(bucket, shardKey, res.idx), res.data)
		}
		available++
		return true
	}

	selfID := r.selfID
	fetchShards := func(indices []int, stopAtK bool) {
		if len(indices) == 0 || available >= recCfg.DataShards {
			return
		}
		readCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		resultCh := make(chan shardResult, len(indices))
		dispatched := 0
		for _, i := range indices {
			if cached[i] || shards[i] != nil {
				continue
			}
			i, node := i, rec.Nodes[i]
			if r.cache == nil {
				readamp.RecordECShard(shardCacheKey(bucket, shardKey, i))
			}
			dispatched++
			go func() {
				var data []byte
				var err error
				var peer string
				var peerOK, canceled bool
				if node == selfID {
					data, err = r.shards.ReadLocalShard(bucket, shardKey, i)
				} else {
					peer = node
					shardCtx, shardCancel := context.WithTimeout(readCtx, shardRPCTimeout)
					defer shardCancel()
					data, err = r.shards.ReadShard(shardCtx, node, bucket, shardKey, i)
					if err != nil {
						if errors.Is(err, context.Canceled) && readCtx.Err() != nil {
							canceled = true
						}
					} else {
						peerOK = true
					}
				}
				resultCh <- shardResult{idx: i, data: data, err: err, peer: peer, peerOK: peerOK, canceled: canceled}
			}()
		}
		if dispatched == 0 {
			return
		}

		for j := 0; j < dispatched; j++ {
			res := <-resultCh
			if applyShardResult(res) && stopAtK && available == recCfg.DataShards {
				cancel()
				for k := j + 1; k < dispatched; k++ {
					select {
					case drained := <-resultCh:
						markPeerHealth(drained)
					case <-ctx.Done():
						return
					}
				}
				return
			}
		}
	}

	// Determine primary and fallback shard indices, swapping hot data shards to
	// parity when BoundedLoads is active.
	primary, fallback := r.computeAttemptOrder(rec, recCfg)

	// Local primary-shard fast path: if all primary shards are local-or-cached,
	// read them without spinning up goroutines.
	localDataFastPath := true
	for _, i := range primary {
		if !cached[i] && rec.Nodes[i] != selfID {
			localDataFastPath = false
			break
		}
	}
	if !localDataFastPath {
		fetchShards(primary, false)
		if available < recCfg.DataShards {
			fetchShards(fallback, true)
			if available < recCfg.DataShards {
				return ECConfig{}, nil, fmt.Errorf("ec get: only %d/%d shards available, need %d",
					available, len(rec.Nodes), recCfg.DataShards)
			}
		}
		return recCfg, shards, nil
	}

	fetchShards(primary, false)
	if available >= recCfg.DataShards {
		return recCfg, shards, nil
	}

	fetchShards(fallback, true)
	if available < recCfg.DataShards {
		return ECConfig{}, nil, fmt.Errorf("ec get: only %d/%d shards available, need %d",
			available, len(rec.Nodes), recCfg.DataShards)
	}
	return recCfg, shards, nil
}

// openShardReaders opens streaming readers for the shards needed to reconstruct
// the object. Uses buffered readers (via readShards) when the object fits in
// the shard cache; otherwise opens direct streaming connections.
func (r ecObjectReader) openShardReaders(ctx context.Context, bucket, shardKey string, rec PlacementRecord, objectSize int64) (ECConfig, []io.ReadCloser, error) {
	recCfg := rec.ECConfigOrFallback(r.ecConfig)
	if len(rec.Nodes) != recCfg.NumShards() {
		return ECConfig{}, nil, fmt.Errorf("placement length %d != expected %d", len(rec.Nodes), recCfg.NumShards())
	}
	if r.shards == nil {
		return ECConfig{}, nil, fmt.Errorf("shard service unavailable")
	}

	if recCfg.Redundant() && objectSize >= 0 && objectSize <= maxECPooledReadObjectSize {
		return r.bufferedShardReaders(ctx, bucket, shardKey, rec)
	}
	if r.cacheCanStore(bucket, shardKey, recCfg, objectSize) {
		return r.bufferedShardReaders(ctx, bucket, shardKey, rec)
	}

	shardReaders := make([]io.ReadCloser, len(rec.Nodes))
	available := 0
	openShard := func(i int) bool {
		readamp.RecordECShard(shardCacheKey(bucket, shardKey, i))
		node := rec.Nodes[i]
		if node == r.selfID {
			rc, err := r.shards.OpenLocalShard(bucket, shardKey, i)
			if err != nil {
				return false
			}
			shardReaders[i] = rc
			available++
			return true
		}

		shardCtx, shardCancel := context.WithTimeout(ctx, shardRPCTimeout)
		rc, err := r.shards.ReadShardStream(shardCtx, node, bucket, shardKey, i)
		if err != nil {
			shardCancel()
			if r.peerHealth != nil {
				r.peerHealth.MarkUnhealthy(node)
			}
			return false
		}
		if r.peerHealth != nil {
			r.peerHealth.MarkHealthy(node)
		}
		shardReaders[i] = &multiReadCloser{Reader: rc, close: func() error {
			err := rc.Close()
			shardCancel()
			return err
		}}
		available++
		return true
	}

	// Apply BL re-routing: hot data shards are swapped to fallback so parity
	// is attempted first. Mirrors the same swap used in readShards.
	primary, fallback := r.computeAttemptOrder(rec, recCfg)
	for _, i := range primary {
		openShard(i)
	}
	if available >= recCfg.DataShards {
		return recCfg, shardReaders, nil
	}
	for _, i := range fallback {
		if available >= recCfg.DataShards {
			break
		}
		openShard(i)
	}
	if available < recCfg.DataShards {
		closeECShardReaders(shardReaders)
		return ECConfig{}, nil, fmt.Errorf("ec get: only %d/%d shards available, need %d",
			available, len(rec.Nodes), recCfg.DataShards)
	}
	return recCfg, shardReaders, nil
}

// bufferedShardReaders collects all shards via readShards and wraps them as
// NopCloser byte readers. Called when the object fits in the shard cache.
func (r ecObjectReader) bufferedShardReaders(ctx context.Context, bucket, shardKey string, rec PlacementRecord) (ECConfig, []io.ReadCloser, error) {
	recCfg, dataShards, err := r.readShards(ctx, bucket, shardKey, rec)
	if err != nil {
		return ECConfig{}, nil, err
	}
	shardReaders := make([]io.ReadCloser, len(dataShards))
	for i, data := range dataShards {
		if data != nil {
			shardReaders[i] = io.NopCloser(bytes.NewReader(data))
		}
	}
	return recCfg, shardReaders, nil
}

// readDataShardAt reads len(buf) bytes at shardOffset within a single data
// shard. Remote reads prefer buffered RPC when len(buf) <= maxShardRangeReplyBytes.
func (r ecObjectReader) readDataShardAt(ctx context.Context, bucket, shardKey, node string, shardIdx int, shardOffset int64, buf []byte) (int, error) {
	readamp.RecordECShard(shardCacheKey(bucket, shardKey, shardIdx))
	rangeCacheKey := ""
	if r.cache != nil && len(buf) > 0 {
		rangeCacheKey = shardRangeCacheKey(bucket, shardKey, shardIdx, shardOffset, int64(len(buf)))
		if data, ok := r.cache.Get(rangeCacheKey); ok {
			return copy(buf, data), nil
		}
	}
	if node == r.selfID {
		return r.shards.ReadLocalShardAt(bucket, shardKey, shardIdx, shardHeaderSize+shardOffset, buf)
	}

	shardCtx, shardCancel := context.WithTimeout(ctx, shardRPCTimeout)
	defer shardCancel()
	if len(buf) <= maxShardRangeReplyBytes {
		data, err := r.shards.ReadShardRange(shardCtx, node, bucket, shardKey, shardIdx, shardHeaderSize+shardOffset, int64(len(buf)))
		if err != nil {
			if r.peerHealth != nil {
				r.peerHealth.MarkUnhealthy(node)
			}
			return 0, err
		}
		if r.peerHealth != nil {
			r.peerHealth.MarkHealthy(node)
		}
		n := copy(buf, data)
		if n != len(buf) || n != len(data) {
			return n, io.ErrUnexpectedEOF
		}
		r.cacheReadAtRange(rangeCacheKey, buf[:n], n, len(buf), nil)
		return n, nil
	}
	rc, err := r.shards.ReadShardRangeStream(shardCtx, node, bucket, shardKey, shardIdx, shardHeaderSize+shardOffset, int64(len(buf)))
	if err != nil {
		if r.peerHealth != nil {
			r.peerHealth.MarkUnhealthy(node)
		}
		return 0, err
	}
	defer rc.Close()
	if r.peerHealth != nil {
		r.peerHealth.MarkHealthy(node)
	}
	n, err := io.ReadFull(rc, buf)
	r.cacheReadAtRange(rangeCacheKey, buf[:n], n, len(buf), err)
	return n, err
}

func (r ecObjectReader) cacheReadAtRange(key string, data []byte, n, want int, err error) {
	if key == "" || r.cache == nil || err != nil || n != want {
		return
	}
	if r.cache.CanStore(key, int64(n)) {
		r.cache.Put(key, data)
	}
}

// cacheCanStore reports whether every shard slot for this object fits in the
// shard cache without evicting currently resident data.
func (r ecObjectReader) cacheCanStore(bucket, shardKey string, cfg ECConfig, objectSize int64) bool {
	if r.cache == nil || r.cache.Stats().CapacityByte <= 0 || cfg.DataShards <= 0 || objectSize < 0 {
		return false
	}
	perDataShard := (objectSize + int64(cfg.DataShards) - 1) / int64(cfg.DataShards)
	shardSize := int64(shardHeaderSize) + perDataShard
	for i := 0; i < cfg.NumShards(); i++ {
		if !r.cache.CanStore(shardCacheKey(bucket, shardKey, i), shardSize) {
			return false
		}
	}
	return true
}

// hasLocalDataShard reports whether any of the K data shards is stored on self.
func (r ecObjectReader) hasLocalDataShard(rec PlacementRecord, cfg ECConfig) bool {
	for i := 0; i < cfg.DataShards && i < len(rec.Nodes); i++ {
		if rec.Nodes[i] == r.selfID {
			return true
		}
	}
	return false
}

func closeECShardReaders(shards []io.ReadCloser) {
	for _, shard := range shards {
		if shard != nil {
			_ = shard.Close()
		}
	}
}
