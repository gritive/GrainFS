package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/metrics/readamp"
)

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
}

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

// readShards collects the k-of-n data shards needed to reconstruct the object.
//
// Strategy: try a local data-shard fast path first; fall back to a full
// parallel fan-out that includes parity shards when any remote data shard is
// unavailable.
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

	// Local data-shard fast path: if all K data shards are local, read them
	// without spinning up goroutines.
	dataIdx := make([]int, 0, recCfg.DataShards)
	localDataFastPath := true
	for i := 0; i < recCfg.DataShards; i++ {
		dataIdx = append(dataIdx, i)
		if !cached[i] && rec.Nodes[i] != selfID {
			localDataFastPath = false
		}
	}
	if !localDataFastPath {
		allIdx := make([]int, 0, len(rec.Nodes))
		for i := range rec.Nodes {
			allIdx = append(allIdx, i)
		}
		fetchShards(allIdx, true)
		if available < recCfg.DataShards {
			return ECConfig{}, nil, fmt.Errorf("ec get: only %d/%d shards available, need %d",
				available, len(rec.Nodes), recCfg.DataShards)
		}
		return recCfg, shards, nil
	}

	fetchShards(dataIdx, false)
	if available >= recCfg.DataShards {
		return recCfg, shards, nil
	}

	parityIdx := make([]int, 0, recCfg.ParityShards)
	for i := recCfg.DataShards; i < len(rec.Nodes); i++ {
		parityIdx = append(parityIdx, i)
	}
	fetchShards(parityIdx, true)
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

	for i := 0; i < recCfg.DataShards; i++ {
		openShard(i)
	}
	if available >= recCfg.DataShards {
		return recCfg, shardReaders, nil
	}
	for i := recCfg.DataShards; i < len(rec.Nodes) && available < recCfg.DataShards; i++ {
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
	return io.ReadFull(rc, buf)
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
