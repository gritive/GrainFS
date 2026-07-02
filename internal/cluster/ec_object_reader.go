package cluster

import (
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

// ecObjectShardCache abstracts the shard LRU cache for EC reads.
type ecObjectShardCache interface {
	Peek(key string) ([]byte, bool)
	Get(key string) ([]byte, bool)
	Put(key string, data []byte)
	CanStore(key string, size int64) bool
	Stats() shardcache.Stats
}

// Verify that concrete types satisfy the interfaces at compile time.
var _ ecShardStore = (*ShardService)(nil)
var _ ecObjectShardCache = (*shardcache.Cache)(nil)

// ecObjectReader reconstructs EC-encoded objects from their constituent shards.
// Mirrors ecObjectWriter: the caller creates one per read, injecting the same
// DistributedBackend fields it already holds.
type ecObjectReader struct {
	selfID     string
	shards     ecShardStore
	peerHealth ecObjectPeerHealth // nil = disabled
	cache      ecObjectShardCache // nil = disabled
	ecConfig   ECConfig           // cluster-level fallback; per-object rec overrides
	bl         hotChecker         // nil-safe; nil → legacy behavior (no swap)
}

// Verify that *BoundedLoads satisfies hotChecker at compile time.
var _ hotChecker = (*BoundedLoads)(nil)

// markShardUnhealthy attributes a shard-level fault (truncation) to the
// serving peer. Local shards never self-mark, matching the endpoint rule.
func (r ecObjectReader) markShardUnhealthy(rec PlacementRecord, idx int) {
	if r.peerHealth == nil || idx < 0 || idx >= len(rec.Nodes) {
		return
	}
	node := rec.Nodes[idx]
	if node == r.selfID {
		return
	}
	r.peerHealth.MarkUnhealthy(node)
}

// markShardHealthy records clean-completion evidence for the serving peer.
// Local shards never self-mark, matching markShardUnhealthy.
func (r ecObjectReader) markShardHealthy(rec PlacementRecord, idx int) {
	if r.peerHealth == nil || idx < 0 || idx >= len(rec.Nodes) {
		return
	}
	node := rec.Nodes[idx]
	if node == r.selfID {
		return
	}
	r.peerHealth.MarkHealthy(node)
}

// attributeShardOpenError applies per-shard health attribution for the typed
// open-time validation failures ecReconstructStreamBodies surfaces (shared by
// the contiguous and stripe paths). A typed header truncation carries the
// shard index: attribute it to the serving peer. An anchored-mode header
// mismatch marks every lying shard's peer, but ONLY when at least one shard
// corroborated the metadata anchor: unanimous disagreement means the anchor
// itself is just as suspect (corrupt metadata would otherwise mass-mark every
// honest peer serving the object, node-level, on every retry).
func (r ecObjectReader) attributeShardOpenError(rec PlacementRecord, err error) {
	var terr *ecShardTruncatedError
	if errors.As(err, &terr) {
		r.markShardUnhealthy(rec, terr.Idx)
	}
	var merr *ecShardHeaderMismatchError
	if errors.As(err, &merr) && merr.AnchorCorroborated {
		for _, i := range merr.Idxs {
			r.markShardUnhealthy(rec, i)
		}
	}
}

// nonClosingReader hides an underlying reader's io.Closer so a downstream
// reader that closes its inputs (stripeDeinterleaveStreamReader.Close) cannot
// double-close shard readers owned by closeECShardReaders.
type nonClosingReader struct{ io.Reader }

// OpenObject returns a streaming reader that reconstructs the object via EC
// decode on the fly. objectSize is the original pre-encoding size in bytes.
func (r ecObjectReader) OpenObject(ctx context.Context, bucket, shardKey string, rec PlacementRecord, objectSize int64) (io.ReadCloser, error) {
	recCfg, shardReaders, err := r.openShardReaders(ctx, bucket, shardKey, rec)
	if err != nil {
		return nil, err
	}
	onShardFault := func(i int) { r.markShardUnhealthy(rec, i) }
	onShardClean := func(i int) { r.markShardHealthy(rec, i) }
	readers := make([]io.Reader, len(shardReaders))
	for i, shard := range shardReaders {
		if shard != nil {
			readers[i] = shard
		}
	}

	if rec.StripeBytes > 0 {
		// Stripe-interleaved objects validate shard headers at open, exactly
		// like the contiguous path: ecReconstructStreamBodies consumes the
		// 8-byte header from every present reader and checks it against the
		// metadata-authoritative objectSize with cross-shard corroboration
		// (see ecShardHeaderMismatchError.AnchorCorroborated), then hands the
		// post-header bodies to the bounded de-interleave reader. Bodies are
		// wrapped nonClosingReader so the stripe reader's Close cannot close
		// them — the underlying shardReaders are closed exactly once by
		// closeECShardReaders below. The returned origSize is discarded: in
		// anchored mode it IS objectSize.
		_, bodies, err := ecReconstructStreamBodies(recCfg, readers, objectSize)
		if err != nil {
			r.attributeShardOpenError(rec, err)
			closeECShardReaders(shardReaders)
			return nil, err
		}
		for i, b := range bodies {
			if b != nil {
				bodies[i] = nonClosingReader{Reader: b}
			}
		}
		rc, err := newStripeDeinterleaveStreamReader(recCfg, bodies, int(rec.StripeBytes), objectSize, onShardFault)
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

	rc, err := newECReconstructStreamReaderWithPrefetch(recCfg, readers, objectSize, func() {
		closeECShardReaders(shardReaders)
	}, onShardFault, onShardClean)
	if err != nil {
		r.attributeShardOpenError(rec, err)
		closeECShardReaders(shardReaders)
		return nil, err
	}
	// rc.Close owns shard-reader teardown (detached, after the background
	// prefetch producers stop reading — see newECReconstructStreamReaderWithPrefetch),
	// so no separate closeECShardReaders here.
	return rc, nil
}

// ReadAt reads len(buf) bytes at offset within the EC object without
// reconstructing the full object.
func (r ecObjectReader) ReadAt(ctx context.Context, bucket, shardKey string, rec PlacementRecord, objectSize, offset int64, buf []byte) (int, error) {
	if rec.StripeBytes > 0 {
		// Interleaved layout: the contiguous offset→shard mapping below is wrong.
		// Delegate to a bounded stream-fallback that de-interleaves on the fly.
		return r.readAtStripedStreaming(ctx, bucket, shardKey, rec, objectSize, offset, buf)
	}
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

// readAtStripedStreaming serves a Range read of a stripe-interleaved object by
// opening the bounded de-interleave stream, discarding `offset` bytes, and
// filling buf. The stream reader buffers at most one stripe, so memory stays
// bounded. (The offset→stripe seek optimization is a deferred follow-up.)
func (r ecObjectReader) readAtStripedStreaming(ctx context.Context, bucket, shardKey string, rec PlacementRecord, objectSize, offset int64, buf []byte) (int, error) {
	rc, err := r.OpenObject(ctx, bucket, shardKey, rec, objectSize)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	if offset > 0 {
		if _, err := io.CopyN(io.Discard, rc, offset); err != nil {
			return 0, fmt.Errorf("striped range seek: %w", err)
		}
	}
	want := len(buf)
	if rem := objectSize - offset; int64(want) > rem {
		want = int(rem)
	}
	n, err := io.ReadFull(rc, buf[:want])
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		err = nil
	}
	return n, err
}

// demotedNode reports whether reads should try other shards before this node:
// it is hot (BoundedLoads) or in an active unhealthy cooldown. Local shards
// are never health-demoted (self is not tracked by peerHealth).
func (r ecObjectReader) demotedNode(node string) bool {
	if r.bl != nil && r.bl.IsHot(node) {
		return true
	}
	if r.peerHealth != nil && node != r.selfID && !r.peerHealth.IsHealthy(node) {
		return true
	}
	return false
}

// orderFallbackByDemotion partitions a fallback idx list so non-demoted
// entries come first: openShardReaders opens fallback shards in listed order
// when a primary open fails, so an unhealthy (cooldown) or hot shard must not
// be attempted before a healthy one just because its index is lower. The
// partition is stable, so an ascending input stays ascending within each
// class (determinism preserved).
func (r ecObjectReader) orderFallbackByDemotion(rec PlacementRecord, idxs []int) []int {
	ordered := make([]int, 0, len(idxs))
	var demoted []int
	for _, i := range idxs {
		if r.demotedNode(rec.Nodes[i]) {
			demoted = append(demoted, i)
		} else {
			ordered = append(ordered, i)
		}
	}
	return append(ordered, demoted...)
}

// computeAttemptOrder returns primary and fallback shard idx lists.
// Demoted (hot or unhealthy-peer) data shards are swapped 1:1 with the lowest
// non-demoted parity idx. Both lists are sorted ascending for determinism,
// with the fallback list additionally partitioned non-demoted-first (see
// orderFallbackByDemotion).
// More demoted data shards than usable parity → bypass (metric incremented).
// bl == nil && peerHealth == nil → legacy: primary=dataIdx, fallback=parityIdx.
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

	if r.bl == nil && r.peerHealth == nil {
		return dataIdx, parityIdx
	}
	var demotedData []int
	for _, i := range dataIdx {
		if r.demotedNode(rec.Nodes[i]) {
			demotedData = append(demotedData, i)
		}
	}
	if len(demotedData) == 0 {
		// No swap needed, but the fallback still opens in listed order on a
		// primary failure: put non-demoted parity first.
		return dataIdx, r.orderFallbackByDemotion(rec, parityIdx)
	}
	var candidates []int // parity idx whose node is itself not demoted
	for _, i := range parityIdx {
		if !r.demotedNode(rec.Nodes[i]) {
			candidates = append(candidates, i)
		}
	}
	if len(demotedData) > len(candidates) {
		metrics.ClusterBLBypassedReads.Inc()
		return dataIdx, parityIdx
	}

	// Swap demotedData[:n] with candidates[:n] (n = len(demotedData)).
	parityIn := candidates[:len(demotedData)]
	primarySet := map[int]struct{}{}
	for _, i := range dataIdx {
		primarySet[i] = struct{}{}
	}
	for _, i := range demotedData {
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
	for _, i := range demotedData {
		fallbackSet[i] = struct{}{}
	}
	for i := range fallbackSet {
		fallback = append(fallback, i)
	}
	sort.Ints(fallback)
	// Leftover non-demoted parity (candidates beyond the swapped-in prefix)
	// must be attempted before the demoted entries sharing the list.
	fallback = r.orderFallbackByDemotion(rec, fallback)

	// Metric: per-demoted-data-node rerank counter.
	for _, i := range demotedData {
		metrics.ClusterBLRerankedReads.WithLabelValues(rec.Nodes[i]).Inc()
	}
	return primary, fallback
}

// notEnoughShardsErr builds the shared "not enough shards available" error used
// by the streaming read paths.
func notEnoughShardsErr(available, total, need int) error {
	return fmt.Errorf("ec get: only %d/%d shards available, need %d", available, total, need)
}

// openShardReaders opens streaming readers for the shards needed to reconstruct
// the object. All reads use direct streaming connections regardless of object size.
func (r ecObjectReader) openShardReaders(ctx context.Context, bucket, shardKey string, rec PlacementRecord) (ECConfig, []io.ReadCloser, error) {
	recCfg := rec.ECConfigOrFallback(r.ecConfig)
	if len(rec.Nodes) != recCfg.NumShards() {
		return ECConfig{}, nil, fmt.Errorf("placement length %d != expected %d", len(rec.Nodes), recCfg.NumShards())
	}
	if r.shards == nil {
		return ECConfig{}, nil, fmt.Errorf("shard service unavailable")
	}

	shardReaders := make([]io.ReadCloser, len(rec.Nodes))
	available := 0
	openShard := func(i int) bool {
		readamp.RecordECShard(shardCacheKey(bucket, shardKey, i))
		ep := r.endpointFor(rec.Nodes[i])
		if ep.IsLocal() {
			rc, err := ep.OpenShardStream(ctx, bucket, shardKey, i)
			if err != nil {
				return false
			}
			shardReaders[i] = rc
			available++
			return true
		}

		shardCtx, shardCancel := context.WithTimeout(ctx, shardRPCTimeout)
		rc, err := ep.OpenShardStream(shardCtx, bucket, shardKey, i)
		if err != nil {
			shardCancel()
			return false
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
	// is attempted first (see computeAttemptOrder).
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
		return ECConfig{}, nil, notEnoughShardsErr(available, len(rec.Nodes), recCfg.DataShards)
	}
	return recCfg, shardReaders, nil
}

// readDataShardAt reads len(buf) bytes at shardOffset within a single data
// shard. The range-result cache is checked first; on a miss the remote streaming
// RPC is used and the result is cached.
func (r ecObjectReader) readDataShardAt(ctx context.Context, bucket, shardKey, node string, shardIdx int, shardOffset int64, buf []byte) (int, error) {
	readamp.RecordECShard(shardCacheKey(bucket, shardKey, shardIdx))
	rangeCacheKey := ""
	if r.cache != nil && len(buf) > 0 {
		rangeCacheKey = shardRangeCacheKey(bucket, shardKey, shardIdx, shardOffset, int64(len(buf)))
		if data, ok := r.cache.Get(rangeCacheKey); ok {
			return copy(buf, data), nil
		}
	}
	ep := r.endpointFor(node)
	if ep.IsLocal() {
		return ep.ReadShardAt(ctx, bucket, shardKey, shardIdx, shardHeaderSize+shardOffset, buf)
	}

	shardCtx, shardCancel := context.WithTimeout(ctx, shardRPCTimeout)
	defer shardCancel()
	n, err := ep.ReadShardAt(shardCtx, bucket, shardKey, shardIdx, shardHeaderSize+shardOffset, buf)
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

func closeECShardReaders(shards []io.ReadCloser) {
	for _, shard := range shards {
		if shard != nil {
			_ = shard.Close()
		}
	}
}
