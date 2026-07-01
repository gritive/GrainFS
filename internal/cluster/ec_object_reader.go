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

// ReadObject reconstructs the full object from EC shards and returns it as a
// byte slice. shardKey is the on-disk path (key or key+"/"+versionID).
func (r ecObjectReader) ReadObject(ctx context.Context, bucket, shardKey string, rec PlacementRecord) ([]byte, error) {
	recCfg, shards, err := r.readShards(ctx, bucket, shardKey, rec)
	if err != nil {
		return nil, err
	}
	if rec.StripeBytes > 0 {
		// Stripe-interleaved objects: ecReconstructBodies yields the same
		// origSize (first present shard's header) and header-stripped bodies
		// that ECReconstruct uses, so de-interleave matches the contiguous
		// path's size source exactly.
		origSize, bodies, err := ecReconstructBodies(recCfg, shards)
		if err != nil {
			return nil, err
		}
		if origSize == 0 {
			return []byte{}, nil
		}
		return stripeDeinterleave(recCfg, bodies, rec.StripeBytes, origSize)
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

	if rec.StripeBytes > 0 {
		// Stripe-interleaved objects: the contiguous stream path strips the
		// 8-byte shard header inside ecReconstructStreamBodies, which the
		// de-interleave path bypasses. Skip the header on each present reader
		// (lazily, no eager blocking read) before handing bodies to the bounded
		// de-interleave reader. skipReader is not an io.Closer, so the stripe
		// reader's Close is a no-op on it — the underlying shardReaders are
		// closed exactly once by closeECShardReaders below.
		for i, shard := range readers {
			if shard != nil {
				readers[i] = &skipReader{r: shard, skip: shardHeaderSize}
			}
		}
		rc, err := newStripeDeinterleaveStreamReader(recCfg, readers, int(rec.StripeBytes), objectSize)
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

	rc, err := newECReconstructStreamReaderWithPrefetch(recCfg, readers)
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

// shardResult is one shard-read outcome flowing back from a fetch goroutine to
// the single-threaded consume loop.
type shardResult struct {
	idx      int
	data     []byte
	err      error
	peer     string // non-empty for remote reads; empty for local/selfID reads
	peerOK   bool   // true if remote read succeeded (only valid when peer != "")
	canceled bool   // true if k-of-n early-exit caused the cancellation error
}

// ecShardCollector holds the mutable shard-collection state for one readShards
// call. All mutation of shards/cached/available happens single-threaded in the
// consume loop (cachePrepass / applyResult); dispatched goroutines do I/O and
// channel-send ONLY and never touch these fields.
type ecShardCollector struct {
	r        ecObjectReader // endpointFor, cache, peerHealth, shards
	recCfg   ECConfig
	bucket   string
	shardKey string
	rec      PlacementRecord

	shards    [][]byte // output buffer (len == NumShards)
	cached    []bool
	available int
}

// cachePrepass satisfies shards from the cache to avoid disk/network I/O and
// returns true when K data shards are available (the read is fully served from
// cache). No-op when the reader has no cache.
//
// Cache-Get accounting (observable via readamp + cache spies) is split exactly
// as the original: when Peek already satisfies K, only the FIRST K of the Peeked
// indices are RecordECShard+Get-promoted; otherwise every Peeked index is
// promoted and then non-cached indices are Get-probed in order until K.
func (c *ecShardCollector) cachePrepass() bool {
	if c.r.cache == nil {
		return false
	}
	cachedIdx := make([]int, 0, len(c.rec.Nodes))
	for i := range c.rec.Nodes {
		if data, ok := c.r.cache.Peek(shardCacheKey(c.bucket, c.shardKey, i)); ok {
			c.shards[i] = data
			c.cached[i] = true
			cachedIdx = append(cachedIdx, i)
			c.available++
		}
	}
	if c.available >= c.recCfg.DataShards {
		for _, i := range cachedIdx[:c.recCfg.DataShards] {
			readamp.RecordECShard(shardCacheKey(c.bucket, c.shardKey, i))
			_, _ = c.r.cache.Get(shardCacheKey(c.bucket, c.shardKey, i))
		}
		return true
	}
	for _, i := range cachedIdx {
		readamp.RecordECShard(shardCacheKey(c.bucket, c.shardKey, i))
		_, _ = c.r.cache.Get(shardCacheKey(c.bucket, c.shardKey, i))
	}
	for i := range c.rec.Nodes {
		if !c.cached[i] {
			// A cache miss means this request will fall through to
			// ReadShard/ReadLocalShard for that shard.
			readamp.RecordECShard(shardCacheKey(c.bucket, c.shardKey, i))
			if data, ok := c.r.cache.Get(shardCacheKey(c.bucket, c.shardKey, i)); ok {
				c.shards[i] = data
				c.cached[i] = true
				c.available++
			}
			if c.available == c.recCfg.DataShards {
				return true
			}
		}
	}
	return false
}

// markPeerHealth records peer health for a remote read result. A result whose
// error is our own k-of-n early-exit cancellation (canceled==true) is skipped:
// the peer did not actually fail.
func (c *ecShardCollector) markPeerHealth(res shardResult) {
	if res.peer != "" && c.r.peerHealth != nil && !res.canceled {
		if res.peerOK {
			c.r.peerHealth.MarkHealthy(res.peer)
		} else {
			c.r.peerHealth.MarkUnhealthy(res.peer)
		}
	}
}

// applyResult marks peer health and, for a successful read, stores the shard
// (write-guarded so late results arriving after K is reached do not overwrite),
// populates the cache, and bumps available. Returns true on a successful read.
func (c *ecShardCollector) applyResult(res shardResult) bool {
	c.markPeerHealth(res)
	if res.err != nil || res.data == nil {
		return false
	}
	if c.available < c.recCfg.DataShards {
		c.shards[res.idx] = res.data
	}
	if c.r.cache != nil {
		c.r.cache.Put(shardCacheKey(c.bucket, c.shardKey, res.idx), res.data)
	}
	c.available++
	return true
}

// fetchShards fans out reads for the given shard indices and consumes results in
// a single-threaded loop. When stopAtK is set and K is reached, it cancels the
// remaining in-flight remote reads and drains their results (still marking peer
// health, but skipping peers canceled by our own cancel).
//
// The two-context structure is load-bearing: local reads run on the parent ctx
// and run to completion; remote reads run on a shardCtx derived from a
// cancelable readCtx, so cancel() aborts only in-flight remote reads and only
// remote results can be flagged canceled. resultCh is sized to len(indices) so a
// goroutine can always non-blocking-send even after an early-return drain.
func (c *ecShardCollector) fetchShards(ctx context.Context, indices []int, stopAtK bool) {
	if len(indices) == 0 || c.available >= c.recCfg.DataShards {
		return
	}
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan shardResult, len(indices))
	dispatched := 0
	for _, i := range indices {
		if c.cached[i] || c.shards[i] != nil {
			continue
		}
		i, node := i, c.rec.Nodes[i]
		if c.r.cache == nil {
			readamp.RecordECShard(shardCacheKey(c.bucket, c.shardKey, i))
		}
		dispatched++
		go func() {
			ep := c.r.endpointFor(node)
			var data []byte
			var err error
			var peer string
			var peerOK, canceled bool
			if ep.IsLocal() {
				data, err = ep.ReadShard(ctx, c.bucket, c.shardKey, i)
			} else {
				peer = node
				shardCtx, shardCancel := context.WithTimeout(readCtx, shardRPCTimeout)
				defer shardCancel()
				data, err = ep.ReadShard(shardCtx, c.bucket, c.shardKey, i)
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
		if c.applyResult(res) && stopAtK && c.available == c.recCfg.DataShards {
			cancel()
			for k := j + 1; k < dispatched; k++ {
				select {
				case drained := <-resultCh:
					c.markPeerHealth(drained)
				case <-ctx.Done():
					return
				}
			}
			return
		}
	}
}

// notEnoughShardsErr builds the shared "not enough shards available" error used
// by the buffered and streaming read paths.
func notEnoughShardsErr(available, total, need int) error {
	return fmt.Errorf("ec get: only %d/%d shards available, need %d", available, total, need)
}

// readShards collects the k-of-n data shards needed to reconstruct the object.
//
// Strategy: satisfy data shards first to avoid healthy-path parity fanout; fall
// back to parity shards only when data shards are unavailable. It is the thin
// orchestrator over an ecShardCollector: validate, cache pre-pass, then
// primary→fallback fan-out.
func (r ecObjectReader) readShards(ctx context.Context, bucket, shardKey string, rec PlacementRecord) (ECConfig, [][]byte, error) {
	recCfg := rec.ECConfigOrFallback(r.ecConfig)
	if len(rec.Nodes) != recCfg.NumShards() {
		return ECConfig{}, nil, fmt.Errorf("placement length %d != expected %d", len(rec.Nodes), recCfg.NumShards())
	}
	if r.shards == nil {
		return ECConfig{}, nil, fmt.Errorf("shard service unavailable")
	}

	c := ecShardCollector{
		r:        r,
		recCfg:   recCfg,
		bucket:   bucket,
		shardKey: shardKey,
		rec:      rec,
		shards:   make([][]byte, len(rec.Nodes)),
		cached:   make([]bool, len(rec.Nodes)),
	}

	// Cache pre-pass: satisfy from cache to avoid disk/network I/O.
	if c.cachePrepass() {
		return recCfg, c.shards, nil
	}

	// Determine primary and fallback shard indices, swapping hot data shards to
	// parity when BoundedLoads is active.
	primary, fallback := r.computeAttemptOrder(rec, recCfg)

	// Try data (primary) shards first; fall back to parity only when they do not
	// satisfy K. fetchShards no-ops once K is reached (its entry guard returns
	// when available >= DataShards), so the fallback call is a cheap early-out on
	// the healthy path.
	c.fetchShards(ctx, primary, false)
	c.fetchShards(ctx, fallback, true)
	if c.available < recCfg.DataShards {
		return ECConfig{}, nil, notEnoughShardsErr(c.available, len(rec.Nodes), recCfg.DataShards)
	}
	return recCfg, c.shards, nil
}

// openShardReaders opens streaming readers for the shards needed to reconstruct
// the object. All reads use direct streaming connections regardless of object size.
func (r ecObjectReader) openShardReaders(ctx context.Context, bucket, shardKey string, rec PlacementRecord, objectSize int64) (ECConfig, []io.ReadCloser, error) {
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

// skipReader discards the first `skip` bytes from the underlying reader before
// passing subsequent reads through. It skips lazily (no eager blocking read) so
// shard readers are not forced to materialize their header until first Read.
// It deliberately does NOT implement io.Closer: the underlying shard readers
// are owned and closed elsewhere (closeECShardReaders).
type skipReader struct {
	r    io.Reader
	skip int
}

func (s *skipReader) Read(p []byte) (int, error) {
	for s.skip > 0 {
		n := s.skip
		if n > len(p) {
			n = len(p)
		}
		m, err := s.r.Read(p[:n])
		s.skip -= m
		if err != nil {
			return 0, err
		}
	}
	return s.r.Read(p)
}

func closeECShardReaders(shards []io.ReadCloser) {
	for _, shard := range shards {
		if shard != nil {
			_ = shard.Close()
		}
	}
}
