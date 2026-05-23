package putpipeline

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

// Config bundles the wiring a Pipeline needs.
type Config struct {
	DataDirs     []string
	Encryptor    *encrypt.Encryptor
	ECConfig     cluster.ECConfig
	StripeBytes  int           // k * blockSize; defaults to 1<<20 (1 MiB) if 0
	ChannelDepth int           // per-actor channel cap; defaults to 8 if 0
	BatchSize    int           // metadata batch size; defaults to 32 if 0
	FlushAfter   time.Duration // metadata flush deadline; defaults to 5 ms if 0
	BadgerDB     *badger.DB    // metadata sink; nil = no-op flush (tests)
}

// Pipeline owns the long-lived actors and dispatches PUT requests.
type Pipeline struct {
	cfg         Config
	nextPutID   atomic.Uint64
	cpu         *CPUPool
	drives      []*DriveActor
	commit      *CommitCoord
	metaBatcher *MetadataBatcher
	stripeCh    chan StripePlaintext
	driveIns    []chan EncryptedShardChunk
	commitIn    chan ShardWriteResult
	metaIn      chan MetadataRecord
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

// New constructs and starts a Pipeline.
func New(cfg Config) *Pipeline {
	if cfg.StripeBytes == 0 {
		cfg.StripeBytes = 1 << 20
	}
	if cfg.ChannelDepth == 0 {
		cfg.ChannelDepth = 8
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 32
	}
	if cfg.FlushAfter == 0 {
		cfg.FlushAfter = 5 * time.Millisecond
	}
	if cfg.ECConfig.NumShards() == 0 {
		// Defensive default: 1+0 single-shard. Real deployments must
		// pass a real ECConfig; tests using this default will only have
		// one drive's worth of shards.
		cfg.ECConfig = cluster.ECConfig{DataShards: 1, ParityShards: 0}
	}

	p := &Pipeline{cfg: cfg}
	p.ctx, p.cancel = context.WithCancel(context.Background())

	p.stripeCh = make(chan StripePlaintext, cfg.ChannelDepth)
	p.commitIn = make(chan ShardWriteResult, cfg.ChannelDepth)
	p.metaIn = make(chan MetadataRecord, cfg.ChannelDepth)
	p.driveIns = make([]chan EncryptedShardChunk, len(cfg.DataDirs))
	p.drives = make([]*DriveActor, len(cfg.DataDirs))
	for i := range cfg.DataDirs {
		p.driveIns[i] = make(chan EncryptedShardChunk, cfg.ChannelDepth)
		p.drives[i] = &DriveActor{
			in:       p.driveIns[i],
			dataDir:  cfg.DataDirs[i],
			commitCh: p.commitIn,
			pending:  make(map[uint64]*shardWriteState),
		}
	}
	p.cpu = &CPUPool{
		in:       p.stripeCh,
		enc:      cfg.Encryptor,
		ecCfg:    cfg.ECConfig,
		workers:  runtime.GOMAXPROCS(0),
		outByPut: make(map[uint64][]chan<- EncryptedShardChunk),
	}
	p.commit = &CommitCoord{
		in:          p.commitIn,
		metaBatchCh: p.metaIn,
		waiters:     make(map[uint64]*putWaiter),
	}
	p.metaBatcher = &MetadataBatcher{
		in:         p.metaIn,
		db:         cfg.BadgerDB,
		batchSize:  cfg.BatchSize,
		flushAfter: cfg.FlushAfter,
	}

	p.wg.Add(3 + len(p.drives))
	go func() { defer p.wg.Done(); p.cpu.Run(p.ctx) }()
	go func() { defer p.wg.Done(); p.commit.Run(p.ctx) }()
	go func() { defer p.wg.Done(); p.metaBatcher.Run(p.ctx) }()
	for _, d := range p.drives {
		d := d
		go func() { defer p.wg.Done(); d.Run(p.ctx) }()
	}
	return p
}

// Put dispatches one PUT through the pipeline. Blocks until K data-shard
// quorum is satisfied (early-ack). Parity shards and metadata commit
// complete in the background.
func (p *Pipeline) Put(ctx context.Context, req PutRequest) (*storage.Object, error) {
	if p.cfg.ECConfig.NumShards() == 0 {
		return nil, fmt.Errorf("pipeline: zero-shard EC config")
	}
	putID := p.nextPutID.Add(1)

	// Map each shard index to its target drive (round-robin by index).
	// CPUPool's per-shard fan-out for shard i points at the DriveActor's
	// long-lived in channel — the DriveActor demultiplexes by PutID via
	// its pending+registry maps.
	numShards := p.cfg.ECConfig.NumShards()
	shardChans := make([]chan<- EncryptedShardChunk, numShards)
	for i := 0; i < numShards; i++ {
		driveIdx := i % len(p.driveIns)
		shardChans[i] = p.driveIns[driveIdx]
		// registerPut must happen BEFORE the first chunk for (PutID, shardIdx)
		// arrives on the DriveActor's channel.
		p.drives[driveIdx].registerPut(putID, req.Bucket, req.Key, i)
	}
	p.cpu.registerPut(putID, shardChans)
	defer p.cpu.unregisterPut(putID)

	earlyAck := make(chan error, 1)
	finalDone := make(chan error, 1)
	p.commit.registerPut(putID, &putWaiter{
		shardsTotal: numShards,
		cfg:         p.cfg.ECConfig,
		earlyAck:    earlyAck,
		finalDone:   finalDone,
		metadata: MetadataRecord{
			Bucket:   req.Bucket,
			Key:      req.Key,
			System:   req.System,
			UserMeta: req.UserMeta,
		},
	})

	// Run the IngestActor for this PUT.
	type ingestResult struct {
		etag  string
		total int64
		err   error
	}
	ingestCh := make(chan ingestResult, 1)
	ingest := &IngestActor{out: p.stripeCh, stripeBytes: p.cfg.StripeBytes}
	go func() {
		etag, total, err := ingest.Run(ctx, putID, req.Bucket, req.Body)
		ingestCh <- ingestResult{etag: etag, total: total, err: err}
	}()

	// Wait for K-quorum ack or context cancellation.
	select {
	case err := <-earlyAck:
		if err != nil {
			<-ingestCh
			return nil, err
		}
	case <-ctx.Done():
		<-ingestCh
		return nil, ctx.Err()
	}

	// CommitCoord can only signal K data shards after every stripe has
	// been dispatched through CPUPool, so IngestActor has finished by now.
	res := <-ingestCh
	if res.err != nil {
		return nil, res.err
	}

	obj := &storage.Object{
		Key:          req.Key,
		Size:         res.total,
		ETag:         res.etag,
		ContentType:  req.ContentType,
		UserMetadata: req.UserMeta,
		LastModified: time.Now().Unix(),
	}

	// Wait for parity finalization in the background. Errors are noted
	// for future metrics wiring (Phase 5.5+).
	go func() {
		select {
		case <-finalDone:
		case <-time.After(30 * time.Second):
		}
	}()
	return obj, nil
}

// Shutdown cancels all actor goroutines and waits for them to drain.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	p.cancel()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("pipeline shutdown timeout: %w", ctx.Err())
	case <-time.After(10 * time.Second):
		return fmt.Errorf("pipeline shutdown timeout (10s)")
	}
}

// PeekPendingMetadata exposes the MetadataBatcher pending map so GET
// callers can preserve read-after-write semantics while a batch is in
// flight. Phase 5.5 wires this into the GET path.
func (p *Pipeline) PeekPendingMetadata(bucket, key, versionID string) (MetadataRecord, bool) {
	return p.metaBatcher.PeekPending(bucket, key, versionID)
}
