package putpipeline

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
)

// CPUPool runs worker goroutines that EC-split incoming stripes and
// AES-GCM-seal each shard into GFSENC2 format.
//
// EC split runs in parallel across workers. Encryption runs inside
// dispatch, which the per-PUT seqGate already serializes, so each
// shard's chunked writer is fed strictly in stripe order. The result
// is that all chunks for one (PutID, ShardIdx), concatenated in stripe
// order, form exactly one valid GFSENC2 stream.
type CPUPool struct {
	in       chan StripePlaintext
	enc      *encrypt.Encryptor
	ecCfg    cluster.ECConfig
	workers  int
	outByPut map[uint64][]chan<- EncryptedShardChunk // protected by mu
	mu       sync.RWMutex

	seqMu sync.Mutex
	seq   map[uint64]*seqGate

	encMu    sync.Mutex
	encByPut map[uint64][]*shardEncoder
}

// shardEncoder bundles the single long-lived chunked writer for one
// (PutID, shardIdx) with the buffer it emits into. dispatch drains buf
// after every stripe; the writer itself spans the whole PUT.
type shardEncoder struct {
	w   *eccodec.EncryptedShardChunkedWriter
	buf *bytes.Buffer
}

// seqGate serializes chunk dispatch per PUT so each shard channel sees
// stripes in ascending StripeIdx order even though workers process
// stripes concurrently.
type seqGate struct {
	lock       sync.Mutex
	cond       *sync.Cond
	nextStripe uint32
}

// registerPut sets up per-shard chunked writers for one PUT.
// bucket and shardKey are used to derive the AAD so that shards are
// readable by the legacy ShardService reader (AAD = bucket+"/"+shardKey+"/"+shardIdx).
func (p *CPUPool) registerPut(putID uint64, bucket, shardKey string, shardChans []chan<- EncryptedShardChunk) {
	p.mu.Lock()
	p.outByPut[putID] = shardChans
	p.mu.Unlock()

	n := p.ecCfg.NumShards()
	encoders := make([]*shardEncoder, n)
	for i := 0; i < n; i++ {
		buf := new(bytes.Buffer)
		aad := []byte(bucket + "/" + shardKey + "/" + strconv.Itoa(i))
		w, err := eccodec.NewEncryptedShardChunkedWriter(buf, p.enc, aad, eccodec.DefaultEncryptedChunkSize)
		if err != nil {
			// NewEncryptedShardChunkedWriter only fails on a nil
			// encryptor or an out-of-range chunk size, both of which
			// are pipeline-construction bugs, not runtime conditions.
			panic(fmt.Sprintf("cpu pool: build shard encoder %d for put %d: %v", i, putID, err))
		}
		encoders[i] = &shardEncoder{w: w, buf: buf}
	}
	p.encMu.Lock()
	if p.encByPut == nil {
		p.encByPut = make(map[uint64][]*shardEncoder)
	}
	p.encByPut[putID] = encoders
	p.encMu.Unlock()
}

func (p *CPUPool) unregisterPut(putID uint64) {
	p.mu.Lock()
	delete(p.outByPut, putID)
	p.mu.Unlock()
	p.seqMu.Lock()
	delete(p.seq, putID)
	p.seqMu.Unlock()
	p.encMu.Lock()
	delete(p.encByPut, putID)
	p.encMu.Unlock()
}

func (p *CPUPool) shardEncoders(putID uint64) ([]*shardEncoder, bool) {
	p.encMu.Lock()
	defer p.encMu.Unlock()
	encoders, ok := p.encByPut[putID]
	return encoders, ok
}

func (p *CPUPool) shardChan(putID uint64, shardIdx int) (chan<- EncryptedShardChunk, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	chans, ok := p.outByPut[putID]
	if !ok || shardIdx >= len(chans) {
		return nil, false
	}
	return chans[shardIdx], true
}

func (p *CPUPool) acquireSeq(putID uint64) *seqGate {
	p.seqMu.Lock()
	defer p.seqMu.Unlock()
	if p.seq == nil {
		p.seq = make(map[uint64]*seqGate)
	}
	g, ok := p.seq[putID]
	if !ok {
		g = &seqGate{}
		g.cond = sync.NewCond(&g.lock)
		p.seq[putID] = g
	}
	return g
}

// Run launches worker goroutines until ctx is done.
func (p *CPUPool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.workerLoop(ctx)
		}()
	}
	wg.Wait()
}

func (p *CPUPool) workerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case stripe, ok := <-p.in:
			if !ok {
				return
			}
			if err := p.processStripe(ctx, stripe); err != nil {
				// Phase 4 wires a failure path to CommitCoord. For now
				// the error is dropped; CommitCoord will time out the
				// PUT when shard results never arrive.
				_ = err
			}
		}
	}
}

// processStripe runs concurrently across workers and does only the EC
// split: it produces k+m plaintext shard byte slices. Encryption is
// deferred to dispatch so it runs serialized per PUT.
//
// IngestActor zero-pads the last stripe to stripeBytes for Reed-Solomon
// alignment. We trim the padding before EC split so the embedded shard
// header records the real byte count — the EC reader uses that header to
// limit the reconstructed stream to the correct length.
func (p *CPUPool) processStripe(ctx context.Context, stripe StripePlaintext) error {
	data := stripe.Data
	if stripe.Padding > 0 && int(stripe.Padding) < len(data) {
		data = data[:len(data)-int(stripe.Padding)]
	}
	shards, err := cluster.ECSplitWithEncode(p.ecCfg, data)
	if err != nil {
		return fmt.Errorf("cpu pool ec split: %w", err)
	}
	return p.dispatch(ctx, stripe, shards)
}

// dispatch encrypts and sends a stripe's shards to the per-shard
// channels, holding the PUT's seqGate so each shard's chunked writer is
// fed in StripeIdx order and chunks land in order on every channel.
//
// Because the seqGate admits exactly one stripe per PUT at a time, the
// long-lived chunked writers need no extra locking: every Write/Close
// across the whole PUT happens here, strictly in stripe order.
func (p *CPUPool) dispatch(ctx context.Context, stripe StripePlaintext, shards [][]byte) error {
	seq := p.acquireSeq(stripe.PutID)
	seq.lock.Lock()
	defer seq.lock.Unlock()
	for seq.nextStripe != stripe.StripeIdx {
		seq.cond.Wait()
	}
	defer func() {
		seq.nextStripe++
		seq.cond.Broadcast()
	}()

	encoders, ok := p.shardEncoders(stripe.PutID)
	if !ok {
		return fmt.Errorf("cpu pool: no shard encoders for put %d", stripe.PutID)
	}
	for i, shard := range shards {
		if i >= len(encoders) {
			break
		}
		enc := encoders[i]
		if _, err := enc.w.Write(shard); err != nil {
			return fmt.Errorf("cpu pool: encrypt put %d shard %d: %w", stripe.PutID, i, err)
		}
		if stripe.LastInPut {
			if err := enc.w.Close(); err != nil {
				return fmt.Errorf("cpu pool: close put %d shard %d: %w", stripe.PutID, i, err)
			}
		}
		// buf.Bytes() aliases the buffer's backing array; copy out
		// before Reset so the next stripe cannot corrupt this chunk.
		ciphertext := make([]byte, enc.buf.Len())
		copy(ciphertext, enc.buf.Bytes())
		enc.buf.Reset()

		out, ok := p.shardChan(stripe.PutID, i)
		if !ok {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- EncryptedShardChunk{
			PutID:      stripe.PutID,
			StripeIdx:  stripe.StripeIdx,
			ShardIdx:   i,
			Ciphertext: ciphertext,
			Padding:    stripe.Padding,
			LastInPut:  stripe.LastInPut,
		}:
		}
	}
	return nil
}
