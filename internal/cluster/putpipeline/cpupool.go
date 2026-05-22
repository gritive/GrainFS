package putpipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
)

// CPUPool runs worker goroutines that EC-split incoming stripes and
// AES-GCM-seal each shard chunk into GFSENC2 format.
type CPUPool struct {
	in       chan StripePlaintext
	enc      *encrypt.Encryptor
	ecCfg    cluster.ECConfig
	workers  int
	outByPut map[uint64][]chan<- EncryptedShardChunk // protected by mu
	mu       sync.RWMutex

	seqMu sync.Mutex
	seq   map[uint64]*seqGate
}

// seqGate serializes chunk dispatch per PUT so each shard channel sees
// stripes in ascending StripeIdx order even though workers process
// stripes concurrently.
type seqGate struct {
	lock       sync.Mutex
	cond       *sync.Cond
	nextStripe uint32
}

func (p *CPUPool) registerPut(putID uint64, shardChans []chan<- EncryptedShardChunk) {
	p.mu.Lock()
	p.outByPut[putID] = shardChans
	p.mu.Unlock()
}

func (p *CPUPool) unregisterPut(putID uint64) {
	p.mu.Lock()
	delete(p.outByPut, putID)
	p.mu.Unlock()
	p.seqMu.Lock()
	delete(p.seq, putID)
	p.seqMu.Unlock()
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

func (p *CPUPool) processStripe(ctx context.Context, stripe StripePlaintext) error {
	shards, err := cluster.ECSplitWithEncode(p.ecCfg, stripe.Data)
	if err != nil {
		return fmt.Errorf("cpu pool ec split: %w", err)
	}
	chunks := make([]EncryptedShardChunk, len(shards))
	for i, shard := range shards {
		ciphertext, err := p.sealShardChunk(stripe.PutID, i, shard)
		if err != nil {
			return fmt.Errorf("cpu pool seal shard %d: %w", i, err)
		}
		chunks[i] = EncryptedShardChunk{
			PutID:      stripe.PutID,
			StripeIdx:  stripe.StripeIdx,
			ShardIdx:   i,
			Ciphertext: ciphertext,
			Padding:    stripe.Padding,
			LastInPut:  stripe.LastInPut,
		}
	}
	p.dispatch(ctx, stripe.PutID, stripe.StripeIdx, chunks)
	return nil
}

// dispatch sends a stripe's chunks to the per-shard channels, holding
// the PUT's seqGate so chunks land in StripeIdx order on every shard.
func (p *CPUPool) dispatch(ctx context.Context, putID uint64, stripeIdx uint32, chunks []EncryptedShardChunk) {
	seq := p.acquireSeq(putID)
	seq.lock.Lock()
	defer seq.lock.Unlock()
	for seq.nextStripe != stripeIdx {
		seq.cond.Wait()
	}
	for i, chunk := range chunks {
		out, ok := p.shardChan(putID, i)
		if !ok {
			continue
		}
		select {
		case <-ctx.Done():
			seq.nextStripe++
			seq.cond.Broadcast()
			return
		case out <- chunk:
		}
	}
	seq.nextStripe++
	seq.cond.Broadcast()
}

// sealShardChunk encrypts one shard's plaintext bytes into a complete
// GFSENC2 chunk stream. Phase 5 will plumb the real bucket/shardKey
// AAD; for now a deterministic per-PUT-per-shard AAD is used.
func (p *CPUPool) sealShardChunk(putID uint64, shardIdx int, plain []byte) ([]byte, error) {
	aad := []byte(fmt.Sprintf("put/%d/shard/%d", putID, shardIdx))
	buf := newBuffer()
	defer releaseBuffer(buf)
	w, err := eccodec.NewEncryptedShardChunkedWriter(buf, p.enc, aad, eccodec.DefaultEncryptedChunkSize)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(plain); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}
