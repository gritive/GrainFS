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

// ciphertextBufPool recycles the per-stripe ciphertext slices that
// flow from CPUPool.deliverStripe to DriveActor.handle. Without it,
// every (stripe, shard) pair allocated a fresh slice ≈ stripe/k bytes
// + AEAD overhead — about 32% of all PUT-path allocations on warp.
//
// Ownership: deliverStripe acquires via getCiphertextBuf and stamps
// the slice into EncryptedShardChunk.Ciphertext; DriveActor.handle
// returns it via putCiphertextBuf on every exit path (defer).
var ciphertextBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1<<20) // 1 MiB initial; grows on demand
		return &b
	},
}

// ciphertextMaxPooled bounds the capacity we'll retain across PUTs.
// Anything bigger goes straight to the GC so we don't keep huge
// outliers (e.g. abnormal stripe sizes) pinned in the pool.
const ciphertextMaxPooled = 16 << 20

func getCiphertextBuf(size int) []byte {
	bp := ciphertextBufPool.Get().(*[]byte)
	b := *bp
	if cap(b) >= size {
		return b[:size]
	}
	// Pool's slice too small; let it go and allocate fresh.
	return make([]byte, size)
}

func putCiphertextBuf(b []byte) {
	if b == nil || cap(b) > ciphertextMaxPooled {
		return
	}
	b = b[:0]
	ciphertextBufPool.Put(&b)
}

// CPUPool runs worker goroutines that EC-split incoming stripes. Each
// PUT owns a dedicated dispatcher goroutine that reorders the workers'
// outputs by StripeIdx, encrypts them through the per-shard chunked
// writer, and forwards EncryptedShardChunk on each shard channel.
//
// EC split runs in parallel across workers; encryption + chunked-writer
// Write + shard send run serially inside the per-PUT dispatcher. No
// shared seqGate, no broadcast wakeups.
type CPUPool struct {
	in      chan StripePlaintext
	enc     *encrypt.Encryptor
	ecCfg   cluster.ECConfig
	workers int

	puts       sync.Map // key: uint64 putID, val: *putDispatchState
	dispatchWG sync.WaitGroup
}

// shardEncoder bundles the single long-lived chunked writer for one
// (PutID, shardIdx) with the buffer it emits into. The per-PUT
// dispatcher owns the slice for its lifetime; no locking needed.
type shardEncoder struct {
	w   *eccodec.EncryptedShardChunkedWriter
	buf *bytes.Buffer
}

// putDispatchState is per-PUT state. It is owned by the dispatcher
// goroutine; workers only push split results into inbox.
type putDispatchState struct {
	encoders   []*shardEncoder
	shardChans []chan<- EncryptedShardChunk
	inbox      chan dispatchMsg
}

// dispatchMsg carries the result of EC split from a worker to the
// per-PUT dispatcher. The dispatcher reorders by stripeIdx.
type dispatchMsg struct {
	stripeIdx uint32
	shards    [][]byte
	padding   uint32
	lastInPut bool
}

// registerPut sets up per-shard chunked writers + the dispatcher
// goroutine for one PUT. bucket and shardKey are used to derive the
// AAD so that shards are readable by the legacy ShardService reader
// (AAD = bucket+"/"+shardKey+"/"+shardIdx). totalSize is written once
// at the start of every shard's plaintext stream as the 8-byte shard
// size header so the EC reader knows the exact byte count across all
// stripes. Per-stripe ECSplitRaw never re-emits the header.
func (p *CPUPool) registerPut(putID uint64, bucket, shardKey string, totalSize int64, shardChans []chan<- EncryptedShardChunk) {
	n := p.ecCfg.NumShards()
	encoders := make([]*shardEncoder, n)
	header := cluster.ShardHeader(totalSize)
	for i := 0; i < n; i++ {
		buf := new(bytes.Buffer)
		aad := []byte(bucket + "/" + shardKey + "/" + strconv.Itoa(i))
		w, err := eccodec.NewEncryptedShardChunkedWriter(buf, p.enc, aad, eccodec.DefaultEncryptedChunkSize)
		if err != nil {
			panic(fmt.Sprintf("cpu pool: build shard encoder %d for put %d: %v", i, putID, err))
		}
		if _, err := w.Write(header[:]); err != nil {
			panic(fmt.Sprintf("cpu pool: write shard header %d for put %d: %v", i, putID, err))
		}
		encoders[i] = &shardEncoder{w: w, buf: buf}
	}

	inboxCap := p.workers
	if inboxCap < 4 {
		inboxCap = 4
	}
	ps := &putDispatchState{
		encoders:   encoders,
		shardChans: shardChans,
		inbox:      make(chan dispatchMsg, inboxCap),
	}
	p.puts.Store(putID, ps)

	p.dispatchWG.Add(1)
	go p.runDispatcher(putID, ps)
}

func (p *CPUPool) unregisterPut(putID uint64) {
	v, ok := p.puts.LoadAndDelete(putID)
	if !ok {
		return
	}
	ps := v.(*putDispatchState)
	close(ps.inbox)
	// runDispatcher drains and exits; no need to wait here because
	// Pipeline.Put has already received earlyAck (drives wrote K shards),
	// which means every stripe passed through the dispatcher already.
}

func (p *CPUPool) loadPut(putID uint64) (*putDispatchState, bool) {
	v, ok := p.puts.Load(putID)
	if !ok {
		return nil, false
	}
	return v.(*putDispatchState), true
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
	// Wait for any per-PUT dispatcher goroutines that are still
	// draining their inbox (unregisterPut closes the inbox; the
	// dispatcher drains then exits).
	p.dispatchWG.Wait()
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
				_ = err
			}
		}
	}
}

// processStripe runs concurrently across workers and does only the EC
// split. The split result is forwarded to the PUT's dispatcher inbox.
//
// IngestActor zero-pads the last stripe to stripeBytes for Reed-Solomon
// alignment. We trim the padding before EC split so each shard receives
// only real bytes — the per-PUT shard header tells the reader where to stop.
func (p *CPUPool) processStripe(ctx context.Context, stripe StripePlaintext) error {
	data := stripe.Data
	if stripe.Padding > 0 && int(stripe.Padding) < len(data) {
		data = data[:len(data)-int(stripe.Padding)]
	}
	shards, err := cluster.ECSplitRaw(p.ecCfg, data)
	if err != nil {
		return fmt.Errorf("cpu pool ec split: %w", err)
	}

	ps, ok := p.loadPut(stripe.PutID)
	if !ok {
		return nil
	}
	msg := dispatchMsg{
		stripeIdx: stripe.StripeIdx,
		shards:    shards,
		padding:   stripe.Padding,
		lastInPut: stripe.LastInPut,
	}
	select {
	case ps.inbox <- msg:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// runDispatcher reorders incoming dispatchMsgs by stripeIdx, writes the
// shards through each shard's long-lived chunked writer (in order),
// then forwards each shard's encrypted ciphertext to its shardChan.
//
// The dispatcher exits when its inbox is closed (unregisterPut) AND
// the pending map is empty.
func (p *CPUPool) runDispatcher(putID uint64, ps *putDispatchState) {
	defer p.dispatchWG.Done()

	var expected uint32
	pending := make(map[uint32]dispatchMsg)
	for msg := range ps.inbox {
		pending[msg.stripeIdx] = msg
		for {
			cur, ok := pending[expected]
			if !ok {
				break
			}
			delete(pending, expected)
			if err := p.deliverStripe(putID, ps, cur); err != nil {
				_ = err
			}
			expected++
		}
	}
}

// deliverStripe writes one stripe's shards through each shard's chunked
// writer, drains the writer's buffer for each shard, and sends the
// resulting EncryptedShardChunk on the shard's channel. Called by the
// per-PUT dispatcher in strict stripeIdx order, so the chunked writer
// state is consistent without any external locking.
func (p *CPUPool) deliverStripe(putID uint64, ps *putDispatchState, msg dispatchMsg) error {
	for i, shard := range msg.shards {
		if i >= len(ps.encoders) {
			break
		}
		enc := ps.encoders[i]
		if _, err := enc.w.Write(shard); err != nil {
			return fmt.Errorf("cpu pool: encrypt put %d shard %d: %w", putID, i, err)
		}
		if msg.lastInPut {
			if err := enc.w.Close(); err != nil {
				return fmt.Errorf("cpu pool: close put %d shard %d: %w", putID, i, err)
			}
		}
		ciphertext := getCiphertextBuf(enc.buf.Len())
		copy(ciphertext, enc.buf.Bytes())
		enc.buf.Reset()

		if i >= len(ps.shardChans) {
			continue
		}
		ps.shardChans[i] <- EncryptedShardChunk{
			PutID:      putID,
			StripeIdx:  msg.stripeIdx,
			ShardIdx:   i,
			Ciphertext: ciphertext,
			Padding:    msg.padding,
			LastInPut:  msg.lastInPut,
		}
	}
	return nil
}
