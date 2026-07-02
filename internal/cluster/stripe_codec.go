package cluster

import (
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/klauspost/reedsolomon"
)

// stripeFragSize: per-shard fragment for a stripe holding stripeDataLen bytes under k data shards.
func stripeFragSize(stripeDataLen, k int) int {
	return (stripeDataLen + k - 1) / k
}

func numStripes(objectSize int64, stripeBytes int) int {
	if stripeBytes <= 0 {
		return 0
	}
	return int((objectSize + int64(stripeBytes) - 1) / int64(stripeBytes))
}

// validateStripeObjectSize rejects object sizes the stripe codec cannot
// address with int arithmetic (negative, or beyond the platform int range).
func validateStripeObjectSize(objectSize int64) error {
	if objectSize < 0 || objectSize > math.MaxInt {
		return fmt.Errorf("stripe: object size %d outside int range", objectSize)
	}
	return nil
}

// stripeReconstructShardBody regenerates ONE missing shard's interleaved body
// (no 8-byte header) directly from its surviving siblings, per stripe. bodies
// holds the header-stripped shard bodies (nil = missing); exactly the shard at
// shardIdx is regenerated. It slices each surviving sibling at the same
// per-stripe fragment offsets the de-interleave reader uses and runs the
// Reed-Solomon Reconstruct on those fragments. Because it operates on the
// siblings' actual on-disk fragments (not on a re-split of the reconstructed
// object), the regenerated fragments are byte-identical to what the pipeline
// wrote, so the repaired shard matches its interleaved siblings.
func stripeReconstructShardBody(cfg ECConfig, bodies [][]byte, stripeBytes, shardIdx int, objectSize int64) ([]byte, error) {
	k, n := cfg.DataShards, cfg.NumShards()
	if len(bodies) != n {
		return nil, fmt.Errorf("stripe reconstruct shard: got %d shards, want %d", len(bodies), n)
	}
	if shardIdx < 0 || shardIdx >= n {
		return nil, fmt.Errorf("stripe reconstruct shard: shardIdx %d out of range [0,%d)", shardIdx, n)
	}
	if stripeBytes <= 0 {
		return nil, fmt.Errorf("stripe reconstruct shard: stripeBytes must be > 0")
	}
	if err := validateStripeObjectSize(objectSize); err != nil {
		return nil, err
	}
	enc, err := getEncoder(cfg)
	if err != nil {
		return nil, fmt.Errorf("stripe reconstruct shard encoder: %w", err)
	}
	var body []byte
	stripes := numStripes(objectSize, stripeBytes)
	offset := 0
	remaining := int(objectSize)
	for s := 0; s < stripes; s++ {
		dS := stripeBytes
		if remaining < dS {
			dS = remaining
		}
		fragSize := stripeFragSize(dS, k)
		frags := make([][]byte, n)
		for i := 0; i < n; i++ {
			if bodies[i] == nil {
				continue
			}
			if offset+fragSize > len(bodies[i]) {
				return nil, fmt.Errorf("stripe reconstruct shard: shard %d short at stripe %d (have %d need %d)", i, s, len(bodies[i]), offset+fragSize)
			}
			frags[i] = append([]byte(nil), bodies[i][offset:offset+fragSize]...)
		}
		if err := enc.Reconstruct(frags); err != nil {
			return nil, fmt.Errorf("stripe reconstruct shard stripe %d: %w", s, err)
		}
		body = append(body, frags[shardIdx]...)
		offset += fragSize
		remaining -= dS
	}
	return body, nil
}

type stripeDeinterleaveStreamReader struct {
	cfg         ECConfig
	enc         reedsolomon.Encoder
	shards      []io.Reader // len K+M; nil = missing
	stripeBytes int
	stripe      int
	remaining   int
	buf         []byte
	bufOff      int
	fragScratch [][]byte
	err         error
	// onShardFault, if non-nil, fires once with the shard index when a
	// fragment read ends cleanly short (truncated shard), so the caller can
	// attribute the fault to the serving peer.
	onShardFault func(int)
}

func newStripeDeinterleaveStreamReader(cfg ECConfig, shards []io.Reader, stripeBytes int, objectSize int64, onShardFault func(int)) (io.ReadCloser, error) {
	n := cfg.NumShards()
	if len(shards) != n {
		return nil, fmt.Errorf("stripe stream: got %d shards, want %d", len(shards), n)
	}
	if stripeBytes <= 0 {
		return nil, fmt.Errorf("stripe stream: stripeBytes must be > 0")
	}
	if err := validateStripeObjectSize(objectSize); err != nil {
		return nil, err
	}
	enc, err := getEncoder(cfg)
	if err != nil {
		return nil, fmt.Errorf("stripe stream encoder: %w", err)
	}
	return &stripeDeinterleaveStreamReader{
		cfg: cfg, enc: enc, shards: shards, stripeBytes: stripeBytes,
		remaining: int(objectSize), fragScratch: make([][]byte, n),
		onShardFault: onShardFault,
	}, nil
}

func (r *stripeDeinterleaveStreamReader) fill() error {
	if r.remaining <= 0 {
		return io.EOF
	}
	k, n := r.cfg.DataShards, r.cfg.NumShards()
	dS := r.stripeBytes
	if r.remaining < dS {
		dS = r.remaining
	}
	fragSize := stripeFragSize(dS, k)
	missing := false
	for i := 0; i < n; i++ {
		if r.shards[i] == nil {
			r.fragScratch[i] = nil
			missing = true
			continue
		}
		if cap(r.fragScratch[i]) < fragSize {
			r.fragScratch[i] = make([]byte, fragSize)
		}
		r.fragScratch[i] = r.fragScratch[i][:fragSize]
		if n, err := io.ReadFull(r.shards[i], r.fragScratch[i]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// A fragment that ends cleanly short is a truncated shard, not
				// end-of-object: report it once so the caller can attribute the
				// fault to the serving peer, and surface the typed error.
				if r.onShardFault != nil {
					r.onShardFault(i)
					r.onShardFault = nil
				}
				err = &ecShardTruncatedError{Idx: i, Want: int64(fragSize), Got: int64(n)}
			}
			return fmt.Errorf("stripe stream read shard %d stripe %d: %w", i, r.stripe, err)
		}
	}
	frags := r.fragScratch
	if missing {
		if err := r.enc.Reconstruct(frags); err != nil {
			return fmt.Errorf("stripe stream reconstruct stripe %d: %w", r.stripe, err)
		}
	}
	if cap(r.buf) < k*fragSize {
		r.buf = make([]byte, 0, k*fragSize)
	}
	r.buf = r.buf[:0]
	for kk := 0; kk < k; kk++ {
		r.buf = append(r.buf, frags[kk]...)
	}
	r.buf = r.buf[:dS] // trim intra-stripe padding
	r.bufOff = 0
	r.stripe++
	r.remaining -= dS
	return nil
}

func (r *stripeDeinterleaveStreamReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	total := 0
	for total < len(p) {
		if r.bufOff >= len(r.buf) {
			if err := r.fill(); err != nil {
				if err == io.EOF {
					if total > 0 {
						return total, nil
					}
					r.err = io.EOF
					return total, io.EOF
				}
				r.err = err
				return total, err
			}
		}
		copied := copy(p[total:], r.buf[r.bufOff:])
		r.bufOff += copied
		total += copied
	}
	return total, nil
}

func (r *stripeDeinterleaveStreamReader) Close() error {
	for _, s := range r.shards {
		if c, ok := s.(io.Closer); ok {
			_ = c.Close()
		}
	}
	return nil
}
