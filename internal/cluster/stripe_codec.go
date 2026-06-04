package cluster

import (
	"fmt"
	"io"

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

// stripeDeinterleave reconstructs the object from stripe-INTERLEAVED shard bodies
// (8-byte header already stripped). bodies has length K+M; nil entry = missing shard
// (RS reconstruct per stripe). Result is exactly objectSize bytes.
func stripeDeinterleave(cfg ECConfig, bodies [][]byte, stripeBytes int, objectSize int64) ([]byte, error) {
	k, n := cfg.DataShards, cfg.NumShards()
	if len(bodies) != n {
		return nil, fmt.Errorf("stripe de-interleave: got %d shards, want %d", len(bodies), n)
	}
	if stripeBytes <= 0 {
		return nil, fmt.Errorf("stripe de-interleave: stripeBytes must be > 0")
	}
	enc, err := getEncoder(cfg)
	if err != nil {
		return nil, fmt.Errorf("stripe de-interleave encoder: %w", err)
	}
	out := make([]byte, 0, objectSize)
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
		missing := false
		for i := 0; i < n; i++ {
			if bodies[i] == nil {
				missing = true
				continue
			}
			if offset+fragSize > len(bodies[i]) {
				return nil, fmt.Errorf("stripe de-interleave: shard %d short at stripe %d (have %d need %d)", i, s, len(bodies[i]), offset+fragSize)
			}
			frags[i] = bodies[i][offset : offset+fragSize]
		}
		if missing {
			rebuilt := make([][]byte, n)
			for i := 0; i < n; i++ {
				if frags[i] != nil {
					rebuilt[i] = append([]byte(nil), frags[i]...)
				}
			}
			if err := enc.Reconstruct(rebuilt); err != nil {
				return nil, fmt.Errorf("stripe de-interleave reconstruct stripe %d: %w", s, err)
			}
			frags = rebuilt
		}
		for kk := 0; kk < k; kk++ {
			out = append(out, frags[kk]...)
		}
		out = out[:len(out)-(k*fragSize-dS)] // trim intra-stripe padding (zeros in last data shard tail)
		offset += fragSize
		remaining -= dS
	}
	return out, nil
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
}

func newStripeDeinterleaveStreamReader(cfg ECConfig, shards []io.Reader, stripeBytes int, objectSize int64) (io.ReadCloser, error) {
	n := cfg.NumShards()
	if len(shards) != n {
		return nil, fmt.Errorf("stripe stream: got %d shards, want %d", len(shards), n)
	}
	if stripeBytes <= 0 {
		return nil, fmt.Errorf("stripe stream: stripeBytes must be > 0")
	}
	enc, err := getEncoder(cfg)
	if err != nil {
		return nil, fmt.Errorf("stripe stream encoder: %w", err)
	}
	return &stripeDeinterleaveStreamReader{
		cfg: cfg, enc: enc, shards: shards, stripeBytes: stripeBytes,
		remaining: int(objectSize), fragScratch: make([][]byte, n),
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
		if _, err := io.ReadFull(r.shards[i], r.fragScratch[i]); err != nil {
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
