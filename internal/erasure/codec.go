package erasure

import (
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// DefaultDataShards is the default number of data shards (k).
const DefaultDataShards = 4

// DefaultParityShards is the default number of parity shards (m).
const DefaultParityShards = 2

// Codec performs erasure coding encode/decode using Reed-Solomon.
type Codec struct {
	DataShards   int
	ParityShards int
}

// NewCodec creates a new erasure coding codec.
func NewCodec(dataShards, parityShards int) *Codec {
	return &Codec{
		DataShards:   dataShards,
		ParityShards: parityShards,
	}
}

// TotalShards returns k + m.
func (c *Codec) TotalShards() int {
	return c.DataShards + c.ParityShards
}

// Encode splits data into k data shards and m parity shards.
// The returned slice has k+m entries. Each shard is the same size.
// originalSize must be stored alongside the shards so Decode can trim padding.
func (c *Codec) Encode(data []byte) ([][]byte, error) {
	enc, err := reedsolomon.New(c.DataShards, c.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("create encoder: %w", err)
	}

	shards, err := enc.Split(data)
	if err != nil {
		return nil, fmt.Errorf("split data: %w", err)
	}

	if err := enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("encode parity: %w", err)
	}

	return shards, nil
}

// Decode reconstructs the original data from shards.
// Nil entries in the shards slice indicate missing/corrupt shards.
// At least k non-nil shards are required for reconstruction.
// originalSize is used to trim padding from the last data shard.
func (c *Codec) Decode(shards [][]byte, originalSize int64) ([]byte, error) {
	if len(shards) != c.TotalShards() {
		return nil, fmt.Errorf("expected %d shards, got %d", c.TotalShards(), len(shards))
	}

	enc, err := reedsolomon.New(c.DataShards, c.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("create encoder: %w", err)
	}

	if err := enc.Reconstruct(shards); err != nil {
		return nil, fmt.Errorf("reconstruct: %w", err)
	}

	ok, err := enc.Verify(shards)
	if err != nil {
		return nil, fmt.Errorf("verify: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("verification failed after reconstruction")
	}

	// Join data shards and trim to original size
	var result []byte
	for i := range c.DataShards {
		result = append(result, shards[i]...)
	}

	if int64(len(result)) < originalSize {
		return nil, fmt.Errorf("reconstructed data too short: got %d, expected %d", len(result), originalSize)
	}

	return result[:originalSize], nil
}
