package erasure

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCodec_Roundtrip(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"tiny_10bytes", 10},
		{"small_100bytes", 100},
		{"exact_shard_boundary", 1024},
		{"medium_1KB", 1000},
		{"large_1MB", 1024 * 1024},
		{"uneven_size", 12345},
	}

	codec := NewCodec(DefaultDataShards, DefaultParityShards)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := make([]byte, tt.size)
			_, err := rand.Read(original)
			require.NoError(t, err)

			shards, err := codec.Encode(original)
			require.NoError(t, err)
			assert.Len(t, shards, codec.TotalShards())

			// All shards should be the same size
			for i := 1; i < len(shards); i++ {
				assert.Equal(t, len(shards[0]), len(shards[i]), "shard size mismatch")
			}

			// Decode with all shards present
			decoded, err := codec.Decode(shards, int64(tt.size))
			require.NoError(t, err)
			assert.Equal(t, original, decoded)
		})
	}
}

func TestCodec_RecoverMissingShards(t *testing.T) {
	codec := NewCodec(4, 2)
	original := bytes.Repeat([]byte("hello erasure coding!"), 100)

	shards, err := codec.Encode(original)
	require.NoError(t, err)

	tests := []struct {
		name    string
		missing []int // indices of shards to remove
		wantErr bool
	}{
		{"no_missing", nil, false},
		{"missing_1_data", []int{0}, false},
		{"missing_1_parity", []int{4}, false},
		{"missing_2_data", []int{0, 1}, false},
		{"missing_2_parity", []int{4, 5}, false},
		{"missing_1_data_1_parity", []int{1, 5}, false},
		{"missing_3_too_many", []int{0, 1, 2}, true},
		{"missing_all_parity_plus_one", []int{2, 4, 5}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy and nil out missing shards
			copied := make([][]byte, len(shards))
			for i, s := range shards {
				copied[i] = make([]byte, len(s))
				copy(copied[i], s)
			}
			for _, idx := range tt.missing {
				copied[idx] = nil
			}

			decoded, err := codec.Decode(copied, int64(len(original)))
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, original, decoded)
			}
		})
	}
}

func TestCodec_WrongShardCount(t *testing.T) {
	codec := NewCodec(4, 2)

	_, err := codec.Decode(make([][]byte, 3), 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected 6 shards, got 3")
}

func TestCodec_EmptyData(t *testing.T) {
	codec := NewCodec(4, 2)

	// reedsolomon.Split doesn't accept empty data
	_, err := codec.Encode([]byte{})
	assert.Error(t, err)
}

func TestCodec_CustomParams(t *testing.T) {
	codec := NewCodec(3, 3) // 3+3 = higher redundancy
	original := []byte("custom erasure coding parameters test data")

	shards, err := codec.Encode(original)
	require.NoError(t, err)
	assert.Len(t, shards, 6)

	// Can lose up to 3 shards
	shards[0] = nil
	shards[2] = nil
	shards[4] = nil

	decoded, err := codec.Decode(shards, int64(len(original)))
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}
