package cluster

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// ecSplitRawInto must produce shards byte-identical to ECSplitRaw for every
// data length, including non-multiples of DataShards (zero-padded tail) and
// the empty case. The existing ECSplitRaw is the oracle.
func TestECSplitRawInto_ByteIdenticalToSplit(t *testing.T) {
	cfgs := []ECConfig{{DataShards: 2, ParityShards: 2}, {DataShards: 4, ParityShards: 2}}
	for _, cfg := range cfgs {
		k := cfg.DataShards
		sizes := []int{0, 1, k - 1, k, k + 1, 7, 1023, 1024, 1<<20 - 1, 1 << 20}
		for _, sz := range sizes {
			data := make([]byte, sz)
			for i := range data {
				data[i] = byte(i*7 + 1)
			}
			want, err := ECSplitRaw(cfg, data)
			require.NoError(t, err)
			// reused backing across calls to exercise dst reuse
			dst := make([]byte, 0, 4<<20)
			got, _, err := ecSplitRawInto(cfg, data, dst)
			require.NoError(t, err)
			require.Equal(t, len(want), len(got), "shard count cfg=%v sz=%d", cfg, sz)
			for i := range want {
				require.Truef(t, bytes.Equal(want[i], got[i]),
					"shard %d mismatch cfg=%v sz=%d:\n want %x\n  got %x", i, cfg, sz, want[i], got[i])
			}
		}
	}
}
