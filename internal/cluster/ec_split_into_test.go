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

// ecSplitRawInto must not leak a prior (larger) stripe's bytes into a later
// (smaller) stripe's zero-padding or parity region when the backing is reused
// from the pool. The previous test always handed a freshly-allocated (zeroed)
// dst, so it never exercised the clear() that guards this — split a large
// stripe to dirty the backing, then reuse that exact backing for a small
// stripe and assert byte-identity to the oracle.
func TestECSplitRawInto_ReusedDirtyBackingNoStaleLeak(t *testing.T) {
	cfgs := []ECConfig{{DataShards: 2, ParityShards: 2}, {DataShards: 4, ParityShards: 2}}
	for _, cfg := range cfgs {
		large := make([]byte, 1<<20)
		for i := range large {
			large[i] = byte(i*7 + 1)
		}
		// First split dirties the backing with the large stripe's data+parity.
		_, backing, err := ecSplitRawInto(cfg, large, nil)
		require.NoError(t, err)
		require.Greater(t, cap(backing), 1024, "backing should retain large-stripe capacity for reuse")

		// Reuse the SAME (dirty, larger-cap) backing for a small stripe whose
		// padding tail + parity region must be re-zeroed, not inherited.
		for _, sz := range []int{1, cfg.DataShards - 1, cfg.DataShards, cfg.DataShards + 1, 1023} {
			small := make([]byte, sz)
			for i := range small {
				small[i] = byte(i*13 + 3)
			}
			want, err := ECSplitRaw(cfg, small)
			require.NoError(t, err)
			got, reused, err := ecSplitRawInto(cfg, small, backing)
			require.NoError(t, err)
			backing = reused // thread the reused backing forward
			require.Equal(t, len(want), len(got), "shard count cfg=%v sz=%d", cfg, sz)
			for i := range want {
				require.Truef(t, bytes.Equal(want[i], got[i]),
					"reused-backing shard %d stale-leak cfg=%v sz=%d:\n want %x\n  got %x", i, cfg, sz, want[i], got[i])
			}
		}
	}
}
