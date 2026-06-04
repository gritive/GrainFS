package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// (A later task adds "io"+"bytes" to this file for streaming tests.)

func TestStripeDeinterleaveBuffered(t *testing.T) {
	const k, m, stripeBytes = 2, 2, 1 << 20
	cfg := ECConfig{DataShards: k, ParityShards: m}
	payload := make([]byte, 4*1024*1024+777)
	for i := range payload {
		payload[i] = byte((i * 7) % 251)
	}
	bodies := buildInterleavedShards(t, cfg, payload, stripeBytes)

	got, err := stripeDeinterleave(cfg, cloneShards(bodies), stripeBytes, int64(len(payload)))
	require.NoError(t, err)
	require.Equal(t, payload, got)

	deg := cloneShards(bodies)
	deg[0] = nil
	got2, err := stripeDeinterleave(cfg, deg, stripeBytes, int64(len(payload)))
	require.NoError(t, err)
	require.Equal(t, payload, got2)
}

// buildInterleavedShards mirrors CPUPool: per stripe, ECSplitRaw and append fragment i to shard i.
func buildInterleavedShards(t *testing.T, cfg ECConfig, payload []byte, stripeBytes int) [][]byte {
	t.Helper()
	n := cfg.NumShards()
	bodies := make([][]byte, n)
	for off := 0; off < len(payload); off += stripeBytes {
		end := off + stripeBytes
		if end > len(payload) {
			end = len(payload)
		}
		frags, err := ECSplitRaw(cfg, payload[off:end])
		require.NoError(t, err)
		for i := 0; i < n; i++ {
			bodies[i] = append(bodies[i], frags[i]...)
		}
	}
	return bodies
}

func cloneShards(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	for i, s := range in {
		if s != nil {
			out[i] = append([]byte(nil), s...)
		}
	}
	return out
}
