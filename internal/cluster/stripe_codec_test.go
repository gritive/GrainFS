package cluster

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestStripeDeinterleaveStreaming(t *testing.T) {
	const k, m, stripeBytes = 2, 2, 1 << 20
	cfg := ECConfig{DataShards: k, ParityShards: m}
	payload := make([]byte, 33*1024*1024+999)
	for i := range payload {
		payload[i] = byte((i*131 + 7) % 251)
	}
	bodies := buildInterleavedShards(t, cfg, payload, stripeBytes)

	rc, err := newStripeDeinterleaveStreamReader(cfg, bodyReaders(cloneShards(bodies)), stripeBytes, int64(len(payload)))
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got)

	deg := cloneShards(bodies)
	deg[0] = nil
	rc2, err := newStripeDeinterleaveStreamReader(cfg, bodyReaders(deg), stripeBytes, int64(len(payload)))
	require.NoError(t, err)
	got2, err := io.ReadAll(rc2)
	require.NoError(t, err)
	require.NoError(t, rc2.Close())
	require.Equal(t, payload, got2)
}

func bodyReaders(bodies [][]byte) []io.Reader {
	out := make([]io.Reader, len(bodies))
	for i, b := range bodies {
		if b != nil {
			out[i] = bytes.NewReader(b)
		}
	}
	return out
}

// Asserts the reader does NOT buffer the whole object: before the first 4 KiB of
// output, each data shard reader is pulled at most ~one fragment, not its whole body.
func TestStripeDeinterleaveStreamingBounded(t *testing.T) {
	const k, m, stripeBytes = 2, 2, 1 << 20
	cfg := ECConfig{DataShards: k, ParityShards: m}
	payload := make([]byte, 16*1024*1024) // 16 stripes
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	bodies := buildInterleavedShards(t, cfg, payload, stripeBytes)
	shardBodyLen := len(bodies[0])

	probes := make([]*countingReader, cfg.NumShards())
	readers := make([]io.Reader, cfg.NumShards())
	for i := range bodies {
		probes[i] = &countingReader{r: bytes.NewReader(bodies[i])}
		readers[i] = probes[i]
	}
	rc, err := newStripeDeinterleaveStreamReader(cfg, readers, stripeBytes, int64(len(payload)))
	require.NoError(t, err)
	first := make([]byte, 4096)
	_, err = io.ReadFull(rc, first)
	require.NoError(t, err)
	require.Equal(t, payload[:4096], first)
	for i := 0; i < cfg.DataShards; i++ {
		require.Less(t, probes[i].n, shardBodyLen/4,
			"shard %d: streaming reader pulled %d of %d bytes before first 4KiB output — looks like it buffered", i, probes[i].n, shardBodyLen)
	}
	_ = rc.Close()
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}
