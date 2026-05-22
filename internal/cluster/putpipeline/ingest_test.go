package putpipeline

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIngestActor_SplitsBodyIntoStripes_5MiB(t *testing.T) {
	const stripe = 1 << 20 // 1 MiB
	const bodyLen = 5 * stripe
	body := make([]byte, bodyLen)
	for i := range body {
		body[i] = byte(i % 251)
	}

	out := make(chan StripePlaintext, 8)
	a := &IngestActor{out: out, stripeBytes: stripe}

	go func() {
		_, _, err := a.Run(context.Background(), 1, "bucket", bytes.NewReader(body))
		require.NoError(t, err)
		close(out)
	}()

	var got []StripePlaintext
	for s := range out {
		got = append(got, s)
	}
	require.Len(t, got, 5, "expected 5 stripes for 5 MiB body")
	for i, s := range got {
		require.Equal(t, uint32(i), s.StripeIdx)
		require.Equal(t, stripe, len(s.Data))
		require.Equal(t, uint32(0), s.Padding)
		require.Equal(t, i == 4, s.LastInPut, "only last stripe sets LastInPut")
	}
	var roundtrip []byte
	for _, s := range got {
		roundtrip = append(roundtrip, s.Data...)
	}
	require.Equal(t, body, roundtrip)
}

func TestIngestActor_LastStripePadded(t *testing.T) {
	const stripe = 1 << 20
	const bodyLen = 3*stripe + 100 // 3 full + 100-byte partial
	body := bytes.Repeat([]byte{0xAB}, bodyLen)

	out := make(chan StripePlaintext, 8)
	a := &IngestActor{out: out, stripeBytes: stripe}
	go func() {
		_, total, err := a.Run(context.Background(), 1, "bucket", bytes.NewReader(body))
		require.NoError(t, err)
		require.Equal(t, int64(bodyLen), total)
		close(out)
	}()

	var got []StripePlaintext
	for s := range out {
		got = append(got, s)
	}
	require.Len(t, got, 4)
	last := got[3]
	require.True(t, last.LastInPut)
	require.Equal(t, uint32(stripe-100), last.Padding)
	require.Equal(t, stripe, len(last.Data))
	for i := 100; i < stripe; i++ {
		require.Equal(t, byte(0), last.Data[i], "padded region must be zero")
	}
	for i := 0; i < 100; i++ {
		require.Equal(t, byte(0xAB), last.Data[i])
	}
}
