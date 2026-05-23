package putpipeline

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"testing"
	"time"

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

func TestIngestActor_ETagMD5(t *testing.T) {
	const stripe = 1 << 20
	body := bytes.Repeat([]byte("hello"), 100000)
	wantSum := md5.Sum(body)
	want := hex.EncodeToString(wantSum[:])

	out := make(chan StripePlaintext, 8)
	a := &IngestActor{out: out, stripeBytes: stripe}
	var gotETag string
	done := make(chan struct{})
	go func() {
		var err error
		gotETag, _, err = a.Run(context.Background(), 1, "external", bytes.NewReader(body))
		require.NoError(t, err)
		close(out)
		close(done)
	}()
	for range out {
	}
	<-done
	require.Equal(t, want, gotETag)
}

func TestIngestActor_PrecomputedETagMatches(t *testing.T) {
	const stripe = 1 << 20
	body := bytes.Repeat([]byte{0xCC}, 3*stripe)
	sum := md5.Sum(body)
	precomputed := hex.EncodeToString(sum[:])

	out := make(chan StripePlaintext, 8)
	a := &IngestActor{
		out:             out,
		stripeBytes:     stripe,
		precomputedETag: precomputed,
	}
	var gotETag string
	done := make(chan struct{})
	go func() {
		var err error
		gotETag, _, err = a.Run(context.Background(), 1, "external", bytes.NewReader(body))
		require.NoError(t, err)
		close(out)
		close(done)
	}()
	for range out {
	}
	<-done
	require.Equal(t, precomputed, gotETag, "matching precomputed Content-MD5 should be returned as ETag")
}

func TestIngestActor_PrecomputedETagMismatchRejects(t *testing.T) {
	const stripe = 1 << 20
	body := bytes.Repeat([]byte{0xCC}, 3*stripe)
	const wrongPrecomputed = "deadbeefcafebabe1234567890abcdef"

	out := make(chan StripePlaintext, 8)
	a := &IngestActor{
		out:             out,
		stripeBytes:     stripe,
		precomputedETag: wrongPrecomputed,
	}
	done := make(chan struct{})
	var runErr error
	go func() {
		_, _, runErr = a.Run(context.Background(), 1, "external", bytes.NewReader(body))
		close(out)
		close(done)
	}()
	for range out {
	}
	<-done
	require.ErrorIs(t, runErr, ErrContentMD5Mismatch, "wrong client Content-MD5 should reject as BadDigest")
}

func TestIngestActor_ContextCancel(t *testing.T) {
	const stripe = 1 << 20
	body := bytes.Repeat([]byte{0xAA}, 16*stripe)

	out := make(chan StripePlaintext) // unbuffered: blocks on first send
	a := &IngestActor{out: out, stripeBytes: stripe}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, _, err := a.Run(ctx, 1, "bucket", bytes.NewReader(body))
		errCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("ingest actor did not honor ctx cancellation")
	}
}
