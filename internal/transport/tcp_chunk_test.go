package transport

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunkedBody_Roundtrip(t *testing.T) {
	for _, size := range []int{0, 1, 4096, 1 << 20, (1 << 20) + 7, 5 << 20} {
		payload := make([]byte, size)
		_, _ = rand.Read(payload)
		var buf bytes.Buffer
		require.NoError(t, writeChunkedBody(&buf, bytes.NewReader(payload)))

		cr := &chunkedBodyReader{r: &buf}
		got, err := io.ReadAll(cr)
		require.NoErrorf(t, err, "size %d", size)
		assert.Equalf(t, payload, got, "size %d roundtrip", size)
		assert.Truef(t, cr.done, "size %d should reach terminator", size)
	}
}

func TestChunkedBody_ReaderStopsAtTerminator(t *testing.T) {
	// Two chunked bodies back-to-back: the first reader must stop exactly at the
	// first terminator, leaving the second body intact (this is what makes the
	// conn reusable).
	var buf bytes.Buffer
	require.NoError(t, writeChunkedBody(&buf, bytes.NewReader([]byte("first"))))
	require.NoError(t, writeChunkedBody(&buf, bytes.NewReader([]byte("second"))))

	cr1 := &chunkedBodyReader{r: &buf}
	got1, err := io.ReadAll(cr1)
	require.NoError(t, err)
	assert.Equal(t, "first", string(got1))

	cr2 := &chunkedBodyReader{r: &buf}
	got2, err := io.ReadAll(cr2)
	require.NoError(t, err)
	assert.Equal(t, "second", string(got2))
}

func TestChunkedBody_OversizeChunkRejected(t *testing.T) {
	var buf bytes.Buffer
	var hdr [4]byte
	// length = tcpMaxChunkSize+1, no payload — the reader must reject before allocating.
	binary.BigEndian.PutUint32(hdr[:], tcpMaxChunkSize+1)
	buf.Write(hdr[:])
	cr := &chunkedBodyReader{r: &buf}
	_, err := io.ReadAll(cr)
	require.Error(t, err)
}
