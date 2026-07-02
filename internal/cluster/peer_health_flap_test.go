package cluster

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A peer that opens fine but failed mid-body seconds ago must NOT be flipped
// back to healthy by the next successful open (the #1014 cooldown would
// otherwise be neutralized within ms under sustained read load).
func TestOpenShardStream_SuccessDoesNotClearActiveCooldown(t *testing.T) {
	ph := NewPeerHealth([]string{"n2"}, 10*time.Second)
	ph.MarkUnhealthy("n2") // active cooldown from a mid-body fault
	store := &streamShardStore{body: io.NopCloser(strings.NewReader("body"))}
	ep := remoteShardEndpoint{node: "n2", shards: store, peerHealth: ph}

	rc, err := ep.OpenShardStream(context.Background(), "b", "k", 0)
	require.NoError(t, err)
	defer rc.Close()
	assert.False(t, ph.IsHealthy("n2"), "open success must not clear an active cooldown")
}

// Clean exact-length body completion IS healthy evidence.
func TestECExactLenReader_CleanCompletionFiresOnClean(t *testing.T) {
	var cleaned []int
	r := &ecExactLenReader{
		r: strings.NewReader("abcd"), idx: 2, want: 4, remaining: 4,
		onClean: func(i int) { cleaned = append(cleaned, i) },
	}
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "abcd", string(got))
	assert.Equal(t, []int{2}, cleaned, "onClean fires exactly once with the shard idx")
}

// Truncation must never fire onClean.
func TestECExactLenReader_TruncationDoesNotFireOnClean(t *testing.T) {
	fired := false
	r := &ecExactLenReader{
		r: strings.NewReader("ab"), idx: 0, want: 4, remaining: 4,
		onClean: func(int) { fired = true },
	}
	_, err := io.ReadAll(r)
	require.Error(t, err)
	assert.False(t, fired)
}

// A peer-fault error delivered WITH the final bytes must not fire onClean —
// otherwise MarkHealthy would instantly clear the cooldown the endpoint layer
// just set for that same error (flap through the back door).
type finalBytesThenErrReader struct{ done bool }

func (r *finalBytesThenErrReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, errors.New("conn reset")
	}
	r.done = true
	n := copy(p, "abcd")
	return n, errors.New("conn reset") // full body + transport error in one call
}

func TestECExactLenReader_FaultAlongsideFinalBytesDoesNotFireOnClean(t *testing.T) {
	fired := false
	r := &ecExactLenReader{
		r: &finalBytesThenErrReader{}, idx: 1, want: 4, remaining: 4,
		onClean: func(int) { fired = true },
	}
	buf := make([]byte, 8)
	n, err := r.Read(buf)
	assert.Equal(t, 4, n)
	require.Error(t, err)
	assert.False(t, fired, "transport fault with final bytes is not clean completion")
}

// io.ErrUnexpectedEOF alongside the final bytes is ALSO a peer fault
// (isPeerFaultReadErr does not exempt it, so the endpoint wrapper marks
// unhealthy) — onClean must not clear that cooldown.
func TestECExactLenReader_UnexpectedEOFAlongsideFinalBytesDoesNotFireOnClean(t *testing.T) {
	fired := false
	r := &ecExactLenReader{
		r: &unexpectedEOFTailReader{}, idx: 1, want: 4, remaining: 4,
		onClean: func(int) { fired = true },
	}
	buf := make([]byte, 8)
	n, err := r.Read(buf)
	assert.Equal(t, 4, n)
	assert.ErrorIs(t, err, io.EOF) // the guard normalizes remaining==0 + EOF-family for the consumer
	assert.False(t, fired, "io.ErrUnexpectedEOF with final bytes must not count as clean completion")
}

type unexpectedEOFTailReader struct{ done bool }

func (r *unexpectedEOFTailReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.ErrUnexpectedEOF
	}
	r.done = true
	return copy(p, "abcd"), io.ErrUnexpectedEOF
}
