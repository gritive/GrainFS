package cluster

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockUntilReader blocks every Read until release is closed, then reports EOF.
// It models a shard body whose remote read stalls (no data arriving) until the
// idle read timeout would fire in production.
type blockUntilReader struct{ release <-chan struct{} }

func (b blockUntilReader) Read(p []byte) (int, error) {
	<-b.release
	return 0, io.EOF
}

// headerThenBlock returns a shard reader that serves the 8-byte shard header
// synchronously (consumed by ecReconstructStreamBodies before prefetchers
// start), then blocks on the body read until release is closed.
func headerThenBlock(origSize int64, release <-chan struct{}) io.Reader {
	h := encodeShardHeader(origSize)
	return io.MultiReader(bytes.NewReader(h[:]), blockUntilReader{release: release})
}

// TestECReconstructStreamPrefetch_CloseReturnsPromptlyWhileShardReadBlocks
// reproduces the close/cancel hang: with all data shards present and >1 data
// shard, the prefetch path spawns background producers that block in io.ReadFull
// on the (stalled) shard bodies. Closing the reader must return promptly instead
// of waiting for the producers to unblock (which, in production, only happens at
// the idle read / shard RPC timeout).
func TestECReconstructStreamPrefetch_CloseReturnsPromptlyWhileShardReadBlocks(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	origSize := int64(4 << 20)
	release := make(chan struct{}) // closed mid-test to release the stalled readers

	shards := []io.Reader{
		headerThenBlock(origSize, release),
		headerThenBlock(origSize, release),
		nil, // parity absent — all data shards present drives the prefetch path
	}

	// closeShards models closeECShardReaders: it must NOT run while a producer is
	// still mid-Read on a shard body (Hertz S8-2), i.e. not until the blocked
	// readers are released.
	closeShardsCalled := make(chan struct{})
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, origSize, func() {
		close(closeShardsCalled)
	}, nil, nil)
	if err != nil {
		t.Fatalf("newECReconstructStreamReaderWithPrefetch: %v", err)
	}

	// Let the background producers reach their blocking io.ReadFull.
	time.Sleep(20 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- rc.Close() }()

	select {
	case <-done:
		// Close returned promptly — good.
	case <-time.After(2 * time.Second):
		t.Fatal("rc.Close() did not return promptly while shard read blocked (hang)")
	}

	// Invariant: shard bodies must not be closed while producers are still
	// blocked mid-Read (would be a forbidden cross-goroutine CloseBodyStream).
	// Held over a bounded window (not an instantaneous check) so an ungated
	// detached teardown that merely hasn't been scheduled yet cannot false-pass.
	select {
	case <-closeShardsCalled:
		t.Fatal("closeShards ran while a producer was still mid-Read (S8-2 violation)")
	case <-time.After(100 * time.Millisecond):
	}

	// Releasing the stalled readers lets the producers exit; only then may the
	// detached teardown close the shard bodies.
	close(release)
	select {
	case <-closeShardsCalled:
		// Shards closed after producers exited — correct ordering.
	case <-time.After(2 * time.Second):
		t.Fatal("closeShards was not invoked after the blocked readers were released")
	}
}

// signalingBlockReader closes entered on the first Read call, then blocks
// until release is closed, then reports EOF. Unlike a bare sleep, the entered
// signal PROVES the reconstruct goroutine is mid-Read when Close fires.
type signalingBlockReader struct {
	entered   chan<- struct{}
	release   <-chan struct{}
	announced bool
}

func (b *signalingBlockReader) Read(p []byte) (int, error) {
	if !b.announced {
		b.announced = true
		close(b.entered)
	}
	<-b.release
	return 0, io.EOF
}

// TestECReconstructStreamMissingData_CloseIsS82SafeWhileShardReadBlocks covers
// the MISSING-data-shard (io.Pipe) branch, mirroring the prefetch-path test
// above: closing the reader while the reconstruct goroutine is blocked mid-Read
// on a shard body must (a) return promptly and (b) NOT close the shard bodies
// until the goroutine has exited (Hertz S8-2: no cross-goroutine
// CloseBodyStream).
func TestECReconstructStreamMissingData_CloseIsS82SafeWhileShardReadBlocks(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	origSize := int64(4 << 20)
	release := make(chan struct{})
	entered := make(chan struct{})

	// Data shard 1 missing, parity present → ecStreamHasAllDataShards is false
	// → the io.Pipe reconstruct branch. Shard 0's body signals when the
	// reconstruct goroutine enters its blocking Read.
	h := encodeShardHeader(origSize)
	shard0 := io.MultiReader(bytes.NewReader(h[:]), &signalingBlockReader{entered: entered, release: release})
	shards := []io.Reader{
		shard0,
		nil,
		headerThenBlock(origSize, release),
	}

	closeShardsCalled := make(chan struct{})
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, origSize, func() {
		close(closeShardsCalled)
	}, nil, nil)
	if err != nil {
		t.Fatalf("newECReconstructStreamReaderWithPrefetch: %v", err)
	}

	// Wait until the reconstruct goroutine is provably blocked mid-Read on a
	// shard body (not a sleep heuristic).
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("reconstruct goroutine never reached the shard body Read")
	}

	done := make(chan error, 1)
	go func() { done <- rc.Close() }()

	select {
	case <-done:
		// Close returned promptly — good.
	case <-time.After(2 * time.Second):
		t.Fatal("rc.Close() did not return promptly while shard read blocked (hang)")
	}

	// Invariant: shard bodies must not be closed while the reconstruct goroutine
	// is still blocked mid-Read (forbidden cross-goroutine CloseBodyStream).
	// Held over a bounded window (not an instantaneous check) so an ungated
	// detached teardown that merely hasn't been scheduled yet cannot false-pass.
	select {
	case <-closeShardsCalled:
		t.Fatal("closeShards ran while the reconstruct goroutine was still mid-Read (S8-2 violation)")
	case <-time.After(100 * time.Millisecond):
	}

	// Releasing the stalled readers lets the goroutine exit; only then may the
	// detached teardown close the shard bodies.
	close(release)
	select {
	case <-closeShardsCalled:
		// Shards closed after the goroutine exited — correct ordering.
	case <-time.After(2 * time.Second):
		t.Fatal("closeShards was not invoked after the blocked readers were released")
	}
}

// TestECReconstructStreamMissingData_NormalCompletionStillClosesShards guards
// the fd-leak side of the detached teardown: when the GET runs to completion
// (no abort), Close must still result in closeShards being invoked — the done
// gate must not swallow the teardown on the happy path.
func TestECReconstructStreamMissingData_NormalCompletionStillClosesShards(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("x"), 64)

	// Build a real 2+1 stripe (ECSplit frames each shard as 8-byte header +
	// body, matching what ecReconstructStreamBodies expects) so the
	// missing-data branch can reconstruct data shard 1 from shard 0 + parity.
	stripe, err := ECSplit(cfg, payload)
	if err != nil {
		t.Fatalf("ECSplit: %v", err)
	}
	shards := []io.Reader{
		bytes.NewReader(stripe[0]),
		nil, // data shard 1 missing → io.Pipe reconstruct branch
		bytes.NewReader(stripe[2]),
	}

	closeShardsCalled := make(chan struct{})
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, int64(len(payload)), func() {
		close(closeShardsCalled)
	}, nil, nil)
	if err != nil {
		t.Fatalf("newECReconstructStreamReaderWithPrefetch: %v", err)
	}

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("reconstructed payload mismatch: got %d bytes", len(got))
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case <-closeShardsCalled:
		// Teardown ran on the happy path — no fd leak.
	case <-time.After(2 * time.Second):
		t.Fatal("closeShards was not invoked after normal completion + Close")
	}
}

// trickleThenBlockReader yields exactly one byte per Read while unblocked; each
// subsequent Read blocks until step is signaled. Models a trickling peer that
// defeats the idle-read deadline (which re-arms on every progressing Read).
type trickleThenBlockReader struct {
	step chan struct{} // send one token per byte to release
}

func (r *trickleThenBlockReader) Read(p []byte) (int, error) {
	<-r.step
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = 'x'
	return 1, nil
}

func TestReadFullOrStop_AbortWinsOverTricklingReader(t *testing.T) {
	step := make(chan struct{}, 4)
	r := &trickleThenBlockReader{step: step}
	stop := make(chan struct{})
	buf := make([]byte, 1<<20)

	step <- struct{}{} // one byte arrives
	done := make(chan struct{})
	var n int
	var err error
	go func() { defer close(done); n, err = readFullOrStop(r, buf, stop) }()

	// Producer consumed the byte and is now blocked awaiting the next trickle.
	// Abort, then deliver ONE more byte: the fill loop must exit at the next
	// stop-check instead of continuing toward 1 MiB.
	close(stop)
	step <- struct{}{}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("readFullOrStop kept filling after stop; trickling peer defeats abort")
	}
	require.ErrorIs(t, err, errECReadAborted)
	assert.LessOrEqual(t, n, 2)
}

func TestReadFullOrStop_MatchesReadFullSemantics(t *testing.T) {
	// full read, EOF-at-zero, and partial-EOF must mirror io.ReadFull.
	n, err := readFullOrStop(strings.NewReader("abcd"), make([]byte, 4), nil)
	require.NoError(t, err)
	assert.Equal(t, 4, n)

	n, err = readFullOrStop(strings.NewReader(""), make([]byte, 4), nil)
	assert.Equal(t, 0, n)
	require.ErrorIs(t, err, io.EOF)

	n, err = readFullOrStop(strings.NewReader("ab"), make([]byte, 4), nil)
	assert.Equal(t, 2, n)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

// TestECReconstructMissingData_AbortReleasesAgainstTricklingPeer covers the
// io.Pipe (missing-data) branch: aborting a GET against a trickling shard
// peer must release the detached teardown within a bounded time (one more
// trickled byte) instead of waiting for the full idle-read window, and the
// parked-teardown gauge must return to its pre-test baseline once the
// teardown has completed.
func TestECReconstructMissingData_AbortReleasesAgainstTricklingPeer(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	payload := bytes.Repeat([]byte("x"), 64)

	stripe, err := ECSplit(cfg, payload)
	if err != nil {
		t.Fatalf("ECSplit: %v", err)
	}

	step := make(chan struct{}, 4)
	// Serve shard 0's header synchronously, then trickle its body — data shard
	// 1 is nil to force the io.Pipe reconstruct branch.
	h := encodeShardHeader(int64(len(payload)))
	shard0 := io.MultiReader(bytes.NewReader(h[:]), &trickleThenBlockReader{step: step})
	shards := []io.Reader{
		shard0,
		nil, // data shard 1 missing → io.Pipe reconstruct branch
		bytes.NewReader(stripe[2]),
	}

	baseline := testutil.ToFloat64(metrics.ECDetachedTeardowns)

	closeShardsCalled := make(chan struct{})
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, int64(len(payload)), func() {
		close(closeShardsCalled)
	}, nil, nil)
	if err != nil {
		t.Fatalf("newECReconstructStreamReaderWithPrefetch: %v", err)
	}

	// Let the reconstruct goroutine reach its blocking Read on shard 0's body.
	step <- struct{}{}
	time.Sleep(20 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- rc.Close() }()

	select {
	case <-done:
		// Close returned promptly — good.
	case <-time.After(2 * time.Second):
		t.Fatal("rc.Close() did not return promptly")
	}

	// While the teardown is parked (producer aborted but not yet exited), the
	// gauge must reflect the in-flight teardown.
	assert.GreaterOrEqual(t, testutil.ToFloat64(metrics.ECDetachedTeardowns), baseline+1)

	// Release ONE more byte: the stop-aware fill loop must exit at the next
	// stop-check instead of continuing to wait on the trickling peer.
	step <- struct{}{}

	select {
	case <-closeShardsCalled:
		// Teardown released promptly after abort — correct.
	case <-time.After(2 * time.Second):
		t.Fatal("closeShards was not invoked promptly after abort + one trickled byte")
	}

	assert.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.ECDetachedTeardowns) == baseline
	}, 2*time.Second, 10*time.Millisecond, "gauge did not return to baseline after teardown completed")
}

// TestECReconstructPrefetch_AbortReleasesAgainstTricklingPeer pins the same
// trickle-abort scenario on the prefetch path (all data shards present, >=2
// data shards so the async prefetcher engages) — internal/cluster/async_prefetch_reader.go's
// stop-aware wiring, not just the shared readFullOrStop helper.
func TestECReconstructPrefetch_AbortReleasesAgainstTricklingPeer(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	origSize := int64(4 << 20)

	step := make(chan struct{}, 4)
	release := make(chan struct{}) // unused by shard 1; kept unclosed so shard 1 must not need release

	shards := []io.Reader{
		headerThenBlock(origSize, release), // stays blocked entirely; never reaches EOF during test
		func() io.Reader {
			h := encodeShardHeader(origSize)
			return io.MultiReader(bytes.NewReader(h[:]), &trickleThenBlockReader{step: step})
		}(),
		nil, // parity absent — all data shards present drives the prefetch path
	}

	baseline := testutil.ToFloat64(metrics.ECDetachedTeardowns)

	closeShardsCalled := make(chan struct{})
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, origSize, func() {
		close(closeShardsCalled)
	}, nil, nil)
	if err != nil {
		t.Fatalf("newECReconstructStreamReaderWithPrefetch: %v", err)
	}

	// Let the background producers reach their blocking reads.
	step <- struct{}{}
	time.Sleep(20 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- rc.Close() }()

	select {
	case <-done:
		// Close returned promptly — good.
	case <-time.After(2 * time.Second):
		t.Fatal("rc.Close() did not return promptly")
	}

	assert.GreaterOrEqual(t, testutil.ToFloat64(metrics.ECDetachedTeardowns), baseline+1)

	// Release one more byte on the trickling shard; the fully-blocked shard
	// (blockUntilReader) is released too so the producer waiting on it can also
	// exit and unblock the teardown.
	step <- struct{}{}
	close(release)

	select {
	case <-closeShardsCalled:
		// Teardown released promptly after abort — correct.
	case <-time.After(2 * time.Second):
		t.Fatal("closeShards was not invoked promptly after abort + one trickled byte")
	}

	assert.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.ECDetachedTeardowns) == baseline
	}, 2*time.Second, 10*time.Millisecond, "gauge did not return to baseline after teardown completed")
}
