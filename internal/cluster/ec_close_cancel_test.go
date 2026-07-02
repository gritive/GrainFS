package cluster

import (
	"bytes"
	"io"
	"testing"
	"time"
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
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, func() {
		close(closeShardsCalled)
	})
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
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, func() {
		close(closeShardsCalled)
	})
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
	rc, err := newECReconstructStreamReaderWithPrefetch(cfg, shards, func() {
		close(closeShardsCalled)
	})
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
