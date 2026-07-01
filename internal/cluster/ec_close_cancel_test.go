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
	select {
	case <-closeShardsCalled:
		t.Fatal("closeShards ran while a producer was still mid-Read (S8-2 violation)")
	default:
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
