package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeShardWriter records every WriteShardStream invocation and returns canned
// errors per attempt. nil entries (or out-of-range index) mean success.
type fakeShardWriter struct {
	mu       sync.Mutex
	errs     []error
	calls    int
	bodyLens []int
}

func (f *fakeShardWriter) WriteShardStream(_ context.Context, _, _, _ string, _ int, body io.Reader) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	// Drain body so per-attempt re-open is exercised. Records bytes drained
	// for the test to assert that each attempt sees a fresh full body.
	n, _ := io.Copy(io.Discard, body)
	f.bodyLens = append(f.bodyLens, int(n))
	if f.calls-1 < len(f.errs) {
		return f.errs[f.calls-1]
	}
	return nil
}

// newSpoolFile materializes a tiny spooledObject backed by a real tmp file so
// sp.Open returns a fresh *os.File per attempt (matching production semantics).
func newSpoolFile(t *testing.T, payload []byte) *spooledObject {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "spool")
	require.NoError(t, os.WriteFile(path, payload, 0o644))
	return &spooledObject{Path: path, Size: int64(len(payload))}
}

func TestWriteSpooledReplicaShardStream_FirstAttemptOK(t *testing.T) {
	b := &DistributedBackend{}
	sp := newSpoolFile(t, []byte("payload"))
	w := &fakeShardWriter{}

	err := b.writeSpooledReplicaShardStream(context.Background(), w, "peer-a", "bkt", "k/v1", sp)
	require.NoError(t, err)
	require.Equal(t, 1, w.calls, "first attempt success must short-circuit retry loop")
	require.Equal(t, []int{7}, w.bodyLens)
}

func TestWriteSpooledReplicaShardStream_RetryThenSucceed(t *testing.T) {
	b := &DistributedBackend{}
	sp := newSpoolFile(t, []byte("payload-bytes"))
	w := &fakeShardWriter{
		errs: []error{fmt.Errorf("transient: stream open"), nil},
	}

	start := time.Now()
	err := b.writeSpooledReplicaShardStream(context.Background(), w, "peer-a", "bkt", "k/v1", sp)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, 2, w.calls, "second attempt must succeed after first failed")
	// First attempt failed → backoff = 1 * 100ms = 100ms before second attempt.
	require.GreaterOrEqual(t, elapsed, 100*time.Millisecond, "must wait first backoff")
	require.Less(t, elapsed, 250*time.Millisecond, "must not wait the second backoff (success on retry)")
	// Each attempt re-opens the body and reads its full length.
	require.Equal(t, []int{13, 13}, w.bodyLens)
}

func TestWriteSpooledReplicaShardStream_AllAttemptsFail(t *testing.T) {
	b := &DistributedBackend{}
	sp := newSpoolFile(t, []byte("payload"))
	finalErr := fmt.Errorf("third-attempt-error")
	w := &fakeShardWriter{
		errs: []error{
			fmt.Errorf("first"),
			fmt.Errorf("second"),
			finalErr,
		},
	}

	err := b.writeSpooledReplicaShardStream(context.Background(), w, "peer-a", "bkt", "k/v1", sp)
	require.Error(t, err)
	require.True(t, errors.Is(err, finalErr) || err.Error() == finalErr.Error(),
		"final error must surface; got: %v", err)
	require.Equal(t, replicaWriteAttempts, w.calls,
		"must exhaust the configured attempt budget")
}

func TestWriteSpooledReplicaShardStream_ContextCancelMidBackoff(t *testing.T) {
	b := &DistributedBackend{}
	sp := newSpoolFile(t, []byte("payload"))
	firstErr := fmt.Errorf("transient")
	w := &fakeShardWriter{
		errs: []error{firstErr, nil}, // would succeed on retry, but ctx will cancel
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a delay shorter than the first backoff window (100ms).
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := b.writeSpooledReplicaShardStream(ctx, w, "peer-a", "bkt", "k/v1", sp)
	elapsed := time.Since(start)
	require.Error(t, err)
	require.Equal(t, firstErr.Error(), err.Error(),
		"must return the last attempt's error, not ctx err")
	require.Equal(t, 1, w.calls,
		"context cancellation during backoff must abort before retry")
	require.Less(t, elapsed, 100*time.Millisecond,
		"must bail out of backoff sleep, not wait the full 100ms")
}
