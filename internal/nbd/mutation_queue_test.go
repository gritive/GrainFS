package nbd

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitForSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

func waitForFlush(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("mutation queue flush did not complete")
		return nil
	}
}

func closeSignal(ch chan struct{}, closed *atomic.Bool) {
	if closed.CompareAndSwap(false, true) {
		close(ch)
	}
}

func TestMutationQueueSerializesSameBlockDifferentOffsets(t *testing.T) {
	q := newMutationQueue(4096)
	enteredFirst := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseFirstClosed atomic.Bool
	defer closeSignal(releaseFirst, &releaseFirstClosed)
	var secondStartedBeforeFirstFinished atomic.Bool

	q.AppendRange(0, 2048, []func() error{
		func() error {
			close(enteredFirst)
			<-releaseFirst
			return nil
		},
	})
	q.AppendRange(2048, 2048, []func() error{
		func() error {
			select {
			case <-releaseFirst:
			default:
				secondStartedBeforeFirstFinished.Store(true)
			}
			return nil
		},
	})

	done := make(chan error, 1)
	go func() {
		done <- q.Flush(context.Background())
	}()

	waitForSignal(t, enteredFirst, "first same-block mutation did not start")
	select {
	case err := <-done:
		t.Fatalf("flush completed before first same-block mutation was released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	closeSignal(releaseFirst, &releaseFirstClosed)
	require.NoError(t, waitForFlush(t, done))
	require.False(t, secondStartedBeforeFirstFinished.Load(), "same-block mutations must not run concurrently")
	require.Equal(t, 0, q.Len())
}

func TestMutationQueueRunsDistinctBlocksInSameWave(t *testing.T) {
	q := newMutationQueue(4096)
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	release := make(chan struct{})
	var releaseClosed atomic.Bool
	defer closeSignal(release, &releaseClosed)

	q.AppendRange(0, 4096, []func() error{
		func() error {
			close(firstEntered)
			<-release
			return nil
		},
	})
	q.AppendRange(4096, 4096, []func() error{
		func() error {
			close(secondEntered)
			<-release
			return nil
		},
	})

	done := make(chan error, 1)
	go func() {
		done <- q.Flush(context.Background())
	}()

	waitForSignal(t, firstEntered, "first block mutation did not start")
	waitForSignal(t, secondEntered, "distinct block mutation did not start in the same wave")
	closeSignal(release, &releaseClosed)
	require.NoError(t, waitForFlush(t, done))
}

func TestMutationQueueUsesConfiguredBlockSize(t *testing.T) {
	q := newMutationQueue(8192)
	enteredFirst := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseFirstClosed atomic.Bool
	defer closeSignal(releaseFirst, &releaseFirstClosed)
	var secondStartedBeforeFirstFinished atomic.Bool

	q.AppendRange(0, 4096, []func() error{
		func() error {
			close(enteredFirst)
			<-releaseFirst
			return nil
		},
	})
	q.AppendRange(4096, 4096, []func() error{
		func() error {
			select {
			case <-releaseFirst:
			default:
				secondStartedBeforeFirstFinished.Store(true)
			}
			return nil
		},
	})

	done := make(chan error, 1)
	go func() {
		done <- q.Flush(context.Background())
	}()

	waitForSignal(t, enteredFirst, "first configured-block mutation did not start")
	select {
	case err := <-done:
		t.Fatalf("flush completed before first configured-block mutation was released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	closeSignal(releaseFirst, &releaseFirstClosed)
	require.NoError(t, waitForFlush(t, done))
	require.False(t, secondStartedBeforeFirstFinished.Load(), "queue must use configured block size, not a hard-coded default")
}

func TestMutationQueuePreservesTransitiveOverlapOrder(t *testing.T) {
	q := newMutationQueue(4096)
	enteredFirst := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseFirstClosed atomic.Bool
	defer closeSignal(releaseFirst, &releaseFirstClosed)
	enteredSecond := make(chan struct{})
	releaseSecond := make(chan struct{})
	var releaseSecondClosed atomic.Bool
	defer closeSignal(releaseSecond, &releaseSecondClosed)
	enteredThird := make(chan struct{})

	q.AppendRange(0, 8192, []func() error{
		func() error {
			close(enteredFirst)
			<-releaseFirst
			return nil
		},
	})
	q.AppendRange(4096, 8192, []func() error{
		func() error {
			close(enteredSecond)
			<-releaseSecond
			return nil
		},
	})
	q.AppendRange(8192, 8192, []func() error{
		func() error {
			close(enteredThird)
			return nil
		},
	})

	done := make(chan error, 1)
	go func() {
		done <- q.Flush(context.Background())
	}()

	waitForSignal(t, enteredFirst, "first transitive-overlap mutation did not start")
	select {
	case err := <-done:
		t.Fatalf("flush completed before transitive overlap chain was released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	closeSignal(releaseFirst, &releaseFirstClosed)
	waitForSignal(t, enteredSecond, "second transitive-overlap mutation did not start after the first was released")
	select {
	case <-enteredThird:
		t.Fatal("third transitive-overlap mutation started before the second mutation finished")
	case <-time.After(20 * time.Millisecond):
	}

	closeSignal(releaseSecond, &releaseSecondClosed)
	require.NoError(t, waitForFlush(t, done))
}

func TestMutationQueueFlushClearsQueueOnError(t *testing.T) {
	q := newMutationQueue(4096)
	wantErr := errors.New("commit failed")
	var ranAfterFailure atomic.Bool

	q.AppendRange(0, 4096, []func() error{
		func() error { return wantErr },
	})
	q.AppendRange(0, 4096, []func() error{
		func() error {
			ranAfterFailure.Store(true)
			return nil
		},
	})

	err := q.Flush(context.Background())
	require.ErrorIs(t, err, wantErr)
	require.False(t, ranAfterFailure.Load(), "later same-block wave should not run after an earlier same-block failure")
	require.Equal(t, 0, q.Len(), "Flush must clear pending entries even on error")
}

func TestMutationQueueDrainIgnoresErrors(t *testing.T) {
	q := newMutationQueue(4096)
	var ranAfterFailure atomic.Bool

	q.AppendRange(0, 4096, []func() error{
		func() error { return errors.New("ignored") },
	})
	q.AppendRange(4096, 4096, []func() error{
		func() error {
			ranAfterFailure.Store(true)
			return nil
		},
	})

	q.Drain()
	require.True(t, ranAfterFailure.Load(), "Drain must continue after ignored commit errors")
	require.Equal(t, 0, q.Len())
}

func TestMutationQueueAppendCopiesCommitFns(t *testing.T) {
	q := newMutationQueue(4096)
	var ranOriginal atomic.Bool
	var ranReplacement atomic.Bool

	fns := []func() error{
		func() error {
			ranOriginal.Store(true)
			return nil
		},
	}
	q.AppendRange(0, 4096, fns)
	fns[0] = func() error {
		ranReplacement.Store(true)
		return nil
	}

	require.NoError(t, q.Flush(context.Background()))
	require.True(t, ranOriginal.Load())
	require.False(t, ranReplacement.Load())
}
