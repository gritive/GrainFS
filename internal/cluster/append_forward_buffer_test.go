package cluster

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAppendForwardBufferReserveRelease(t *testing.T) {
	sem := newAppendForwardBuffer(1024)

	if err := sem.Acquire(context.Background(), 600); err != nil {
		t.Fatalf("first Acquire(600): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := sem.Acquire(ctx, 500)
	if !errors.Is(err, ErrForwardBufferFull) {
		t.Fatalf("second Acquire(500) err = %v; want ErrForwardBufferFull", err)
	}
	sem.Release(600)
	if err := sem.Acquire(context.Background(), 500); err != nil {
		t.Fatalf("post-release Acquire(500): %v", err)
	}
	if got := sem.InflightBytes(); got != 500 {
		t.Errorf("InflightBytes = %d; want 500", got)
	}
	sem.Release(500)
}

func TestAppendForwardBufferOversize(t *testing.T) {
	sem := newAppendForwardBuffer(1024)
	err := sem.Acquire(context.Background(), 2048)
	if !errors.Is(err, ErrForwardBufferFull) {
		t.Fatalf("oversize Acquire err = %v; want ErrForwardBufferFull", err)
	}
}
