package cluster

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestCoalesceWorkerEnqueueAndProcess(t *testing.T) {
	var processed []coalesceJob
	var mu sync.Mutex
	w := newCoalesceWorker(8, func(_ context.Context, job coalesceJob) error {
		mu.Lock()
		defer mu.Unlock()
		processed = append(processed, job)
		return nil
	})
	w.Start(context.Background())
	defer w.Stop()

	w.Enqueue(coalesceJob{Bucket: "b", Key: "k1"})
	w.Enqueue(coalesceJob{Bucket: "b", Key: "k2"})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(processed)
		mu.Unlock()
		if n == 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("not processed in time: %+v", processed)
}

func TestCoalesceWorkerDedup(t *testing.T) {
	block := make(chan struct{})
	var seen []string
	var mu sync.Mutex
	w := newCoalesceWorker(8, func(_ context.Context, job coalesceJob) error {
		<-block
		mu.Lock()
		seen = append(seen, job.Key)
		mu.Unlock()
		return nil
	})
	w.Start(context.Background())
	defer w.Stop()

	// Same (bucket, key) enqueued 3x while job is blocked → coalesced into 1
	w.Enqueue(coalesceJob{Bucket: "b", Key: "k1"})
	w.Enqueue(coalesceJob{Bucket: "b", Key: "k1"})
	w.Enqueue(coalesceJob{Bucket: "b", Key: "k1"})
	close(block)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) > 2 { // 1 in-flight + at most 1 pending OK; > 2 means dedup failed
		t.Fatalf("expected dedup, got %d runs: %v", len(seen), seen)
	}
}
