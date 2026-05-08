package server

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

// recordingObserver is the single test double used across every test in
// this package (broker dispatch, regression tests, ordering tests). Fields:
//   - writes/deletes/copies/creates/dropped: per-method capture
//   - name + log: optional ordering hooks (set both for ordering tests,
//     leave nil for capture-only tests)
type recordingObserver struct {
	name string    // optional, used by ordering tests
	log  *[]string // optional shared slice, appended as "{name}:{action}"

	writes  []writeCall
	deletes []deleteCall
	copies  []copyCall
	creates []string
	dropped []string
}

type writeCall struct {
	bucket, key string
	result      *storage.PutObjectResult
}
type deleteCall struct {
	bucket, key string
	result      *storage.DeleteObjectResult
}
type copyCall struct {
	srcBucket, srcKey, dstBucket, dstKey string
	result                               *storage.CopyObjectResult
}

func (r *recordingObserver) trace(action string) {
	if r.log != nil {
		*r.log = append(*r.log, r.name+":"+action)
	}
}

func (r *recordingObserver) OnObjectWrite(_ context.Context, bucket, key string, res *storage.PutObjectResult) {
	r.writes = append(r.writes, writeCall{bucket, key, res})
	r.trace("write")
}
func (r *recordingObserver) OnObjectDelete(_ context.Context, bucket, key string, res *storage.DeleteObjectResult) {
	r.deletes = append(r.deletes, deleteCall{bucket, key, res})
	r.trace("delete")
}
func (r *recordingObserver) OnObjectCopy(_ context.Context, srcBucket, srcKey, dstBucket, dstKey string, res *storage.CopyObjectResult) {
	r.copies = append(r.copies, copyCall{srcBucket, srcKey, dstBucket, dstKey, res})
	r.trace("copy")
}
func (r *recordingObserver) OnBucketCreate(_ context.Context, bucket string) {
	r.creates = append(r.creates, bucket)
	r.trace("createBucket")
}
func (r *recordingObserver) OnBucketDelete(_ context.Context, bucket string) {
	r.dropped = append(r.dropped, bucket)
	r.trace("deleteBucket")
}

// TestMutationObserverInterfaceCompiles is a structural test. If the
// interface methods change shape, this file no longer compiles. The act
// of compiling is the assertion.
func TestMutationObserverInterfaceCompiles(t *testing.T) {
	var obs MutationObserver = &recordingObserver{}
	_ = obs
}

// TestMutationBrokerFansOutToAllObservers exercises every interface
// method to guard against accidental copy-paste mistakes inside the
// broker dispatch loop (e.g., OnBucketDelete forgetting to iterate).
func TestMutationBrokerFansOutToAllObservers(t *testing.T) {
	a, b := &recordingObserver{}, &recordingObserver{}
	br := NewMutationBroker(a, b)
	ctx := context.Background()

	br.OnObjectWrite(ctx, "buck", "k", nil)
	br.OnObjectDelete(ctx, "buck", "k", nil)
	br.OnObjectCopy(ctx, "src-b", "src-k", "dst-b", "dst-k", nil)
	br.OnBucketCreate(ctx, "newbuck")
	br.OnBucketDelete(ctx, "oldbuck")

	for label, o := range map[string]*recordingObserver{"a": a, "b": b} {
		if len(o.writes) != 1 || o.writes[0].bucket != "buck" || o.writes[0].key != "k" {
			t.Fatalf("%s.writes = %+v, want one entry for buck/k", label, o.writes)
		}
		if len(o.deletes) != 1 || o.deletes[0].bucket != "buck" {
			t.Fatalf("%s.deletes = %+v", label, o.deletes)
		}
		if len(o.copies) != 1 || o.copies[0].dstBucket != "dst-b" || o.copies[0].dstKey != "dst-k" {
			t.Fatalf("%s.copies = %+v", label, o.copies)
		}
		if len(o.creates) != 1 || o.creates[0] != "newbuck" {
			t.Fatalf("%s.creates = %+v", label, o.creates)
		}
		if len(o.dropped) != 1 || o.dropped[0] != "oldbuck" {
			t.Fatalf("%s.dropped = %+v", label, o.dropped)
		}
	}
}

func TestMutationBrokerNoObserversIsNoop(t *testing.T) {
	br := NewMutationBroker()
	br.OnObjectWrite(context.Background(), "b", "k", nil)
	br.OnObjectDelete(context.Background(), "b", "k", nil)
	br.OnObjectCopy(context.Background(), "b", "s", "b", "d", nil)
	br.OnBucketCreate(context.Background(), "b")
	br.OnBucketDelete(context.Background(), "b")
	// no panic, no observers, no assertion needed
}

func TestMutationBrokerNilSafe(t *testing.T) {
	// A nil *MutationBroker must not panic; handlers may run before the
	// broker is wired in tests that bypass NewServer.
	var br *MutationBroker
	br.OnObjectWrite(context.Background(), "b", "k", nil) // must not panic
}

func TestMutationBrokerPreservesRegistrationOrder(t *testing.T) {
	var order []string
	a := &recordingObserver{name: "a", log: &order}
	b := &recordingObserver{name: "b", log: &order}
	br := NewMutationBroker(a, b)
	br.OnObjectWrite(context.Background(), "x", "y", nil)
	if len(order) != 2 || order[0] != "a:write" || order[1] != "b:write" {
		t.Fatalf("order = %v, want [a:write b:write]", order)
	}
}

func TestMetricsObserverWriteDoesNotPanic(t *testing.T) {
	// Smoke test: observer correctly delegates to recordObjectWriteMetrics
	// without panicking. Pure-delta correctness is covered by
	// object_metrics_test.go (objectWriteMetricDelta tests). End-to-end
	// gauge mutation is exercised by handler-level integration tests.
	res := &storage.PutObjectResult{
		Object: storage.ObjectFacts{Size: 100},
	}
	obs := newMetricsObserver()
	obs.OnObjectWrite(context.Background(), "b", "k", res)
}

func TestMetricsObserverHandlesNilResult(t *testing.T) {
	// Defensive: handlers should never call observer with nil result, but
	// the observer must not panic if invariant is violated.
	obs := newMetricsObserver()
	obs.OnObjectWrite(context.Background(), "b", "k", nil)
	obs.OnObjectDelete(context.Background(), "b", "k", nil)
	obs.OnObjectCopy(context.Background(), "b", "s", "b", "d", nil)
}

func TestMetricsObserverBucketLifecycleIsNoop(t *testing.T) {
	obs := newMetricsObserver()
	obs.OnBucketCreate(context.Background(), "b")
	obs.OnBucketDelete(context.Background(), "b") // must not panic
}
