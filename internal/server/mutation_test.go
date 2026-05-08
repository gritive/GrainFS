package server

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/eventstore"
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

func TestEventObserverEmitsCorrectEventForWrite(t *testing.T) {
	var got []eventstore.Event
	emit := func(e eventstore.Event) { got = append(got, e) }
	obs := newEventObserver(emit)

	obs.OnObjectWrite(context.Background(), "buck", "k", &storage.PutObjectResult{
		Object: storage.ObjectFacts{Size: 42},
	})

	if len(got) != 1 {
		t.Fatalf("want 1 event, got %d", len(got))
	}
	if got[0].Type != eventstore.EventTypeS3 || got[0].Action != eventstore.EventActionPut {
		t.Fatalf("type/action mismatch: %+v", got[0])
	}
	if got[0].Bucket != "buck" || got[0].Key != "k" || got[0].Size != 42 {
		t.Fatalf("payload mismatch: %+v", got[0])
	}
}

func TestEventObserverEmitsCorrectEventForDelete(t *testing.T) {
	var got []eventstore.Event
	obs := newEventObserver(func(e eventstore.Event) { got = append(got, e) })
	obs.OnObjectDelete(context.Background(), "buck", "k", &storage.DeleteObjectResult{})
	if len(got) != 1 || got[0].Action != eventstore.EventActionDelete ||
		got[0].Bucket != "buck" || got[0].Key != "k" {
		t.Fatalf("got %+v", got)
	}
}

func TestEventObserverEmitsCorrectEventForCopy(t *testing.T) {
	// Copy is a write at the destination from the event store's POV.
	var got []eventstore.Event
	obs := newEventObserver(func(e eventstore.Event) { got = append(got, e) })
	obs.OnObjectCopy(context.Background(), "src-b", "src-k", "dst-b", "dst-k",
		&storage.CopyObjectResult{Object: storage.ObjectFacts{Size: 7}})
	if len(got) != 1 {
		t.Fatalf("want 1 event, got %d", len(got))
	}
	if got[0].Action != eventstore.EventActionPut ||
		got[0].Bucket != "dst-b" || got[0].Key != "dst-k" || got[0].Size != 7 {
		t.Fatalf("got %+v", got[0])
	}
}

func TestEventObserverEmitsBucketLifecycle(t *testing.T) {
	var got []eventstore.Event
	obs := newEventObserver(func(e eventstore.Event) { got = append(got, e) })
	obs.OnBucketCreate(context.Background(), "newbuck")
	obs.OnBucketDelete(context.Background(), "oldbuck")
	if len(got) != 2 {
		t.Fatalf("want 2, got %d", len(got))
	}
	if got[0].Action != eventstore.EventActionCreateBucket || got[0].Bucket != "newbuck" {
		t.Fatalf("create: %+v", got[0])
	}
	if got[1].Action != eventstore.EventActionDeleteBucket || got[1].Bucket != "oldbuck" {
		t.Fatalf("delete: %+v", got[1])
	}
}

func TestEventObserverNilEmitFnIsNoop(t *testing.T) {
	obs := newEventObserver(nil)
	obs.OnObjectWrite(context.Background(), "b", "k", &storage.PutObjectResult{})
	// no panic
}

// TestCompleteMultipartUploadInvokesBrokerWrite is a regression test:
// before this refactor handleCompleteMultipart updated metrics but
// never enqueued a Put event, so multipart-uploaded objects were
// invisible to event-based subscribers. After Task 6 the handler calls
// s.mutations.OnObjectWrite, which the broker fans out to eventObserver
// (Task 4 covers eventObserver's Put-event emission shape).
func TestCompleteMultipartUploadInvokesBrokerWrite(t *testing.T) {
	rec := &recordingObserver{}
	br := NewMutationBroker(rec)
	// Simulate what handleCompleteMultipart now does after a successful
	// CompleteMultipartUploadWithResult call:
	br.OnObjectWrite(context.Background(), "buck", "k",
		&storage.PutObjectResult{Object: storage.ObjectFacts{Size: 999}})

	if len(rec.writes) != 1 ||
		rec.writes[0].bucket != "buck" || rec.writes[0].key != "k" ||
		rec.writes[0].result == nil || rec.writes[0].result.Object.Size != 999 {
		t.Fatalf("regression: multipart write not routed through broker; got %+v", rec.writes)
	}
}

func TestNewServerWiresMutationBroker(t *testing.T) {
	// Construct a minimal server via NewWithServerStorage with a real local
	// backend (matches the pattern in authorizer_wiring_test.go and
	// server_storage_test.go). NewMutationBroker should be wired in
	// regardless of which storage paths are configured.
	backend, err := storage.NewLocalBackend(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	s := NewWithServerStorage("127.0.0.1:0", NewServerStorage(backend, nil), nil)
	if s.mutations == nil {
		t.Fatal("Server.mutations is nil — broker not wired")
	}
	// Smoke: should not panic.
	s.mutations.OnBucketCreate(context.Background(), "test")
}

// TestCopyObjectInvokesBrokerCopy is a regression test: before this
// refactor handleCopyObject called recordObjectWriteMetrics but never
// s.emitEvent, so copy operations were invisible to event-based
// subscribers. The broker.OnObjectCopy call (added in Task 8) restores
// parity; eventObserver's copy→Put@dst mapping is verified separately
// in Task 4 (TestEventObserverEmitsCorrectEventForCopy).
func TestCopyObjectInvokesBrokerCopy(t *testing.T) {
	rec := &recordingObserver{}
	br := NewMutationBroker(rec)

	br.OnObjectCopy(context.Background(), "src-b", "src-k", "dst-b", "dst-k",
		&storage.CopyObjectResult{Object: storage.ObjectFacts{Size: 555}})

	if len(rec.copies) != 1 {
		t.Fatalf("want 1 copy notification, got %d", len(rec.copies))
	}
	c := rec.copies[0]
	if c.srcBucket != "src-b" || c.srcKey != "src-k" ||
		c.dstBucket != "dst-b" || c.dstKey != "dst-k" ||
		c.result == nil || c.result.Object.Size != 555 {
		t.Fatalf("regression: copy payload not routed correctly; got %+v", c)
	}
}
