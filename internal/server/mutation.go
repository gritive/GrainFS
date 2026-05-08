package server

import (
	"context"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/storage"
)

// MutationObserver receives notifications after a successful S3-level
// state-changing operation. The broker (see MutationBroker) fans out to
// every registered observer; each observer decides its own sync/async
// semantics. Methods MUST NOT block the request hot path beyond what the
// observer is documented to do.
//
// Adding a new mutation method here forces every observer (including any
// future ones) to recompile, which is the contract: "if you observe
// mutations, you must handle every kind."
type MutationObserver interface {
	OnObjectWrite(ctx context.Context, bucket, key string, result *storage.PutObjectResult)
	OnObjectDelete(ctx context.Context, bucket, key string, result *storage.DeleteObjectResult)
	OnObjectCopy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, result *storage.CopyObjectResult)
	OnBucketCreate(ctx context.Context, bucket string)
	OnBucketDelete(ctx context.Context, bucket string)
}

// MutationBroker dispatches mutation notifications to a fixed list of
// observers in registration order. The broker itself does no work and
// never errors; per-observer failure semantics are owned by the observer.
//
// Methods on a nil *MutationBroker are no-ops, which keeps tests that
// construct a Server piecemeal from panicking.
type MutationBroker struct {
	observers []MutationObserver
}

// NewMutationBroker returns a broker that fans out to the given observers
// in argument order. Pass zero observers for a no-op broker.
func NewMutationBroker(observers ...MutationObserver) *MutationBroker {
	return &MutationBroker{observers: observers}
}

func (b *MutationBroker) OnObjectWrite(ctx context.Context, bucket, key string, r *storage.PutObjectResult) {
	if b == nil {
		return
	}
	for _, o := range b.observers {
		o.OnObjectWrite(ctx, bucket, key, r)
	}
}

func (b *MutationBroker) OnObjectDelete(ctx context.Context, bucket, key string, r *storage.DeleteObjectResult) {
	if b == nil {
		return
	}
	for _, o := range b.observers {
		o.OnObjectDelete(ctx, bucket, key, r)
	}
}

func (b *MutationBroker) OnObjectCopy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, r *storage.CopyObjectResult) {
	if b == nil {
		return
	}
	for _, o := range b.observers {
		o.OnObjectCopy(ctx, srcBucket, srcKey, dstBucket, dstKey, r)
	}
}

func (b *MutationBroker) OnBucketCreate(ctx context.Context, bucket string) {
	if b == nil {
		return
	}
	for _, o := range b.observers {
		o.OnBucketCreate(ctx, bucket)
	}
}

func (b *MutationBroker) OnBucketDelete(ctx context.Context, bucket string) {
	if b == nil {
		return
	}
	for _, o := range b.observers {
		o.OnBucketDelete(ctx, bucket)
	}
}

// metricsObserver synchronously updates Prometheus counters for object
// write/delete/copy. Bucket lifecycle has no metric (counts are derived
// from existing bucket-level gauges populated elsewhere).
type metricsObserver struct{}

func newMetricsObserver() *metricsObserver { return &metricsObserver{} }

func (m *metricsObserver) OnObjectWrite(_ context.Context, _, _ string, r *storage.PutObjectResult) {
	if r == nil {
		return
	}
	recordObjectWriteMetrics(r.Previous, r.Object.Size)
}

func (m *metricsObserver) OnObjectDelete(_ context.Context, _, _ string, r *storage.DeleteObjectResult) {
	if r == nil {
		return
	}
	recordObjectDeleteMetrics(r.Previous)
}

func (m *metricsObserver) OnObjectCopy(_ context.Context, _, _, _, _ string, r *storage.CopyObjectResult) {
	if r == nil {
		return
	}
	recordObjectWriteMetrics(r.Previous, r.Object.Size)
}

func (m *metricsObserver) OnBucketCreate(context.Context, string) {}
func (m *metricsObserver) OnBucketDelete(context.Context, string) {}

// eventObserver enqueues mutation events to the event store via an
// injected emit function. The emit function is expected to be
// non-blocking (existing s.emitEvent uses a bounded channel with
// drop-on-full). Same closure-injection pattern as heal_emitter.
type eventObserver struct {
	emit func(eventstore.Event)
}

func newEventObserver(emit func(eventstore.Event)) *eventObserver {
	return &eventObserver{emit: emit}
}

func (o *eventObserver) OnObjectWrite(_ context.Context, bucket, key string, r *storage.PutObjectResult) {
	if o.emit == nil {
		return
	}
	var size int64
	if r != nil {
		size = r.Object.Size
	}
	o.emit(eventstore.Event{
		Type:   eventstore.EventTypeS3,
		Action: eventstore.EventActionPut,
		Bucket: bucket,
		Key:    key,
		Size:   size,
	})
}

func (o *eventObserver) OnObjectDelete(_ context.Context, bucket, key string, _ *storage.DeleteObjectResult) {
	if o.emit == nil {
		return
	}
	o.emit(eventstore.Event{
		Type:   eventstore.EventTypeS3,
		Action: eventstore.EventActionDelete,
		Bucket: bucket,
		Key:    key,
	})
}

func (o *eventObserver) OnObjectCopy(_ context.Context, _, _, dstBucket, dstKey string, r *storage.CopyObjectResult) {
	if o.emit == nil {
		return
	}
	var size int64
	if r != nil {
		size = r.Object.Size
	}
	// Copy surfaces as Put at the destination, matching how legacy
	// PutObject handlers emit. Source bucket/key are intentionally
	// not encoded — eventstore.Event has no source field.
	o.emit(eventstore.Event{
		Type:   eventstore.EventTypeS3,
		Action: eventstore.EventActionPut,
		Bucket: dstBucket,
		Key:    dstKey,
		Size:   size,
	})
}

func (o *eventObserver) OnBucketCreate(_ context.Context, bucket string) {
	if o.emit == nil {
		return
	}
	o.emit(eventstore.Event{
		Type:   eventstore.EventTypeS3,
		Action: eventstore.EventActionCreateBucket,
		Bucket: bucket,
	})
}

func (o *eventObserver) OnBucketDelete(_ context.Context, bucket string) {
	if o.emit == nil {
		return
	}
	o.emit(eventstore.Event{
		Type:   eventstore.EventTypeS3,
		Action: eventstore.EventActionDeleteBucket,
		Bucket: bucket,
	})
}
