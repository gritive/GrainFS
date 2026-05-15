package server

import (
	"context"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/storage"
)

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
	// not encoded because eventstore.Event has no source field.
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
