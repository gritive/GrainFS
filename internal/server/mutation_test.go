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
