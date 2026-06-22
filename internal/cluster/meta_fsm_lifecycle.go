package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/lifecycle"
)

// SetLifecycle wires the lifecycle store into the MetaFSM. Must be called
// before raft Start so apply does not race with replay.
func (f *MetaFSM) SetLifecycle(store *lifecycle.Store) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lifecycleStore = store
}

func (f *MetaFSM) applyBucketLifecyclePut(payload []byte) error {
	if f.lifecycleStore == nil {
		return fmt.Errorf("meta_fsm: lifecycle store not wired")
	}
	bucket, raw, err := lifecycle.DecodePutPayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: BucketLifecyclePut: %w", err)
	}
	return f.lifecycleStore.PutRaw(bucket, raw)
}

func (f *MetaFSM) applyBucketLifecycleDelete(payload []byte) error {
	if f.lifecycleStore == nil {
		return fmt.Errorf("meta_fsm: lifecycle store not wired")
	}
	bucket, err := lifecycle.DecodeDeletePayload(payload)
	if err != nil {
		return fmt.Errorf("meta_fsm: BucketLifecycleDelete: %w", err)
	}
	return f.lifecycleStore.Delete(bucket)
}
