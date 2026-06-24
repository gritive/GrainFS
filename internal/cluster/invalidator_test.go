package cluster

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testInvalidator struct {
	mu    sync.Mutex
	calls []string
}

func (t *testInvalidator) Invalidate(bucket, key string) {
	t.mu.Lock()
	t.calls = append(t.calls, bucket+"/"+key)
	t.mu.Unlock()
}

func TestRegistry_ConcurrentRegisterAndInvalidate(t *testing.T) {
	// This test would trigger the race detector without the mutex fix.
	r := NewRegistry()

	var wg sync.WaitGroup
	inv := &testInvalidator{}

	// Writer: concurrent Register calls
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Register("vol1", inv)
		}()
	}
	// Reader: concurrent InvalidateAll calls
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.InvalidateAll("bkt", "key")
		}()
	}
	wg.Wait()
}

func TestRegistry_InvalidateAll_CallsAllInvalidators(t *testing.T) {
	r := NewRegistry()
	inv1 := &testInvalidator{}
	inv2 := &testInvalidator{}
	r.Register("vol1", inv1)
	r.Register("vol2", inv2)

	r.InvalidateAll("mybkt", "mykey")

	assert.Len(t, inv1.calls, 1)
	assert.Equal(t, "mybkt/mykey", inv1.calls[0])
	assert.Len(t, inv2.calls, 1)
}

func TestRegistry_Unregister_RemovesInvalidator(t *testing.T) {
	r := NewRegistry()
	inv := &testInvalidator{}
	r.Register("vol1", inv)
	r.Unregister("vol1")

	r.InvalidateAll("mybkt", "mykey")

	assert.Empty(t, inv.calls)
	assert.Nil(t, r.GetInvalidator("vol1"))
}

func TestCacheInvalidatorFunc_Invalidates(t *testing.T) {
	var gotBucket, gotKey string
	inv := CacheInvalidatorFunc(func(bucket, key string) {
		gotBucket = bucket
		gotKey = key
	})

	inv.Invalidate("mybkt", "mykey")

	assert.Equal(t, "mybkt", gotBucket)
	assert.Equal(t, "mykey", gotKey)
}

func TestRegistry_InvalidateAll_EmptyRegistry(t *testing.T) {
	r := NewRegistry()
	// Should not panic and should be a no-op.
	r.InvalidateAll("bkt", "key")
}

func TestRegistry_GetInvalidators_ReturnsCopy(t *testing.T) {
	r := NewRegistry()
	inv1 := &testInvalidator{}
	r.Register("vol1", inv1)

	snap := r.GetInvalidators()
	assert.Len(t, snap, 1)

	// Mutating the snapshot must not affect the registry.
	delete(snap, "vol1")
	assert.Len(t, r.GetInvalidators(), 1, "registry should be unaffected by snapshot mutation")
}

func TestRegistry_GetInvalidator_ConcurrentRegister(t *testing.T) {
	r := NewRegistry()
	inv := &testInvalidator{}
	r.Register("vol1", inv)

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Register("vol2", &testInvalidator{})
		}()
	}
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := r.GetInvalidator("vol1")
			assert.NotNil(t, got)
		}()
	}
	wg.Wait()
}

// notifyOnApply no longer drives object-cache invalidation: the per-object FSM
// commands that once named an object key were retired (data-plane raft-free
// Slice 2), and object mutations now invalidate caches directly via the
// quorum-meta blob write path. A retired object slot (CommandType 3, formerly
// CmdPutObjectMeta) hits the default path in notifyOnApply and must NOT call any
// registered invalidator.
func TestDistributedBackend_RegisterCacheInvalidator_RetiredObjectSlotNotInvalidated(t *testing.T) {
	backend := &DistributedBackend{registry: NewRegistry()}
	inv := &testInvalidator{}
	backend.RegisterCacheInvalidator("s3-cache", inv)

	payload, err := encodeQuorumMetaBlob(PutObjectMetaCmd{Bucket: "mybkt", Key: "mykey"})
	require.NoError(t, err)
	raw, err := buildRawCommand(CommandType(3), payload)
	require.NoError(t, err)

	backend.notifyOnApply(raw)

	assert.Empty(t, inv.calls, "apply path must not invalidate object cache for the retired object slot")
}

func TestDistributedBackend_UnregisterCacheInvalidator_RemovesInvalidator(t *testing.T) {
	backend := &DistributedBackend{registry: NewRegistry()}
	inv := &testInvalidator{}
	backend.RegisterCacheInvalidator("s3-cache", inv)
	backend.UnregisterCacheInvalidator("s3-cache")

	// After Unregister the invalidator must not be called. Also note notifyOnApply
	// no longer drives object-cache invalidation at all (the per-object FSM commands
	// were retired in data-plane raft-free Slice 2), so a retired object slot
	// (CommandType 3) is a no-op on the apply hook regardless of registration.
	payload, err := encodeQuorumMetaBlob(PutObjectMetaCmd{Bucket: "mybkt", Key: "mykey"})
	require.NoError(t, err)
	raw, err := buildRawCommand(CommandType(3), payload)
	require.NoError(t, err)

	backend.notifyOnApply(raw)

	assert.Empty(t, inv.calls)
}

// notifyOnApply does NOT fan out object-cache invalidation to the registry or the
// legacy onApply callback for a retired object slot: the per-object FSM commands
// that once named an object key were retired (data-plane raft-free Slice 2), and
// object mutations now invalidate caches directly via the quorum-meta blob write
// path. A retired object slot (CommandType 3, formerly CmdPutObjectMeta) hits the
// default no-op path, so neither callback fires.
func TestDistributedBackend_NotifyOnApply_RetiredObjectSlotSkipsRegistryAndLegacy(t *testing.T) {
	backend := &DistributedBackend{registry: NewRegistry()}
	var mu sync.Mutex
	var calls []string
	backend.RegisterCacheInvalidator("s3-cache", CacheInvalidatorFunc(func(bucket, key string) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, "registry:"+bucket+"/"+key)
	}))
	backend.SetOnApply(func(cmdType CommandType, bucket, key string) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, "legacy:"+bucket+"/"+key)
	})

	payload, err := encodeQuorumMetaBlob(PutObjectMetaCmd{Bucket: "mybkt", Key: "mykey"})
	require.NoError(t, err)
	raw, err := buildRawCommand(CommandType(3), payload)
	require.NoError(t, err)

	backend.notifyOnApply(raw)

	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, calls, "apply hook must not invoke registry or legacy callbacks for the retired object slot")
}
