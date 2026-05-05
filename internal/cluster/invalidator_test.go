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

func TestDistributedBackend_RegisterCacheInvalidator_NotifiedOnApply(t *testing.T) {
	backend := &DistributedBackend{registry: NewRegistry()}
	inv := &testInvalidator{}
	backend.RegisterCacheInvalidator("s3-cache", inv)

	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{Bucket: "mybkt", Key: "mykey"})
	require.NoError(t, err)

	backend.notifyOnApply(raw)

	assert.Equal(t, []string{"mybkt/mykey"}, inv.calls)
}

func TestDistributedBackend_UnregisterCacheInvalidator_RemovesInvalidator(t *testing.T) {
	backend := &DistributedBackend{registry: NewRegistry()}
	inv := &testInvalidator{}
	backend.RegisterCacheInvalidator("s3-cache", inv)
	backend.UnregisterCacheInvalidator("s3-cache")

	raw, err := EncodeCommand(CmdDeleteObject, DeleteObjectCmd{Bucket: "mybkt", Key: "mykey"})
	require.NoError(t, err)

	backend.notifyOnApply(raw)

	assert.Empty(t, inv.calls)
}

func TestDistributedBackend_NotifyOnApply_CallsLegacyCallbackAfterRegistry(t *testing.T) {
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

	raw, err := EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{Bucket: "mybkt", Key: "mykey"})
	require.NoError(t, err)

	backend.notifyOnApply(raw)

	assert.Equal(t, []string{"registry:mybkt/mykey", "legacy:mybkt/mykey"}, calls)
}
