package cluster

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
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
