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
