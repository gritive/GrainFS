package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockInvalidator is a mock CacheInvalidator for testing.
type mockInvalidator struct {
	called bool
	bucket string
	key    string
}

func (m *mockInvalidator) Invalidate(bucket, key string) {
	m.called = true
	m.bucket = bucket
	m.key = key
}

// TestRegistryRegister verifies that invalidators can be registered.
func TestRegistryRegister(t *testing.T) {
	reg := NewRegistry()
	assert.NotNil(t, reg)

	inv := &mockInvalidator{}
	reg.Register("volume1", inv)

	// Verify we can retrieve by calling InvalidateAll
	reg.InvalidateAll("test-bucket", "test-key")

	assert.True(t, inv.called, "invalidator should be called")
	assert.Equal(t, "test-bucket", inv.bucket)
	assert.Equal(t, "test-key", inv.key)
}

// TestRegistryMultipleInvalidators verifies that all invalidators are called.
func TestRegistryMultipleInvalidators(t *testing.T) {
	reg := NewRegistry()

	inv1 := &mockInvalidator{}
	inv2 := &mockInvalidator{}
	inv3 := &mockInvalidator{}

	reg.Register("volume1", inv1)
	reg.Register("volume2", inv2)
	reg.Register("volume3", inv3)

	// Call InvalidateAll
	reg.InvalidateAll("mybucket", "mykey")

	// All invalidators should be called
	assert.True(t, inv1.called)
	assert.True(t, inv2.called)
	assert.True(t, inv3.called)

	// All should receive same bucket/key
	assert.Equal(t, "mybucket", inv1.bucket)
	assert.Equal(t, "mybucket", inv2.bucket)
	assert.Equal(t, "mybucket", inv3.bucket)
	assert.Equal(t, "mykey", inv1.key)
	assert.Equal(t, "mykey", inv2.key)
	assert.Equal(t, "mykey", inv3.key)
}

// TestRegistryEmpty verifies that InvalidateAll works with no invalidators.
func TestRegistryEmpty(t *testing.T) {
	reg := NewRegistry()

	// Should not panic
	reg.InvalidateAll("bucket", "key")
}
