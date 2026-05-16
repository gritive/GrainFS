package storage

import (
	"testing"
)

// BenchmarkOperationsPlanForCallFastPath asserts that the cache-hit path of
// planForCall performs zero allocations. Regression-guards the lock-free
// publication pattern for the storage decorator capability plan: any future
// change that re-introduces a per-call slice or map allocation here will
// surface as non-zero allocs/op.
func BenchmarkOperationsPlanForCallFastPath(b *testing.B) {
	swappable := NewSwappableBackend(&aclNoCapabilityBackend{})
	ops := NewOperations(swappable)
	// Prime the cache.
	_ = ops.planForCall()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ops.planForCall()
	}
}

// BenchmarkOperationsPlanForCallFastPathNoGenSource covers the chain with no
// SwappableBackend (genSource == nil). The fast path should still be
// alloc-free and skip the Generation() call entirely.
func BenchmarkOperationsPlanForCallFastPathNoGenSource(b *testing.B) {
	ops := NewOperations(&aclNoCapabilityBackend{})
	_ = ops.planForCall()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ops.planForCall()
	}
}

// BenchmarkOperationsACLPlanForCallFastPath asserts the same property for the
// ACL-specific capability plan.
func BenchmarkOperationsACLPlanForCallFastPath(b *testing.B) {
	swappable := NewSwappableBackend(&aclNoCapabilityBackend{})
	ops := NewOperations(swappable)
	_ = ops.aclPlanForCall()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ops.aclPlanForCall()
	}
}
