package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectMetaRMWLock_StablePerKey(t *testing.T) {
	b := newTestDistributedBackend(t)
	l1 := b.objectMetaRMWLock("bkt", "k")
	l2 := b.objectMetaRMWLock("bkt", "k")
	require.Same(t, l1, l2, "same (bucket,key) must return the same mutex")
	require.NotSame(t, l1, b.objectMetaRMWLock("bkt", "other"))

	// Lock key uses "\x00" separator (matching shardLocks) so (bucket,key)
	// pairs that would alias under a "/" separator stay distinct: object keys
	// may contain "/".
	require.NotSame(t, b.objectMetaRMWLock("a", "b/c"), b.objectMetaRMWLock("a/b", "c"))
}
