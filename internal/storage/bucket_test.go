package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsInternalBucket(t *testing.T) {
	tests := []struct {
		bucket string
		want   bool
	}{
		// __grainfs_nfs4 is a normal internal bucket — NFS support removed.
		{"__grainfs_nfs4", true},
		{"__grainfs_vfs_default", true},
		{"__grainfs_test_internal", true},
		{"my-bucket", false},
		{"", false},
		{"__other_", false},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, IsInternalBucket(tt.bucket), "IsInternalBucket(%q)", tt.bucket)
	}
}

type countingWalkBackend struct {
	basicBackend
	objects []*Object
}

func (b *countingWalkBackend) WalkObjects(_ context.Context, _, _ string, fn func(*Object) error) error {
	for _, obj := range b.objects {
		if err := fn(obj); err != nil {
			return err
		}
	}
	return nil
}

func TestCountObjects_Empty(t *testing.T) {
	ops := NewOperations(&countingWalkBackend{})
	n, err := ops.CountObjects(context.Background(), "b")
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestCountObjects_Three(t *testing.T) {
	backend := &countingWalkBackend{
		objects: []*Object{{Key: "a"}, {Key: "b"}, {Key: "c"}},
	}
	ops := NewOperations(backend)
	n, err := ops.CountObjects(context.Background(), "b")
	require.NoError(t, err)
	require.Equal(t, int64(3), n)
}
