package storage

import (
	"context"
	"testing"
)

func TestIsVFSBucket(t *testing.T) {
	tests := []struct {
		bucket string
		want   bool
	}{
		{"__grainfs_vfs_default", true},
		{"__grainfs_vfs_volA", true},
		{"my-app-bucket", false},
		{"", false},
		{"__grainfs_vfs_", true},
	}
	for _, tt := range tests {
		if got := IsVFSBucket(tt.bucket); got != tt.want {
			t.Errorf("IsVFSBucket(%q) = %v, want %v", tt.bucket, got, tt.want)
		}
	}
}

func TestVFSBucketPrefixConst(t *testing.T) {
	if VFSBucketPrefix != "__grainfs_vfs_" {
		t.Errorf("VFSBucketPrefix = %q, want %q", VFSBucketPrefix, "__grainfs_vfs_")
	}
}

func TestIsInternalBucket(t *testing.T) {
	tests := []struct {
		bucket string
		want   bool
	}{
		{"__grainfs_nfs4", true},
		{"__grainfs_vfs_default", true},
		{"__grainfs_volumes", true},
		{"my-bucket", false},
		{"", false},
		{"__other_", false},
	}
	for _, tt := range tests {
		if got := IsInternalBucket(tt.bucket); got != tt.want {
			t.Errorf("IsInternalBucket(%q) = %v, want %v", tt.bucket, got, tt.want)
		}
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
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("got %d, want 0", n)
	}
}

func TestCountObjects_Three(t *testing.T) {
	backend := &countingWalkBackend{
		objects: []*Object{{Key: "a"}, {Key: "b"}, {Key: "c"}},
	}
	ops := NewOperations(backend)
	n, err := ops.CountObjects(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("got %d, want 3", n)
	}
}
