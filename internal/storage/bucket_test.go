package storage

import "testing"

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
