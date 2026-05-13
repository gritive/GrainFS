package nfs4server

import "testing"

func TestExtractBucketAndKey(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantBucket string
		wantKey    string
	}{
		{"empty", "", "__grainfs_nfs4", ""},
		{"root", "/", "__grainfs_nfs4", ""},
		{"single segment no slash", "file.txt", "__grainfs_nfs4", "file.txt"},
		{"leading slash single", "/file.txt", "__grainfs_nfs4", "file.txt"},
		{"nested", "/dir/file.txt", "__grainfs_nfs4", "dir/file.txt"},
		{"deep nested", "/a/b/c/d.txt", "__grainfs_nfs4", "a/b/c/d.txt"},
		{"trailing slash", "/dir/", "__grainfs_nfs4", "dir/"},
		{"double slash sanitized", "/dir//file", "__grainfs_nfs4", "dir//file"},
		{"sidecar meta key", "__meta/foo", "__grainfs_nfs4", "__meta/foo"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBucket, gotKey := extractBucketAndKey(tt.path)
			if gotBucket != tt.wantBucket || gotKey != tt.wantKey {
				t.Errorf("extractBucketAndKey(%q) = (%q, %q), want (%q, %q)",
					tt.path, gotBucket, gotKey, tt.wantBucket, tt.wantKey)
			}
		})
	}
}
