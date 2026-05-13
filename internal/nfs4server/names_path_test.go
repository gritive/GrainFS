package nfs4server

import "testing"

func TestExtractBucketAndKey(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantBucket string
		wantKey    string
	}{
		{"empty", "", "", ""},
		{"root", "/", "", ""},
		{"bucket only", "/bucket", "bucket", ""},
		{"bucket trailing slash", "/bucket/", "bucket", ""},
		{"bucket + leaf", "/bucket/key", "bucket", "key"},
		{"bucket + nested", "/bucket/a/b/c", "bucket", "a/b/c"},
		{"bucket + deep + trailing", "/bucket/a/b/", "bucket", "a/b/"},
		{"double slash mid", "/bucket//x", "bucket", "/x"},
		{"name with dot", "/my.bucket/key", "my.bucket", "key"},
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
