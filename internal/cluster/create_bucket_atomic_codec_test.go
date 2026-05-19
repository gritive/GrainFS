package cluster

import (
	"testing"
)

func TestEncodeDecodeMetaCreateBucketWithPolicyAttachCmd(t *testing.T) {
	tests := []struct {
		name         string
		bucket       string
		attachSA     string
		attachPolicy string
	}{
		{"with attach", "my-bucket", "sa-1", "readonly"},
		{"create only", "other-bucket", "", ""},
		{"bucket only attach sa empty policy", "b", "", "p"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := EncodeMetaCreateBucketWithPolicyAttachCmd(tc.bucket, tc.attachSA, tc.attachPolicy)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			gotBucket, gotSA, gotPolicy, err := decodeMetaCreateBucketWithPolicyAttachCmd(payload)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if gotBucket != tc.bucket {
				t.Errorf("bucket: got %q, want %q", gotBucket, tc.bucket)
			}
			if gotSA != tc.attachSA {
				t.Errorf("sa: got %q, want %q", gotSA, tc.attachSA)
			}
			if gotPolicy != tc.attachPolicy {
				t.Errorf("policy: got %q, want %q", gotPolicy, tc.attachPolicy)
			}
		})
	}
}

func TestDecodeMetaCreateBucketWithPolicyAttachCmd_EmptyPayload(t *testing.T) {
	_, _, _, err := decodeMetaCreateBucketWithPolicyAttachCmd([]byte{})
	if err == nil {
		t.Fatal("expected error for empty payload, got nil")
	}
}
