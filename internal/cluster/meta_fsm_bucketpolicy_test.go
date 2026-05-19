package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/iam/bucketpolicy"
)

// buildBucketPolicyPutCmd encodes a BucketPolicyPut MetaCmd.
func buildBucketPolicyPutCmd(t *testing.T, bucket string, docJSON []byte) []byte {
	t.Helper()
	payload, err := EncodeBucketPolicyPutPayload(bucket, docJSON)
	if err != nil {
		t.Fatalf("EncodeBucketPolicyPutPayload: %v", err)
	}
	cmd, err := encodeMetaCmd(MetaCmdTypeBucketPolicyPut, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(BucketPolicyPut): %v", err)
	}
	return cmd
}

// buildBucketPolicyDeleteCmd encodes a BucketPolicyDelete MetaCmd.
func buildBucketPolicyDeleteCmd(t *testing.T, bucket string) []byte {
	t.Helper()
	payload, err := EncodeBucketPolicyDeletePayload(bucket)
	if err != nil {
		t.Fatalf("EncodeBucketPolicyDeletePayload: %v", err)
	}
	cmd, err := encodeMetaCmd(MetaCmdTypeBucketPolicyDelete, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(BucketPolicyDelete): %v", err)
	}
	return cmd
}

// TestMetaFSM_Apply_BucketPolicyPut_StoresPolicy verifies that applying a
// BucketPolicyPut cmd persists the document in the bucket policy store.
func TestMetaFSM_Apply_BucketPolicyPut_StoresPolicy(t *testing.T) {
	store := bucketpolicy.NewInMemoryStore()

	f := NewMetaFSM()
	f.SetBucketPolicyStore(store)

	raw := []byte(`{"Statement":[{"Effect":"Allow"}]}`)
	if err := f.applyCmd(buildBucketPolicyPutCmd(t, "my-bucket", raw)); err != nil {
		t.Fatalf("applyCmd BucketPolicyPut: %v", err)
	}

	got, err := store.Get(context.Background(), "my-bucket")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(raw) {
		t.Fatalf("doc mismatch: got %q, want %q", got, raw)
	}
}

// TestMetaFSM_Apply_BucketPolicyDelete_RemovesPolicy verifies that applying a
// BucketPolicyDelete cmd removes the stored document.
func TestMetaFSM_Apply_BucketPolicyDelete_RemovesPolicy(t *testing.T) {
	store := bucketpolicy.NewInMemoryStore()

	f := NewMetaFSM()
	f.SetBucketPolicyStore(store)

	raw := []byte(`{"x":1}`)
	if err := f.applyCmd(buildBucketPolicyPutCmd(t, "del-bucket", raw)); err != nil {
		t.Fatalf("applyCmd BucketPolicyPut: %v", err)
	}
	if err := f.applyCmd(buildBucketPolicyDeleteCmd(t, "del-bucket")); err != nil {
		t.Fatalf("applyCmd BucketPolicyDelete: %v", err)
	}

	_, err := store.Get(context.Background(), "del-bucket")
	if !errors.Is(err, bucketpolicy.ErrNotFound) {
		t.Fatalf("after delete: got %v, want ErrNotFound", err)
	}
}

// TestMetaFSM_Apply_BucketPolicyPut_NilStore_SafeNoop verifies nil-safe no-op.
func TestMetaFSM_Apply_BucketPolicyPut_NilStore_SafeNoop(t *testing.T) {
	f := NewMetaFSM()
	if err := f.applyCmd(buildBucketPolicyPutCmd(t, "b", []byte(`{}`))); err != nil {
		t.Fatalf("BucketPolicyPut with nil store: got error %v, want nil", err)
	}
}

// TestMetaFSM_Apply_BucketPolicyDelete_NilStore_SafeNoop verifies nil-safe no-op.
func TestMetaFSM_Apply_BucketPolicyDelete_NilStore_SafeNoop(t *testing.T) {
	f := NewMetaFSM()
	if err := f.applyCmd(buildBucketPolicyDeleteCmd(t, "b")); err != nil {
		t.Fatalf("BucketPolicyDelete with nil store: got error %v, want nil", err)
	}
}
