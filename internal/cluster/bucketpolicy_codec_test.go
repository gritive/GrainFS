package cluster

import (
	"testing"
)

func TestBucketPolicyCodec_PutRoundTrip(t *testing.T) {
	raw := []byte(`{"Statement":[{"Effect":"Allow"}]}`)
	data, err := EncodeBucketPolicyPutPayload("my-bucket", raw)
	if err != nil {
		t.Fatalf("EncodeBucketPolicyPutPayload: %v", err)
	}
	gotBucket, gotDoc, err := DecodeBucketPolicyPutPayload(data)
	if err != nil {
		t.Fatalf("DecodeBucketPolicyPutPayload: %v", err)
	}
	if gotBucket != "my-bucket" {
		t.Fatalf("bucket: got %q, want %q", gotBucket, "my-bucket")
	}
	if string(gotDoc) != string(raw) {
		t.Fatalf("doc: got %q, want %q", gotDoc, raw)
	}
}

func TestBucketPolicyCodec_PutDocIsIndependent(t *testing.T) {
	raw := []byte(`{"x":1}`)
	data, _ := EncodeBucketPolicyPutPayload("b", raw)
	_, gotDoc, _ := DecodeBucketPolicyPutPayload(data)
	gotDoc[0] = 'X'
	_, gotDoc2, _ := DecodeBucketPolicyPutPayload(data)
	if gotDoc2[0] != '{' {
		t.Fatal("Decoded doc must be independent of input buffer")
	}
}

func TestBucketPolicyCodec_DeleteRoundTrip(t *testing.T) {
	data, err := EncodeBucketPolicyDeletePayload("del-bucket")
	if err != nil {
		t.Fatalf("EncodeBucketPolicyDeletePayload: %v", err)
	}
	gotBucket, err := DecodeBucketPolicyDeletePayload(data)
	if err != nil {
		t.Fatalf("DecodeBucketPolicyDeletePayload: %v", err)
	}
	if gotBucket != "del-bucket" {
		t.Fatalf("bucket: got %q, want %q", gotBucket, "del-bucket")
	}
}
