package cluster

import (
	"bytes"
	"testing"
)

func TestPolicyCodec_PutRoundTrip(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	for _, builtin := range []bool{false, true} {
		data, err := EncodePolicyPutPayload("my-policy", doc, builtin)
		if err != nil {
			t.Fatalf("EncodePolicyPutPayload(builtin=%v): %v", builtin, err)
		}
		gotName, gotDoc, gotBuiltin, err := DecodePolicyPutPayload(data)
		if err != nil {
			t.Fatalf("DecodePolicyPutPayload(builtin=%v): %v", builtin, err)
		}
		if gotName != "my-policy" {
			t.Fatalf("name mismatch: got %q, want %q", gotName, "my-policy")
		}
		if !bytes.Equal(gotDoc, doc) {
			t.Fatalf("doc mismatch: got %q, want %q", gotDoc, doc)
		}
		if gotBuiltin != builtin {
			t.Fatalf("builtin mismatch: got %v, want %v", gotBuiltin, builtin)
		}
	}
}

// TestPolicyCodec_PutDocIsIndependent ensures that the returned doc bytes are a
// copy independent of the encoded buffer. Simulates raft log buffer reuse by
// XOR-mutating the source buffer after decode and asserting the decoded doc
// is unchanged (mirrors TestDecodeClusterConfigPatchCmd_SecretBytesIndependent).
func TestPolicyCodec_PutDocIsIndependent(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	data, err := EncodePolicyPutPayload("iso-policy", doc, false)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	_, gotDoc, _, err := DecodePolicyPutPayload(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	original := append([]byte(nil), gotDoc...)
	// Mutate the source buffer to simulate a pooled/reused Raft log entry.
	for i := range data {
		data[i] ^= 0xFF
	}
	if !bytes.Equal(gotDoc, original) {
		t.Fatal("decoded doc must be independent of the source buffer")
	}
}

func TestPolicyCodec_DeleteRoundTrip(t *testing.T) {
	data, err := EncodePolicyDeletePayload("my-policy")
	if err != nil {
		t.Fatalf("EncodePolicyDeletePayload: %v", err)
	}
	gotName, err := DecodePolicyDeletePayload(data)
	if err != nil {
		t.Fatalf("DecodePolicyDeletePayload: %v", err)
	}
	if gotName != "my-policy" {
		t.Fatalf("name mismatch: got %q, want %q", gotName, "my-policy")
	}
}
