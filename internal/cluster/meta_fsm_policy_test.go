package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/iam/policystore"
)

// buildPolicyPutCmd encodes a PolicyPut MetaCmd ready for MetaFSM.applyCmd.
func buildPolicyPutCmd(t *testing.T, name string, doc []byte, builtin bool) []byte {
	t.Helper()
	payload, err := EncodePolicyPutPayload(name, doc, builtin)
	if err != nil {
		t.Fatalf("EncodePolicyPutPayload: %v", err)
	}
	cmd, err := encodeMetaCmd(MetaCmdTypePolicyPut, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(PolicyPut): %v", err)
	}
	return cmd
}

// buildPolicyDeleteCmd encodes a PolicyDelete MetaCmd ready for MetaFSM.applyCmd.
func buildPolicyDeleteCmd(t *testing.T, name string) []byte {
	t.Helper()
	payload, err := EncodePolicyDeletePayload(name)
	if err != nil {
		t.Fatalf("EncodePolicyDeletePayload: %v", err)
	}
	cmd, err := encodeMetaCmd(MetaCmdTypePolicyDelete, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(PolicyDelete): %v", err)
	}
	return cmd
}

// TestMetaFSM_Apply_PolicyPut_StoresDoc verifies that applying a PolicyPut cmd
// with a wired policyStore causes the doc to appear in the store.
func TestMetaFSM_Apply_PolicyPut_StoresDoc(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	store := policystore.NewInMemoryStore()

	f := NewMetaFSM()
	f.SetPolicyStore(store)

	if err := f.applyCmd(buildPolicyPutCmd(t, "my-policy", doc, false)); err != nil {
		t.Fatalf("applyCmd PolicyPut: %v", err)
	}

	got, err := store.GetRaw(context.Background(), "my-policy")
	if err != nil {
		t.Fatalf("GetRaw after PolicyPut: %v", err)
	}
	if string(got) != string(doc) {
		t.Fatalf("stored doc mismatch: got %q, want %q", got, doc)
	}
}

// TestMetaFSM_Apply_PolicyDelete_RemovesDoc verifies that applying a PolicyDelete
// cmd removes the previously stored policy.
func TestMetaFSM_Apply_PolicyDelete_RemovesDoc(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	store := policystore.NewInMemoryStore()

	f := NewMetaFSM()
	f.SetPolicyStore(store)

	if err := f.applyCmd(buildPolicyPutCmd(t, "del-policy", doc, false)); err != nil {
		t.Fatalf("applyCmd PolicyPut: %v", err)
	}
	if err := f.applyCmd(buildPolicyDeleteCmd(t, "del-policy")); err != nil {
		t.Fatalf("applyCmd PolicyDelete: %v", err)
	}

	_, err := store.GetRaw(context.Background(), "del-policy")
	if !errors.Is(err, policystore.ErrPolicyNotFound) {
		t.Fatalf("GetRaw after PolicyDelete: got %v, want ErrPolicyNotFound", err)
	}
}

// TestMetaFSM_Apply_PolicyPut_NilStore_SafeNoop verifies that a PolicyPut cmd
// is a safe no-op when policyStore has not been wired (nil).
func TestMetaFSM_Apply_PolicyPut_NilStore_SafeNoop(t *testing.T) {
	doc := []byte(`{"Statement":[]}`)
	f := NewMetaFSM()
	// policyStore intentionally not wired — must not panic, must not error.
	if err := f.applyCmd(buildPolicyPutCmd(t, "noop", doc, false)); err != nil {
		t.Fatalf("PolicyPut with nil store: got error %v, want nil", err)
	}
}

// TestMetaFSM_Apply_PolicyDelete_NilStore_SafeNoop verifies that a PolicyDelete
// cmd is a safe no-op when policyStore has not been wired (nil).
func TestMetaFSM_Apply_PolicyDelete_NilStore_SafeNoop(t *testing.T) {
	f := NewMetaFSM()
	if err := f.applyCmd(buildPolicyDeleteCmd(t, "noop")); err != nil {
		t.Fatalf("PolicyDelete with nil store: got error %v, want nil", err)
	}
}
