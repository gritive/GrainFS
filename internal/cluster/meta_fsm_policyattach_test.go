package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/iam/policyattach"
)

// buildPolicyAttachToSAPutCmd encodes a PolicyAttachToSAPut MetaCmd.
func buildPolicyAttachToSAPutCmd(t *testing.T, saID, policy string) []byte {
	t.Helper()
	payload, err := EncodePolicyAttachToSAPutPayload(saID, policy)
	if err != nil {
		t.Fatalf("EncodePolicyAttachToSAPutPayload: %v", err)
	}
	cmd, err := encodeMetaCmd(MetaCmdTypePolicyAttachToSAPut, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(PolicyAttachToSAPut): %v", err)
	}
	return cmd
}

// buildPolicyAttachToSADeleteCmd encodes a PolicyAttachToSADelete MetaCmd.
func buildPolicyAttachToSADeleteCmd(t *testing.T, saID, policy string) []byte {
	t.Helper()
	payload, err := EncodePolicyAttachToSADeletePayload(saID, policy)
	if err != nil {
		t.Fatalf("EncodePolicyAttachToSADeletePayload: %v", err)
	}
	cmd, err := encodeMetaCmd(MetaCmdTypePolicyAttachToSADelete, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(PolicyAttachToSADelete): %v", err)
	}
	return cmd
}

// buildPolicyAttachToGroupPutCmd encodes a PolicyAttachToGroupPut MetaCmd.
func buildPolicyAttachToGroupPutCmd(t *testing.T, group, policy string) []byte {
	t.Helper()
	payload, err := EncodePolicyAttachToGroupPutPayload(group, policy)
	if err != nil {
		t.Fatalf("EncodePolicyAttachToGroupPutPayload: %v", err)
	}
	cmd, err := encodeMetaCmd(MetaCmdTypePolicyAttachToGroupPut, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(PolicyAttachToGroupPut): %v", err)
	}
	return cmd
}

// buildPolicyAttachToGroupDeleteCmd encodes a PolicyAttachToGroupDelete MetaCmd.
func buildPolicyAttachToGroupDeleteCmd(t *testing.T, group, policy string) []byte {
	t.Helper()
	payload, err := EncodePolicyAttachToGroupDeletePayload(group, policy)
	if err != nil {
		t.Fatalf("EncodePolicyAttachToGroupDeletePayload: %v", err)
	}
	cmd, err := encodeMetaCmd(MetaCmdTypePolicyAttachToGroupDelete, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(PolicyAttachToGroupDelete): %v", err)
	}
	return cmd
}

// TestMetaFSM_Apply_PolicyAttachToSAPut_StoresAttachment verifies that
// applying a PolicyAttachToSAPut cmd wires the policy into the attach store.
func TestMetaFSM_Apply_PolicyAttachToSAPut_StoresAttachment(t *testing.T) {
	store := policyattach.NewInMemoryStore()

	f := NewMetaFSM()
	f.SetPolicyAttachStore(store)

	if err := f.applyCmd(buildPolicyAttachToSAPutCmd(t, "sa-1", "readonly")); err != nil {
		t.Fatalf("applyCmd PolicyAttachToSAPut: %v", err)
	}

	pols, err := store.SAPolicies(context.Background(), "sa-1")
	if err != nil {
		t.Fatalf("SAPolicies: %v", err)
	}
	if len(pols) != 1 || pols[0] != "readonly" {
		t.Fatalf("policies = %v, want [readonly]", pols)
	}
}

// TestMetaFSM_Apply_PolicyAttachToSADelete_RemovesAttachment verifies detach.
func TestMetaFSM_Apply_PolicyAttachToSADelete_RemovesAttachment(t *testing.T) {
	store := policyattach.NewInMemoryStore()

	f := NewMetaFSM()
	f.SetPolicyAttachStore(store)

	if err := f.applyCmd(buildPolicyAttachToSAPutCmd(t, "sa-2", "admin")); err != nil {
		t.Fatalf("applyCmd PolicyAttachToSAPut: %v", err)
	}
	if err := f.applyCmd(buildPolicyAttachToSADeleteCmd(t, "sa-2", "admin")); err != nil {
		t.Fatalf("applyCmd PolicyAttachToSADelete: %v", err)
	}

	pols, err := store.SAPolicies(context.Background(), "sa-2")
	if err != nil {
		t.Fatalf("SAPolicies: %v", err)
	}
	if len(pols) != 0 {
		t.Fatalf("after detach, policies = %v, want []", pols)
	}
}

// TestMetaFSM_Apply_PolicyAttachToGroupPut_StoresAttachment verifies group attach.
func TestMetaFSM_Apply_PolicyAttachToGroupPut_StoresAttachment(t *testing.T) {
	store := policyattach.NewInMemoryStore()

	f := NewMetaFSM()
	f.SetPolicyAttachStore(store)

	if err := f.applyCmd(buildPolicyAttachToGroupPutCmd(t, "engineers", "bucket-rw")); err != nil {
		t.Fatalf("applyCmd PolicyAttachToGroupPut: %v", err)
	}

	pols, err := store.GroupPolicies(context.Background(), "engineers")
	if err != nil {
		t.Fatalf("GroupPolicies: %v", err)
	}
	if len(pols) != 1 || pols[0] != "bucket-rw" {
		t.Fatalf("policies = %v, want [bucket-rw]", pols)
	}
}

// TestMetaFSM_Apply_PolicyAttachToGroupDelete_RemovesAttachment verifies group detach.
func TestMetaFSM_Apply_PolicyAttachToGroupDelete_RemovesAttachment(t *testing.T) {
	store := policyattach.NewInMemoryStore()

	f := NewMetaFSM()
	f.SetPolicyAttachStore(store)

	if err := f.applyCmd(buildPolicyAttachToGroupPutCmd(t, "ops", "bucket-ro")); err != nil {
		t.Fatalf("applyCmd PolicyAttachToGroupPut: %v", err)
	}
	if err := f.applyCmd(buildPolicyAttachToGroupDeleteCmd(t, "ops", "bucket-ro")); err != nil {
		t.Fatalf("applyCmd PolicyAttachToGroupDelete: %v", err)
	}

	pols, err := store.GroupPolicies(context.Background(), "ops")
	if err != nil {
		t.Fatalf("GroupPolicies: %v", err)
	}
	if len(pols) != 0 {
		t.Fatalf("after detach, policies = %v, want []", pols)
	}
}

// TestMetaFSM_Apply_PolicyAttachToSAPut_NilStore_SafeNoop verifies nil-safe no-op.
func TestMetaFSM_Apply_PolicyAttachToSAPut_NilStore_SafeNoop(t *testing.T) {
	f := NewMetaFSM()
	if err := f.applyCmd(buildPolicyAttachToSAPutCmd(t, "sa-x", "p")); err != nil {
		t.Fatalf("PolicyAttachToSAPut with nil store: got error %v, want nil", err)
	}
}

// TestMetaFSM_Apply_PolicyAttachToGroupPut_NilStore_SafeNoop verifies nil-safe no-op.
func TestMetaFSM_Apply_PolicyAttachToGroupPut_NilStore_SafeNoop(t *testing.T) {
	f := NewMetaFSM()
	if err := f.applyCmd(buildPolicyAttachToGroupPutCmd(t, "g-x", "p")); err != nil {
		t.Fatalf("PolicyAttachToGroupPut with nil store: got error %v, want nil", err)
	}
}
