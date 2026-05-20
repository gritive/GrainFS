package cluster

import (
	"context"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/iampb"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/iam/policystore"
)

// buildCreateBucketWithPolicyAttachCmd encodes a CreateBucketWithPolicyAttach
// MetaCmd ready for MetaFSM.applyCmd.
func buildCreateBucketWithPolicyAttachCmd(t *testing.T, bucket, sa, policy string) []byte {
	t.Helper()
	payload, err := EncodeMetaCreateBucketWithPolicyAttachCmd(bucket, sa, policy)
	if err != nil {
		t.Fatalf("EncodeMetaCreateBucketWithPolicyAttachCmd: %v", err)
	}
	cmd, err := encodeMetaCmd(MetaCmdTypeCreateBucketWithPolicyAttach, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(CreateBucketWithPolicyAttach): %v", err)
	}
	return cmd
}

// buildSACreateCmdForAtomic encodes an IAMSACreate MetaCmd for test fixture setup.
func buildSACreateCmdForAtomic(t *testing.T, saID string) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(64)
	idOff := b.CreateString(saID)
	nameOff := b.CreateString("test-sa")
	descOff := b.CreateString("")
	cbOff := b.CreateString("")
	iampb.SACreatePayloadStart(b)
	iampb.SACreatePayloadAddSaId(b, idOff)
	iampb.SACreatePayloadAddName(b, nameOff)
	iampb.SACreatePayloadAddDescription(b, descOff)
	iampb.SACreatePayloadAddCreatedAtUnixNs(b, time.Now().UnixNano())
	iampb.SACreatePayloadAddCreatedBy(b, cbOff)
	b.Finish(iampb.SACreatePayloadEnd(b))
	payload := b.FinishedBytes()
	cmd, err := encodeMetaCmd(MetaCmdTypeIAMSACreate, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(IAMSACreate): %v", err)
	}
	return cmd
}

// newFSMWithIAMAndAttach builds a MetaFSM with a pre-seeded SA + policy,
// returning the FSM together with the attach store for assertions.
func newFSMWithIAMAndAttach(t *testing.T, saID, policyName string, policyDoc []byte) (*MetaFSM, *policyattach.InMemoryStore) {
	t.Helper()
	enc := newIAMTestEncryptor(t)
	iamStore := iam.NewStore()
	iamApplier := iam.NewApplier(iamStore, enc)

	polStore := policystore.NewInMemoryStore()
	attachStore := policyattach.NewInMemoryStore()

	f := NewMetaFSM()
	f.SetIAM(iamStore, iamApplier)
	f.SetPolicyStore(polStore)
	f.SetPolicyAttachStore(attachStore)

	// Seed SA.
	if err := f.applyCmd(buildSACreateCmdForAtomic(t, saID)); err != nil {
		t.Fatalf("seed SA: %v", err)
	}
	// Seed policy. Use isBuiltin=true so the FSM guard allows seeding a built-in name.
	if err := f.applyCmd(buildPolicyPutCmd(t, policyName, policyDoc, true)); err != nil {
		t.Fatalf("seed policy: %v", err)
	}

	return f, attachStore
}

// TestCreateBucketWithPolicyAttach_AttachPath verifies that applying the cmd
// with a valid SA + policy attaches the policy to the SA.
func TestCreateBucketWithPolicyAttach_AttachPath(t *testing.T) {
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	f, attachStore := newFSMWithIAMAndAttach(t, "sa-1", "readonly", doc)

	cmd := buildCreateBucketWithPolicyAttachCmd(t, "my-bucket", "sa-1", "readonly")
	if err := f.applyCmd(cmd); err != nil {
		t.Fatalf("applyCmd: %v", err)
	}

	pols, err := attachStore.SAPolicies(context.Background(), "sa-1")
	if err != nil {
		t.Fatalf("SAPolicies: %v", err)
	}
	if len(pols) != 1 || pols[0] != "readonly" {
		t.Fatalf("policies = %v, want [readonly]", pols)
	}
}

// TestCreateBucketWithPolicyAttach_RejectsMissingSA verifies that a missing SA
// causes apply to return an error before any state mutation (F#2).
func TestCreateBucketWithPolicyAttach_RejectsMissingSA(t *testing.T) {
	doc := []byte(`{"Statement":[]}`)
	f, attachStore := newFSMWithIAMAndAttach(t, "sa-real", "p", doc)

	cmd := buildCreateBucketWithPolicyAttachCmd(t, "my-bucket", "sa-does-not-exist", "p")
	err := f.applyCmd(cmd)
	if err == nil {
		t.Fatal("expected error for missing SA, got nil")
	}

	// Nothing should have been attached.
	pols, listErr := attachStore.SAPolicies(context.Background(), "sa-does-not-exist")
	if listErr != nil {
		t.Fatalf("SAPolicies: %v", listErr)
	}
	if len(pols) != 0 {
		t.Fatalf("no attachment expected after SA-missing rejection, got %v", pols)
	}
}

// TestCreateBucketWithPolicyAttach_RejectsMissingPolicy verifies that an
// existing SA but unknown policy causes apply to return an error before
// any state mutation (F#2).
func TestCreateBucketWithPolicyAttach_RejectsMissingPolicy(t *testing.T) {
	doc := []byte(`{"Statement":[]}`)
	f, attachStore := newFSMWithIAMAndAttach(t, "sa-1", "known-policy", doc)

	cmd := buildCreateBucketWithPolicyAttachCmd(t, "my-bucket", "sa-1", "unknown-policy")
	err := f.applyCmd(cmd)
	if err == nil {
		t.Fatal("expected error for missing policy, got nil")
	}

	// Nothing should have been attached.
	pols, listErr := attachStore.SAPolicies(context.Background(), "sa-1")
	if listErr != nil {
		t.Fatalf("SAPolicies: %v", listErr)
	}
	if len(pols) != 0 {
		t.Fatalf("no attachment expected after policy-missing rejection, got %v", pols)
	}
}

// TestCreateBucketWithPolicyAttach_CreateOnlyEmptySA verifies that an empty
// sa field is a no-op on the IAM stores.
func TestCreateBucketWithPolicyAttach_CreateOnlyEmptySA(t *testing.T) {
	doc := []byte(`{"Statement":[]}`)
	f, attachStore := newFSMWithIAMAndAttach(t, "sa-1", "p", doc)

	cmd := buildCreateBucketWithPolicyAttachCmd(t, "my-bucket", "", "")
	if err := f.applyCmd(cmd); err != nil {
		t.Fatalf("create-only apply: %v", err)
	}

	// No policies should be attached.
	pols, err := attachStore.SAPolicies(context.Background(), "sa-1")
	if err != nil {
		t.Fatalf("SAPolicies: %v", err)
	}
	if len(pols) != 0 {
		t.Fatalf("create-only: unexpected attachment %v", pols)
	}
}
