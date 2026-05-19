package cluster

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/gritive/GrainFS/internal/iam/bucketpolicy"
	"github.com/gritive/GrainFS/internal/iam/group"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/iam/policystore"
	"github.com/gritive/GrainFS/internal/raft"
)

// wireIPSTStores wires all 4 policy stores into the MetaFSM.
func wireIPSTStores(f *MetaFSM) (*policystore.InMemoryStore, *group.InMemoryStore, *policyattach.InMemoryStore, *bucketpolicy.InMemoryStore) {
	ps := policystore.NewInMemoryStore()
	gs := group.NewInMemoryStore()
	as := policyattach.NewInMemoryStore()
	bp := bucketpolicy.NewInMemoryStore()
	f.SetPolicyStore(ps)
	f.SetGroupStore(gs)
	f.SetPolicyAttachStore(as)
	f.SetBucketPolicyStore(bp)
	return ps, gs, as, bp
}

// seedIPSTState applies a representative set of PolicyPut, GroupPut, GroupMemberPut,
// PolicyAttachToSAPut, PolicyAttachToGroupPut, and BucketPolicyPut commands into f.
func seedIPSTState(t *testing.T, f *MetaFSM) {
	t.Helper()
	ctx := context.Background()
	_ = ctx

	// Two policies
	if err := f.applyCmd(buildPolicyPutCmd(t, "readonly", []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":"*"}]}`), true)); err != nil {
		t.Fatalf("applyCmd PolicyPut readonly: %v", err)
	}
	if err := f.applyCmd(buildPolicyPutCmd(t, "custom-pol", []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::my-bucket/*"}]}`), false)); err != nil {
		t.Fatalf("applyCmd PolicyPut custom-pol: %v", err)
	}

	// One group with two members and a policy
	if err := f.applyCmd(buildGroupPutCmd(t, "admins", []string{"readonly"})); err != nil {
		t.Fatalf("applyCmd GroupPut admins: %v", err)
	}
	if err := f.applyCmd(buildGroupMemberPutCmd(t, "admins", "sa-alice")); err != nil {
		t.Fatalf("applyCmd GroupMemberPut sa-alice: %v", err)
	}
	if err := f.applyCmd(buildGroupMemberPutCmd(t, "admins", "sa-bob")); err != nil {
		t.Fatalf("applyCmd GroupMemberPut sa-bob: %v", err)
	}

	// SA attachments
	if err := f.applyCmd(buildPolicyAttachToSAPutCmd(t, "sa-alice", "readonly")); err != nil {
		t.Fatalf("applyCmd PolicyAttachToSAPut sa-alice: %v", err)
	}
	if err := f.applyCmd(buildPolicyAttachToSAPutCmd(t, "sa-alice", "custom-pol")); err != nil {
		t.Fatalf("applyCmd PolicyAttachToSAPut sa-alice custom-pol: %v", err)
	}

	// Group attachment
	if err := f.applyCmd(buildPolicyAttachToGroupPutCmd(t, "admins", "readonly")); err != nil {
		t.Fatalf("applyCmd PolicyAttachToGroupPut admins: %v", err)
	}

	// Bucket policy
	if err := f.applyCmd(buildBucketPolicyPutCmd(t, "my-bucket", []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::my-bucket/*"}]}`))); err != nil {
		t.Fatalf("applyCmd BucketPolicyPut my-bucket: %v", err)
	}
}

// TestMetaFSM_IPSTSnapshot_RoundTrip verifies that all 4 §2 IAM policy stores
// survive a Snapshot → Restore round-trip on a fresh MetaFSM.
func TestMetaFSM_IPSTSnapshot_RoundTrip(t *testing.T) {
	src := NewMetaFSM()
	wireIPSTStores(src)
	seedIPSTState(t, src)

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Verify the IPST trailer is present at the end.
	if len(snap) < ipstSnapshotTrailerLen {
		t.Fatalf("snapshot too small to contain IPST trailer: %d bytes", len(snap))
	}
	gotMagic := binary.LittleEndian.Uint32(snap[len(snap)-4:])
	if gotMagic != ipstSnapshotTrailerMagic {
		t.Fatalf("last 4 bytes magic = 0x%08X, want 0x%08X (IPST)", gotMagic, ipstSnapshotTrailerMagic)
	}

	// Restore into a fresh FSM with fresh stores.
	dst := NewMetaFSM()
	ps2, gs2, as2, bp2 := wireIPSTStores(dst)

	if err := dst.Restore(raft.SnapshotMeta{}, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	ctx := context.Background()

	// PolicyStore assertions
	gotDoc, err := ps2.GetRaw(ctx, "readonly")
	if err != nil {
		t.Fatalf("GetRaw readonly: %v", err)
	}
	if len(gotDoc) == 0 {
		t.Fatal("readonly policy doc is empty after restore")
	}
	if !ps2.IsBuiltin("readonly") {
		t.Fatal("readonly policy should be builtin after restore")
	}
	gotCustom, err := ps2.GetRaw(ctx, "custom-pol")
	if err != nil {
		t.Fatalf("GetRaw custom-pol: %v", err)
	}
	if len(gotCustom) == 0 {
		t.Fatal("custom-pol doc is empty after restore")
	}
	if ps2.IsBuiltin("custom-pol") {
		t.Fatal("custom-pol should NOT be builtin after restore")
	}

	// GroupStore assertions
	apols, err := gs2.AttachedPolicies(ctx, "admins")
	if err != nil {
		t.Fatalf("AttachedPolicies admins: %v", err)
	}
	if len(apols) != 1 || apols[0] != "readonly" {
		t.Fatalf("admins attached_policies = %v, want [readonly]", apols)
	}
	members, err := gs2.MembersOf(ctx, "admins")
	if err != nil {
		t.Fatalf("MembersOf admins: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("admins members len = %d, want 2", len(members))
	}
	memberSet := map[string]bool{}
	for _, m := range members {
		memberSet[m] = true
	}
	if !memberSet["sa-alice"] || !memberSet["sa-bob"] {
		t.Fatalf("admins members = %v, want {sa-alice, sa-bob}", members)
	}

	// PolicyAttachStore assertions
	saPols, err := as2.SAPolicies(ctx, "sa-alice")
	if err != nil {
		t.Fatalf("SAPolicies sa-alice: %v", err)
	}
	if len(saPols) != 2 {
		t.Fatalf("sa-alice policies len = %d, want 2", len(saPols))
	}
	saPolSet := map[string]bool{}
	for _, p := range saPols {
		saPolSet[p] = true
	}
	if !saPolSet["readonly"] || !saPolSet["custom-pol"] {
		t.Fatalf("sa-alice policies = %v, want {readonly, custom-pol}", saPols)
	}
	grpPols, err := as2.GroupPolicies(ctx, "admins")
	if err != nil {
		t.Fatalf("GroupPolicies admins: %v", err)
	}
	if len(grpPols) != 1 || grpPols[0] != "readonly" {
		t.Fatalf("admins group policies = %v, want [readonly]", grpPols)
	}

	// BucketPolicyStore assertions
	bpDoc, err := bp2.Get(ctx, "my-bucket")
	if err != nil {
		t.Fatalf("Get my-bucket: %v", err)
	}
	if len(bpDoc) == 0 {
		t.Fatal("my-bucket policy doc is empty after restore")
	}
}

// TestMetaFSM_IPSTSnapshot_LegacySnapshot_NoIPST verifies that a snapshot without
// an IPST trailer (legacy pre-C1 format) restores cleanly without errors, and the
// wired stores remain empty.
func TestMetaFSM_IPSTSnapshot_LegacySnapshot_NoIPST(t *testing.T) {
	// Build a snapshot from an FSM that has NO policy stores wired.
	src := NewMetaFSM()
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot (no stores): %v", err)
	}

	// Sanity: no IPST trailer (stores were nil).
	if len(snap) >= ipstSnapshotTrailerLen {
		magic := binary.LittleEndian.Uint32(snap[len(snap)-4:])
		if magic == ipstSnapshotTrailerMagic {
			t.Fatal("expected no IPST trailer on FSM with nil stores, but found one")
		}
	}

	// Restore into a fresh FSM that HAS stores wired.
	dst := NewMetaFSM()
	ps2, gs2, as2, bp2 := wireIPSTStores(dst)

	if err := dst.Restore(raft.SnapshotMeta{}, snap); err != nil {
		t.Fatalf("Restore legacy snapshot: %v", err)
	}

	// Stores should be empty (no IPST data to restore).
	if names := ps2.List(); len(names) != 0 {
		t.Fatalf("policyStore has %d entries after legacy restore, want 0", len(names))
	}
	_ = gs2
	_ = as2
	_ = bp2
}

// TestMetaFSM_IPSTSnapshot_NilStores_WarnOnly verifies that restoring a snapshot
// with an IPST trailer into an FSM that has no stores wired returns no error
// (only a warn log) — so nodes that receive a snapshot before wiring stores don't crash.
func TestMetaFSM_IPSTSnapshot_NilStores_WarnOnly(t *testing.T) {
	// Source FSM with stores wired and populated.
	src := NewMetaFSM()
	wireIPSTStores(src)
	seedIPSTState(t, src)

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restore into FSM with NO stores wired — must not error.
	dst := NewMetaFSM()
	if err := dst.Restore(raft.SnapshotMeta{}, snap); err != nil {
		t.Fatalf("Restore to nil-store FSM: %v", err)
	}
}

// TestMetaFSM_IPSTSnapshot_EmptyStores verifies that an FSM with all stores wired
// but empty produces a snapshot that restores to empty stores (no ghost entries).
func TestMetaFSM_IPSTSnapshot_EmptyStores(t *testing.T) {
	src := NewMetaFSM()
	wireIPSTStores(src)

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// IPST trailer must be present (stores are wired, even if empty).
	if len(snap) < ipstSnapshotTrailerLen {
		t.Fatalf("snapshot too small: %d bytes", len(snap))
	}
	magic := binary.LittleEndian.Uint32(snap[len(snap)-4:])
	if magic != ipstSnapshotTrailerMagic {
		t.Fatalf("expected IPST trailer magic, got 0x%08X", magic)
	}

	dst := NewMetaFSM()
	ps2, _, _, _ := wireIPSTStores(dst)
	if err := dst.Restore(raft.SnapshotMeta{}, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if names := ps2.List(); len(names) != 0 {
		t.Fatalf("policyStore has %d entries after empty restore, want 0", len(names))
	}
}
