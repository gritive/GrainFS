package iam

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// TestMetaProposer_DispatchesCorrectCmdTypes verifies that each method
// calls the underlying ProposeFunc with the matching MetaCmdType. Payload
// bytes are validated by Applier round-trip in fsm_test.go; here we only
// check the type dispatch is right.
func TestMetaProposer_DispatchesCorrectCmdTypes(t *testing.T) {
	captured := make([]clusterpb.MetaCmdType, 0)
	p := &MetaProposer{
		Propose: func(ctx context.Context, t clusterpb.MetaCmdType, payload []byte) error {
			captured = append(captured, t)
			return nil
		},
	}
	ctx := context.Background()
	_ = p.ProposeSACreate(ctx, ServiceAccount{ID: "sa-1"})
	_ = p.ProposeSADelete(ctx, "sa-1")
	_ = p.ProposeKeyCreate(ctx, AccessKey{AccessKey: "AK", SAID: "sa-1"})
	_ = p.ProposeKeyRevoke(ctx, "AK")
	_ = p.ProposeGrantPut(ctx, Grant{SAID: "sa-1", Bucket: "b", Role: RoleRead})
	_ = p.ProposeGrantDelete(ctx, "sa-1", "b")
	_ = p.ProposeGrantWildcardPut(ctx, Grant{SAID: "sa-1", Role: RoleAdmin})
	_ = p.ProposeGrantWildcardDelete(ctx, "sa-1")
	_ = p.ProposeInitFirstSA(ctx, ServiceAccount{ID: "sa-1"}, AccessKey{AccessKey: "AK", SAID: "sa-1"}, Grant{SAID: "sa-1", Role: RoleAdmin})

	want := []clusterpb.MetaCmdType{
		clusterpb.MetaCmdTypeIAMSACreate,
		clusterpb.MetaCmdTypeIAMSADelete,
		clusterpb.MetaCmdTypeIAMKeyCreate,
		clusterpb.MetaCmdTypeIAMKeyRevoke,
		clusterpb.MetaCmdTypeIAMGrantPut,
		clusterpb.MetaCmdTypeIAMGrantDelete,
		clusterpb.MetaCmdTypeIAMGrantWildcardPut,
		clusterpb.MetaCmdTypeIAMGrantWildcardDelete,
		clusterpb.MetaCmdTypeIAMInitFirstSA,
	}
	if len(captured) < len(want) {
		t.Fatalf("captured %d, want %d (got %v)", len(captured), len(want), captured)
	}
	for i, w := range want {
		if captured[i] != w {
			t.Errorf("idx %d: got %v, want %v", i, captured[i], w)
		}
	}
}
