package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/iam/group"
)

// buildGroupPutCmd encodes a GroupPut MetaCmd ready for MetaFSM.applyCmd.
func buildGroupPutCmd(t *testing.T, name string, policies []string) []byte {
	t.Helper()
	payload, err := EncodeGroupPutPayload(name, policies)
	if err != nil {
		t.Fatalf("EncodeGroupPutPayload: %v", err)
	}
	cmd, err := encodeMetaCmd(MetaCmdTypeGroupPut, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(GroupPut): %v", err)
	}
	return cmd
}

// buildGroupDeleteCmd encodes a GroupDelete MetaCmd ready for MetaFSM.applyCmd.
func buildGroupDeleteCmd(t *testing.T, name string) []byte {
	t.Helper()
	payload, err := EncodeGroupDeletePayload(name)
	if err != nil {
		t.Fatalf("EncodeGroupDeletePayload: %v", err)
	}
	cmd, err := encodeMetaCmd(MetaCmdTypeGroupDelete, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(GroupDelete): %v", err)
	}
	return cmd
}

// buildGroupMemberPutCmd encodes a GroupMemberPut MetaCmd ready for MetaFSM.applyCmd.
func buildGroupMemberPutCmd(t *testing.T, grp, saID string) []byte {
	t.Helper()
	payload, err := EncodeGroupMemberPutPayload(grp, saID)
	if err != nil {
		t.Fatalf("EncodeGroupMemberPutPayload: %v", err)
	}
	cmd, err := encodeMetaCmd(MetaCmdTypeGroupMemberPut, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(GroupMemberPut): %v", err)
	}
	return cmd
}

// buildGroupMemberDeleteCmd encodes a GroupMemberDelete MetaCmd ready for MetaFSM.applyCmd.
func buildGroupMemberDeleteCmd(t *testing.T, grp, saID string) []byte {
	t.Helper()
	payload, err := EncodeGroupMemberDeletePayload(grp, saID)
	if err != nil {
		t.Fatalf("EncodeGroupMemberDeletePayload: %v", err)
	}
	cmd, err := encodeMetaCmd(MetaCmdTypeGroupMemberDelete, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd(GroupMemberDelete): %v", err)
	}
	return cmd
}

// TestMetaFSM_Apply_GroupPut_StoresGroup verifies that applying a GroupPut cmd
// with a wired groupStore causes the group to appear in the store.
func TestMetaFSM_Apply_GroupPut_StoresGroup(t *testing.T) {
	store := group.NewInMemoryStore()

	f := NewMetaFSM()
	f.SetGroupStore(store)

	if err := f.applyCmd(buildGroupPutCmd(t, "admins", []string{"readwrite"})); err != nil {
		t.Fatalf("applyCmd GroupPut: %v", err)
	}

	pol, err := store.AttachedPolicies(context.Background(), "admins")
	if err != nil {
		t.Fatalf("AttachedPolicies after GroupPut: %v", err)
	}
	if len(pol) != 1 || pol[0] != "readwrite" {
		t.Fatalf("policies = %v, want [readwrite]", pol)
	}
}

// TestMetaFSM_Apply_GroupDelete_RemovesGroup verifies that applying a GroupDelete
// cmd removes the previously stored group.
func TestMetaFSM_Apply_GroupDelete_RemovesGroup(t *testing.T) {
	store := group.NewInMemoryStore()

	f := NewMetaFSM()
	f.SetGroupStore(store)

	if err := f.applyCmd(buildGroupPutCmd(t, "del-group", nil)); err != nil {
		t.Fatalf("applyCmd GroupPut: %v", err)
	}
	if err := f.applyCmd(buildGroupDeleteCmd(t, "del-group")); err != nil {
		t.Fatalf("applyCmd GroupDelete: %v", err)
	}

	_, err := store.AttachedPolicies(context.Background(), "del-group")
	if !errors.Is(err, group.ErrGroupNotFound) {
		t.Fatalf("AttachedPolicies after GroupDelete: got %v, want ErrGroupNotFound", err)
	}
}

// TestMetaFSM_Apply_GroupMemberPut_AddsMember verifies that GroupMemberPut adds
// the sa_id to the group's member set.
func TestMetaFSM_Apply_GroupMemberPut_AddsMember(t *testing.T) {
	store := group.NewInMemoryStore()

	f := NewMetaFSM()
	f.SetGroupStore(store)

	if err := f.applyCmd(buildGroupPutCmd(t, "g", nil)); err != nil {
		t.Fatalf("applyCmd GroupPut: %v", err)
	}
	if err := f.applyCmd(buildGroupMemberPutCmd(t, "g", "sa-1")); err != nil {
		t.Fatalf("applyCmd GroupMemberPut: %v", err)
	}

	groups, err := store.MembershipOf(context.Background(), "sa-1")
	if err != nil {
		t.Fatalf("MembershipOf: %v", err)
	}
	if len(groups) != 1 || groups[0] != "g" {
		t.Fatalf("groups = %v, want [g]", groups)
	}
}

// TestMetaFSM_Apply_GroupMemberDelete_RemovesMember verifies that GroupMemberDelete
// removes the sa_id from the group.
func TestMetaFSM_Apply_GroupMemberDelete_RemovesMember(t *testing.T) {
	store := group.NewInMemoryStore()

	f := NewMetaFSM()
	f.SetGroupStore(store)

	if err := f.applyCmd(buildGroupPutCmd(t, "g", nil)); err != nil {
		t.Fatalf("applyCmd GroupPut: %v", err)
	}
	if err := f.applyCmd(buildGroupMemberPutCmd(t, "g", "sa-1")); err != nil {
		t.Fatalf("applyCmd GroupMemberPut: %v", err)
	}
	if err := f.applyCmd(buildGroupMemberDeleteCmd(t, "g", "sa-1")); err != nil {
		t.Fatalf("applyCmd GroupMemberDelete: %v", err)
	}

	groups, err := store.MembershipOf(context.Background(), "sa-1")
	if err != nil {
		t.Fatalf("MembershipOf: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("after remove, still in groups: %v", groups)
	}
}

// TestMetaFSM_Apply_GroupPut_NilStore_SafeNoop verifies that a GroupPut cmd is a
// safe no-op when groupStore has not been wired (nil).
func TestMetaFSM_Apply_GroupPut_NilStore_SafeNoop(t *testing.T) {
	f := NewMetaFSM()
	if err := f.applyCmd(buildGroupPutCmd(t, "noop", nil)); err != nil {
		t.Fatalf("GroupPut with nil store: got error %v, want nil", err)
	}
}

// TestMetaFSM_Apply_GroupDelete_NilStore_SafeNoop verifies that a GroupDelete cmd
// is a safe no-op when groupStore has not been wired (nil).
func TestMetaFSM_Apply_GroupDelete_NilStore_SafeNoop(t *testing.T) {
	f := NewMetaFSM()
	if err := f.applyCmd(buildGroupDeleteCmd(t, "noop")); err != nil {
		t.Fatalf("GroupDelete with nil store: got error %v, want nil", err)
	}
}

// TestMetaFSM_Apply_GroupMemberPut_NilStore_SafeNoop verifies that a GroupMemberPut
// cmd is a safe no-op when groupStore has not been wired (nil).
func TestMetaFSM_Apply_GroupMemberPut_NilStore_SafeNoop(t *testing.T) {
	f := NewMetaFSM()
	if err := f.applyCmd(buildGroupMemberPutCmd(t, "g", "sa-1")); err != nil {
		t.Fatalf("GroupMemberPut with nil store: got error %v, want nil", err)
	}
}

// TestMetaFSM_Apply_GroupMemberDelete_NilStore_SafeNoop verifies that a GroupMemberDelete
// cmd is a safe no-op when groupStore has not been wired (nil).
func TestMetaFSM_Apply_GroupMemberDelete_NilStore_SafeNoop(t *testing.T) {
	f := NewMetaFSM()
	if err := f.applyCmd(buildGroupMemberDeleteCmd(t, "g", "sa-1")); err != nil {
		t.Fatalf("GroupMemberDelete with nil store: got error %v, want nil", err)
	}
}
