package cluster

import (
	"testing"
)

func TestGroupCodec_PutRoundTrip(t *testing.T) {
	policies := []string{"readonly", "custom-1"}
	data, err := EncodeGroupPutPayload("admins", policies)
	if err != nil {
		t.Fatalf("EncodeGroupPutPayload: %v", err)
	}
	gotName, gotPolicies, err := DecodeGroupPutPayload(data)
	if err != nil {
		t.Fatalf("DecodeGroupPutPayload: %v", err)
	}
	if gotName != "admins" {
		t.Fatalf("name: got %q, want %q", gotName, "admins")
	}
	if len(gotPolicies) != len(policies) {
		t.Fatalf("policies len: got %d, want %d", len(gotPolicies), len(policies))
	}
	for i, p := range policies {
		if gotPolicies[i] != p {
			t.Fatalf("policy[%d]: got %q, want %q", i, gotPolicies[i], p)
		}
	}
}

func TestGroupCodec_PutEmptyPolicies(t *testing.T) {
	data, err := EncodeGroupPutPayload("empty-group", nil)
	if err != nil {
		t.Fatalf("EncodeGroupPutPayload: %v", err)
	}
	gotName, gotPolicies, err := DecodeGroupPutPayload(data)
	if err != nil {
		t.Fatalf("DecodeGroupPutPayload: %v", err)
	}
	if gotName != "empty-group" {
		t.Fatalf("name: got %q, want %q", gotName, "empty-group")
	}
	if len(gotPolicies) != 0 {
		t.Fatalf("expected empty policies, got %v", gotPolicies)
	}
}

func TestGroupCodec_DeleteRoundTrip(t *testing.T) {
	data, err := EncodeGroupDeletePayload("admins")
	if err != nil {
		t.Fatalf("EncodeGroupDeletePayload: %v", err)
	}
	gotName, err := DecodeGroupDeletePayload(data)
	if err != nil {
		t.Fatalf("DecodeGroupDeletePayload: %v", err)
	}
	if gotName != "admins" {
		t.Fatalf("name: got %q, want %q", gotName, "admins")
	}
}

func TestGroupCodec_MemberPutRoundTrip(t *testing.T) {
	data, err := EncodeGroupMemberPutPayload("admins", "sa-abc123")
	if err != nil {
		t.Fatalf("EncodeGroupMemberPutPayload: %v", err)
	}
	gotGroup, gotSAID, err := DecodeGroupMemberPutPayload(data)
	if err != nil {
		t.Fatalf("DecodeGroupMemberPutPayload: %v", err)
	}
	if gotGroup != "admins" {
		t.Fatalf("group: got %q, want %q", gotGroup, "admins")
	}
	if gotSAID != "sa-abc123" {
		t.Fatalf("sa_id: got %q, want %q", gotSAID, "sa-abc123")
	}
}

func TestGroupCodec_MemberDeleteRoundTrip(t *testing.T) {
	data, err := EncodeGroupMemberDeletePayload("admins", "sa-abc123")
	if err != nil {
		t.Fatalf("EncodeGroupMemberDeletePayload: %v", err)
	}
	gotGroup, gotSAID, err := DecodeGroupMemberDeletePayload(data)
	if err != nil {
		t.Fatalf("DecodeGroupMemberDeletePayload: %v", err)
	}
	if gotGroup != "admins" {
		t.Fatalf("group: got %q, want %q", gotGroup, "admins")
	}
	if gotSAID != "sa-abc123" {
		t.Fatalf("sa_id: got %q, want %q", gotSAID, "sa-abc123")
	}
}
