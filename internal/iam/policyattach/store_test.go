package policyattach

import (
	"context"
	"sort"
	"testing"
)

func TestPolicyAttach_SARoundTrip(t *testing.T) {
	s := NewInMemoryStore()
	_ = s.AttachToSA(context.Background(), "sa-1", "readonly")
	_ = s.AttachToSA(context.Background(), "sa-1", "custom-1")
	got, _ := s.SAPolicies(context.Background(), "sa-1")
	sort.Strings(got)
	if len(got) != 2 || got[0] != "custom-1" || got[1] != "readonly" {
		t.Fatalf("policies = %v", got)
	}
	_ = s.DetachFromSA(context.Background(), "sa-1", "readonly")
	got, _ = s.SAPolicies(context.Background(), "sa-1")
	if len(got) != 1 || got[0] != "custom-1" {
		t.Fatalf("after detach: %v", got)
	}
}

func TestPolicyAttach_GroupRoundTrip(t *testing.T) {
	s := NewInMemoryStore()
	_ = s.AttachToGroup(context.Background(), "admins", "bucket-admin")
	got, _ := s.GroupPolicies(context.Background(), "admins")
	if len(got) != 1 || got[0] != "bucket-admin" {
		t.Fatalf("group policies = %v", got)
	}
}

func TestPolicyAttach_DetachUnknownNoError(t *testing.T) {
	s := NewInMemoryStore()
	// detach on never-attached pair should not error.
	if err := s.DetachFromSA(context.Background(), "sa-x", "p"); err != nil {
		t.Fatal(err)
	}
	if err := s.DetachFromGroup(context.Background(), "g-x", "p"); err != nil {
		t.Fatal(err)
	}
}
