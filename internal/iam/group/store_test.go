package group

import (
	"context"
	"errors"
	"sort"
	"testing"
)

func TestGroup_PutAndAddMember(t *testing.T) {
	s := NewInMemoryStore()
	if err := s.Put(context.Background(), "admins", []string{"readwrite"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.AddMember(context.Background(), "admins", "sa-1"); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	groups, _ := s.MembershipOf(context.Background(), "sa-1")
	if len(groups) != 1 || groups[0] != "admins" {
		t.Fatalf("groups = %v", groups)
	}
}

func TestGroup_PoliciesAttached(t *testing.T) {
	s := NewInMemoryStore()
	_ = s.Put(context.Background(), "g", []string{"readonly", "custom-1"})
	pol, _ := s.AttachedPolicies(context.Background(), "g")
	if len(pol) != 2 {
		t.Fatalf("policies = %v", pol)
	}
}

func TestGroup_RemoveMember(t *testing.T) {
	s := NewInMemoryStore()
	_ = s.Put(context.Background(), "g", nil)
	_ = s.AddMember(context.Background(), "g", "sa-1")
	if err := s.RemoveMember(context.Background(), "g", "sa-1"); err != nil {
		t.Fatalf("RemoveMember: %v", err)
	}
	members, _ := s.MembershipOf(context.Background(), "sa-1")
	if len(members) != 0 {
		t.Fatalf("after remove, still in groups: %v", members)
	}
}

func TestGroup_DeleteNotFound(t *testing.T) {
	s := NewInMemoryStore()
	if err := s.Delete(context.Background(), "nope"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("Delete: got %v, want ErrGroupNotFound", err)
	}
}

func TestGroup_PutPreservesMembersOnUpdate(t *testing.T) {
	s := NewInMemoryStore()
	_ = s.Put(context.Background(), "g", []string{"p1"})
	_ = s.AddMember(context.Background(), "g", "sa-1")
	_ = s.Put(context.Background(), "g", []string{"p2", "p3"}) // update policies
	members, _ := s.MembersOf(context.Background(), "g")
	if len(members) != 1 || members[0] != "sa-1" {
		t.Fatalf("members lost on Put update: %v", members)
	}
	pol, _ := s.AttachedPolicies(context.Background(), "g")
	sort.Strings(pol)
	if len(pol) != 2 || pol[0] != "p2" || pol[1] != "p3" {
		t.Fatalf("policies = %v, want [p2 p3]", pol)
	}
}
