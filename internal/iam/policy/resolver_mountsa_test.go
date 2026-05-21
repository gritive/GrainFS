package policy

import (
	"context"
	"testing"
	"time"
)

// TestResolver_Effective_MountSAPolicies verifies that when PrincipalType is
// PrincipalTypeMount, Resolver.Effective consults MountSAPolicies rather
// than SAPolicies. This is the F-§B-resolver-mountsa fix: prior to FU#5
// the resolver always reads the S3-SA pool, so mount-SA principals with
// attached policies returned an empty principal-policy set and fell
// through to implicit Deny.
func TestResolver_Effective_MountSAPolicies(t *testing.T) {
	s := &fakeStore{
		mountSAToPols: map[string][]string{"alice": {"9PAttachOnly"}},
		docs: map[string]string{
			"9PAttachOnly": `{"Statement":[{"Effect":"Allow","Action":"grainfs:9PAttach","Resource":"*"}]}`,
		},
	}
	r := NewResolver(s, time.Minute)
	in, err := r.Effective(context.Background(), "alice", "bucket-x", PrincipalTypeMount)
	if err != nil {
		t.Fatalf("Effective: %v", err)
	}
	if len(in.PrincipalPolicies) != 1 {
		t.Fatalf("PrincipalPolicies len: got %d want 1", len(in.PrincipalPolicies))
	}
	if len(in.PrincipalPolicyNames) != 1 || in.PrincipalPolicyNames[0] != "9PAttachOnly" {
		t.Fatalf("PrincipalPolicyNames: got %#v want [9PAttachOnly]", in.PrincipalPolicyNames)
	}
}

// TestResolver_Effective_S3SAPolicies_Unchanged verifies the S3 path keeps
// reading the S3-SA pool and does NOT spill into the mount-SA pool when a
// name happens to collide. Mount-SA pool is only consulted when
// PrincipalType == PrincipalTypeMount.
func TestResolver_Effective_S3SAPolicies_Unchanged(t *testing.T) {
	s := &fakeStore{
		saToPols:      map[string][]string{"bob": {"S3ReadOnly"}},
		mountSAToPols: map[string][]string{"bob": {"9PAttachOnly"}}, // must be ignored on S3 path
		docs: map[string]string{
			"S3ReadOnly":   `{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`,
			"9PAttachOnly": `{"Statement":[{"Effect":"Allow","Action":"grainfs:9PAttach","Resource":"*"}]}`,
		},
	}
	r := NewResolver(s, time.Minute)
	in, err := r.Effective(context.Background(), "bob", "bucket-x", PrincipalTypeS3)
	if err != nil {
		t.Fatalf("Effective: %v", err)
	}
	if len(in.PrincipalPolicyNames) != 1 || in.PrincipalPolicyNames[0] != "S3ReadOnly" {
		t.Fatalf("PrincipalPolicyNames: got %#v want [S3ReadOnly]", in.PrincipalPolicyNames)
	}
}

// TestResolver_Effective_PrincipalTypeCacheSeparation verifies the cache
// key distinguishes S3 vs mount-SA principals with the same name. Without
// type-aware cache keys, the second lookup would be served the first
// lookup's policies.
func TestResolver_Effective_PrincipalTypeCacheSeparation(t *testing.T) {
	s := &fakeStore{
		saToPols:      map[string][]string{"alice": {"S3ReadOnly"}},
		mountSAToPols: map[string][]string{"alice": {"9PAttachOnly"}},
		docs: map[string]string{
			"S3ReadOnly":   `{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`,
			"9PAttachOnly": `{"Statement":[{"Effect":"Allow","Action":"grainfs:9PAttach","Resource":"*"}]}`,
		},
	}
	r := NewResolver(s, time.Minute)
	s3In, err := r.Effective(context.Background(), "alice", "bkt", PrincipalTypeS3)
	if err != nil {
		t.Fatalf("Effective S3: %v", err)
	}
	mntIn, err := r.Effective(context.Background(), "alice", "bkt", PrincipalTypeMount)
	if err != nil {
		t.Fatalf("Effective mount: %v", err)
	}
	if len(s3In.PrincipalPolicyNames) != 1 || s3In.PrincipalPolicyNames[0] != "S3ReadOnly" {
		t.Fatalf("s3 path: got %#v want [S3ReadOnly]", s3In.PrincipalPolicyNames)
	}
	if len(mntIn.PrincipalPolicyNames) != 1 || mntIn.PrincipalPolicyNames[0] != "9PAttachOnly" {
		t.Fatalf("mount path: got %#v want [9PAttachOnly]", mntIn.PrincipalPolicyNames)
	}
}

// TestResolver_Effective_MountSA_SkipsGroups verifies that mount-SA
// principals do NOT expand through the group store. Cross-namespace
// attach-time reject (NFS§A T4) is scoped to direct policy attach;
// groups remain S3-only.
func TestResolver_Effective_MountSA_SkipsGroups(t *testing.T) {
	s := &fakeStore{
		mountSAToPols: map[string][]string{"alice": {"9PAttachOnly"}},
		saToGroups:    map[string][]string{"alice": {"admins"}}, // must NOT be consulted
		groupToPols:   map[string][]string{"admins": {"S3FullAdmin"}},
		docs: map[string]string{
			"9PAttachOnly": `{"Statement":[{"Effect":"Allow","Action":"grainfs:9PAttach","Resource":"*"}]}`,
			"S3FullAdmin":  `{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
		},
	}
	r := NewResolver(s, time.Minute)
	in, err := r.Effective(context.Background(), "alice", "bucket-x", PrincipalTypeMount)
	if err != nil {
		t.Fatalf("Effective: %v", err)
	}
	if len(in.PrincipalPolicyNames) != 1 || in.PrincipalPolicyNames[0] != "9PAttachOnly" {
		t.Fatalf("mount-SA must not pull group policies: got %#v want [9PAttachOnly]", in.PrincipalPolicyNames)
	}
}
