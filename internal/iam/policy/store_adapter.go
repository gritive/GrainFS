package policy

import (
	"context"
	"errors"

	"github.com/gritive/GrainFS/internal/iam/bucketpolicy"
	"github.com/gritive/GrainFS/internal/iam/group"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/iam/policystore"
)

// StoreAdapter bridges the four CRUD stores into the policy.Store interface
// consumed by Resolver. All fields must be non-nil before use.
type StoreAdapter struct {
	Policies *policystore.InMemoryStore
	Attach   *policyattach.InMemoryStore
	Groups   *group.InMemoryStore
	Buckets  *bucketpolicy.InMemoryStore
}

var _ Store = (*StoreAdapter)(nil)

func (a *StoreAdapter) SAPolicies(ctx context.Context, saID string) ([]string, error) {
	return a.Attach.SAPolicies(ctx, saID)
}

func (a *StoreAdapter) SAGroups(ctx context.Context, saID string) ([]string, error) {
	return a.Groups.MembershipOf(ctx, saID)
}

// GroupPolicies unions the policies attached directly to the group (group.Group.AttachedPolicies,
// set via GroupPut) with the policies attached separately via PolicyAttachToGroupPut.
// If the group itself does not exist (e.g. stale membership reference), returns nil
// rather than ErrGroupNotFound — the resolver treats a missing group as zero policies.
func (a *StoreAdapter) GroupPolicies(ctx context.Context, g string) ([]string, error) {
	gp, err := a.Groups.AttachedPolicies(ctx, g)
	if err != nil && !errors.Is(err, group.ErrGroupNotFound) {
		return nil, err
	}
	ap, err := a.Attach.GroupPolicies(ctx, g)
	if err != nil {
		return nil, err
	}
	return append(gp, ap...), nil
}

func (a *StoreAdapter) PolicyDoc(ctx context.Context, name string) (*Document, error) {
	raw, err := a.Policies.GetRaw(ctx, name)
	if err != nil {
		if errors.Is(err, policystore.ErrPolicyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return Parse(raw)
}

func (a *StoreAdapter) BucketPolicy(ctx context.Context, bucket string) (*Document, error) {
	raw, err := a.Buckets.Get(ctx, bucket)
	if err != nil {
		if errors.Is(err, bucketpolicy.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return Parse(raw)
}
