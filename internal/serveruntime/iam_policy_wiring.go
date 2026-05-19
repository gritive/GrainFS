package serveruntime

import (
	"context"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/iam/bucketpolicy"
	"github.com/gritive/GrainFS/internal/iam/builtin"
	"github.com/gritive/GrainFS/internal/iam/group"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/iam/policystore"
)

// IAMStores bundles the four in-memory IAM stores and the resolver. The bundle
// is constructed once per node at boot and injected into the MetaFSM so apply
// hooks for MetaCmds 50-61 land on the same instances that authz reads from.
type IAMStores struct {
	Policies   *policystore.InMemoryStore
	Groups     *group.InMemoryStore
	Attach     *policyattach.InMemoryStore
	BucketPols *bucketpolicy.InMemoryStore
	Resolver   *policy.Resolver
	Adapter    *policy.StoreAdapter
}

// WireIAMPolicyStores constructs the IAM store bundle, injects every store into
// the MetaFSM (idempotent — re-injection on snapshot restore is safe), and seeds
// the four built-in managed policies (readonly/readwrite/writeonly/bucket-admin).
//
// A zero or negative resolverTTL defaults to 5s per spec. Pass a nonzero value
// for tests that need deterministic cache expiry.
//
// The bundle is returned so callers (S3 auth path, admin handlers) can read
// policies without going through the FSM injectors.
//
// fsm may be nil — useful in unit tests that only need the bundle itself.
func WireIAMPolicyStores(ctx context.Context, fsm *cluster.MetaFSM, resolverTTL time.Duration) (*IAMStores, error) {
	if resolverTTL <= 0 {
		resolverTTL = 5 * time.Second
	}

	s := &IAMStores{
		Policies:   policystore.NewInMemoryStore(),
		Groups:     group.NewInMemoryStore(),
		Attach:     policyattach.NewInMemoryStore(),
		BucketPols: bucketpolicy.NewInMemoryStore(),
	}
	s.Adapter = &policy.StoreAdapter{
		Policies: s.Policies,
		Attach:   s.Attach,
		Groups:   s.Groups,
		Buckets:  s.BucketPols,
	}
	s.Resolver = policy.NewResolver(s.Adapter, resolverTTL)

	if fsm != nil {
		fsm.SetPolicyStore(s.Policies)
		fsm.SetGroupStore(s.Groups)
		fsm.SetPolicyAttachStore(s.Attach)
		fsm.SetBucketPolicyStore(s.BucketPols)
		fsm.SetPolicyResolver(s.Resolver)
	}

	if err := builtin.SeedAll(ctx, s.Policies); err != nil {
		return nil, err
	}
	return s, nil
}
