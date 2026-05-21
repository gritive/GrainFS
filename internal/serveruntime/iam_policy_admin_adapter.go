package serveruntime

import (
	"context"
	"errors"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/policystore"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// iamPolicyAdminAdapter bridges IAMStores + MetaRaft.Propose into admin.IAMPolicyService.
type iamPolicyAdminAdapter struct {
	stores  *IAMStores
	propose func(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error
}

var _ admin.IAMPolicyService = (*iamPolicyAdminAdapter)(nil)

// NewIAMPolicyAdminAdapter constructs the adapter. propose is typically
// state.metaRaft.Propose.
func NewIAMPolicyAdminAdapter(
	stores *IAMStores,
	propose func(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error,
) admin.IAMPolicyService {
	return &iamPolicyAdminAdapter{stores: stores, propose: propose}
}

func (a *iamPolicyAdminAdapter) Propose(
	ctx context.Context,
	cmdType clusterpb.MetaCmdType,
	payload []byte,
) error {
	return a.propose(ctx, cmdType, payload)
}

func (a *iamPolicyAdminAdapter) PolicyDoc(ctx context.Context, name string) ([]byte, error) {
	raw, err := a.stores.Policies.GetRaw(ctx, name)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return raw, nil
}

func (a *iamPolicyAdminAdapter) PolicyList(_ context.Context) ([]string, error) {
	return a.stores.Policies.List(), nil
}

// Simulate evaluates a hypothetical request against current IAM state using
// the Resolver and policy.Evaluate.
func (a *iamPolicyAdminAdapter) Simulate(
	ctx context.Context,
	saID, action, resource string,
) (admin.PolicySimulateResult, error) {
	in, err := a.stores.Resolver.Effective(ctx, saID, "" /* no bucket scoping for simulate */, policy.PrincipalTypeS3)
	if err != nil {
		return admin.PolicySimulateResult{}, err
	}
	in.Ctx = policy.RequestContext{Action: action, Resource: resource}
	result := policy.Evaluate(in)
	return admin.PolicySimulateResult{
		Effect:        result.Decision.String(),
		MatchedPolicy: result.MatchedPolicy,
		MatchedSID:    result.MatchedSid,
		Reason:        result.Reason,
	}, nil
}

// isNotFound returns true if err is policystore.ErrPolicyNotFound.
func isNotFound(err error) bool {
	return errors.Is(err, policystore.ErrPolicyNotFound)
}
