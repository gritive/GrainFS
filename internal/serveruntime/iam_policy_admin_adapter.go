package serveruntime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/policystore"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// iamPolicyAdminAdapter bridges IAMStores + MetaRaft.Propose into admin.IAMPolicyService.
type iamPolicyAdminAdapter struct {
	stores  *IAMStores
	propose func(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error
}

var _ admin.IAMPolicyService = (*iamPolicyAdminAdapter)(nil)
var _ admin.AdminSelfEffectGuard = (*iamPolicyAdminAdapter)(nil)

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

func (a *iamPolicyAdminAdapter) PolicyAffectsPrincipal(ctx context.Context, actor principal.Principal, policyName string) (bool, error) {
	names, err := a.effectivePolicyNames(ctx, actor)
	if err != nil {
		return false, err
	}
	return containsString(names, policyName), nil
}

func (a *iamPolicyAdminAdapter) GroupAffectsPrincipal(ctx context.Context, actor principal.Principal, group string) (bool, error) {
	switch actor.Kind {
	case principal.KindOIDC:
		return containsString(actor.GroupNames(), group), nil
	case principal.KindServiceAccount, principal.KindProtocolCredential:
		groups, err := a.stores.Groups.MembershipOf(ctx, actor.ID)
		if err != nil {
			return false, err
		}
		return containsString(groups, group), nil
	default:
		return false, nil
	}
}

func (a *iamPolicyAdminAdapter) effectivePolicyNames(ctx context.Context, actor principal.Principal) ([]string, error) {
	switch actor.Kind {
	case principal.KindOIDC:
		names, err := a.stores.Attach.SAPolicies(ctx, actor.ID)
		if err != nil {
			return nil, err
		}
		for _, group := range actor.GroupNames() {
			groupNames, err := a.stores.Adapter.GroupPolicies(ctx, group)
			if err != nil {
				return nil, err
			}
			names = append(names, groupNames...)
		}
		return names, nil
	case principal.KindServiceAccount, principal.KindProtocolCredential:
		names, err := a.stores.Attach.SAPolicies(ctx, actor.ID)
		if err != nil {
			return nil, err
		}
		groups, err := a.stores.Groups.MembershipOf(ctx, actor.ID)
		if err != nil {
			return nil, err
		}
		for _, group := range groups {
			groupNames, err := a.stores.Adapter.GroupPolicies(ctx, group)
			if err != nil {
				return nil, err
			}
			names = append(names, groupNames...)
		}
		return names, nil
	default:
		return nil, nil
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// Simulate evaluates a hypothetical request against current IAM state using
// the Resolver and policy.Evaluate.
func (a *iamPolicyAdminAdapter) Simulate(
	ctx context.Context,
	req admin.PolicySimulateRequest,
) (admin.PolicySimulateResult, error) {
	p, err := simulatePrincipal(req)
	if err != nil {
		return admin.PolicySimulateResult{}, err
	}
	in, err := a.stores.Resolver.EffectivePrincipal(ctx, p, "" /* no bucket scoping for simulate */)
	if err != nil {
		return admin.PolicySimulateResult{}, err
	}
	in.Ctx = policy.RequestContext{Action: req.Action, Resource: req.Resource}
	result := policy.Evaluate(in)
	return admin.PolicySimulateResult{
		Effect:        result.Decision.String(),
		MatchedPolicy: result.MatchedPolicy,
		MatchedSID:    result.MatchedSid,
		Reason:        result.Reason,
		PrincipalKind: string(p.Kind),
		PrincipalID:   p.ID,
		Issuer:        p.Issuer,
		Subject:       p.Subject,
		Groups:        p.GroupNames(),
	}, nil
}

func simulatePrincipal(req admin.PolicySimulateRequest) (principal.Principal, error) {
	if req.SAID != "" {
		return principal.ServiceAccount(req.SAID), nil
	}
	switch principal.Kind(req.PrincipalKind) {
	case principal.KindServiceAccount:
		return principal.ServiceAccount(req.PrincipalID), nil
	case principal.KindProtocolCredential:
		return principal.ProtocolCredential(req.PrincipalID, ""), nil
	case principal.KindOIDC:
		if req.Issuer != "" && req.Subject != "" {
			expected := oidcPrincipalID(req.Issuer, req.Subject)
			if req.PrincipalID != expected {
				return principal.Principal{}, admin.NewInvalid(fmt.Sprintf("oidc principal_id mismatch: got %q want %q", req.PrincipalID, expected))
			}
		}
		return principal.OIDC(req.Issuer, req.Subject, req.PrincipalID, req.Groups), nil
	default:
		return principal.Principal{}, admin.NewInvalid(fmt.Sprintf("unsupported principal_kind %q", req.PrincipalKind))
	}
}

func oidcPrincipalID(issuer, subject string) string {
	issuerSum := sha256.Sum256([]byte(issuer))
	subjectSum := sha256.Sum256([]byte(subject))
	return fmt.Sprintf("oidc:%x:%x", issuerSum[:8], subjectSum[:16])
}

// isNotFound returns true if err is policystore.ErrPolicyNotFound.
func isNotFound(err error) bool {
	return errors.Is(err, policystore.ErrPolicyNotFound)
}
