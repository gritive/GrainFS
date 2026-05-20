package admin

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/iam/builtin"
	"github.com/gritive/GrainFS/internal/iam/policy"
)

// PolicySimulateResult is returned by IAMPolicyService.Simulate.
type PolicySimulateResult struct {
	Effect        string `json:"effect"`
	MatchedPolicy string `json:"matched_policy"`
	MatchedSID    string `json:"matched_sid"`
	Reason        string `json:"reason"`
}

// PolicySimulateRequest is the wire body for POST /v1/iam/policy/simulate.
type PolicySimulateRequest struct {
	SAID     string `json:"sa_id"`
	Action   string `json:"action"`
	Resource string `json:"resource"`
}

// IAMPolicyService is the optional dependency that exposes policy CRUD via Raft.
// Nil disables the policy admin endpoints.
type IAMPolicyService interface {
	// Propose submits a Raft command.
	Propose(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error
	// PolicyDoc returns the raw JSON bytes for the named policy.
	PolicyDoc(ctx context.Context, name string) ([]byte, error)
	// PolicyList returns the names of all stored policies.
	PolicyList(ctx context.Context) ([]string, error)
	// Simulate evaluates a hypothetical request. Returns 501 if unimplemented.
	Simulate(ctx context.Context, saID, action, resource string) (PolicySimulateResult, error)
}

// PutPolicy stores or updates a custom policy document via Raft.
func PutPolicy(ctx context.Context, d *Deps, name string, docJSON []byte) error {
	if d.IAMPolicy == nil {
		return NewInternal("iam policy admin disabled")
	}
	// Parse to fail-fast; avoids a useless Raft round-trip.
	if _, err := policy.Parse(docJSON); err != nil {
		return NewInvalid(fmt.Sprintf("invalid policy: %v", err))
	}
	payload, err := cluster.EncodePolicyPutPayload(name, docJSON, false)
	if err != nil {
		return err
	}
	return d.IAMPolicy.Propose(ctx, clusterpb.MetaCmdTypePolicyPut, payload)
}

// GetPolicy returns the raw JSON bytes for the named policy.
func GetPolicy(ctx context.Context, d *Deps, name string) ([]byte, error) {
	if d.IAMPolicy == nil {
		return nil, NewInternal("iam policy admin disabled")
	}
	raw, err := d.IAMPolicy.PolicyDoc(ctx, name)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, NewNotFound(fmt.Sprintf("policy %q not found", name))
	}
	return raw, nil
}

// ListPolicies returns the names of all policies.
func ListPolicies(ctx context.Context, d *Deps) ([]string, error) {
	if d.IAMPolicy == nil {
		return nil, NewInternal("iam policy admin disabled")
	}
	return d.IAMPolicy.PolicyList(ctx)
}

// DeletePolicy removes a custom policy; refuses built-in names.
func DeletePolicy(ctx context.Context, d *Deps, name string) error {
	if d.IAMPolicy == nil {
		return NewInternal("iam policy admin disabled")
	}
	if builtin.IsBuiltinName(name) {
		return NewForbidden(fmt.Sprintf("cannot delete built-in policy: %q", name))
	}
	payload, err := cluster.EncodePolicyDeletePayload(name)
	if err != nil {
		return err
	}
	return d.IAMPolicy.Propose(ctx, clusterpb.MetaCmdTypePolicyDelete, payload)
}

// AttachPolicyToSA attaches a policy to a ServiceAccount via Raft.
func AttachPolicyToSA(ctx context.Context, d *Deps, policyName, saID string) error {
	if d.IAMPolicy == nil {
		return NewInternal("iam policy admin disabled")
	}
	payload, err := cluster.EncodePolicyAttachToSAPutPayload(saID, policyName)
	if err != nil {
		return err
	}
	return d.IAMPolicy.Propose(ctx, clusterpb.MetaCmdTypePolicyAttachToSAPut, payload)
}

// DetachPolicyFromSA detaches a policy from a ServiceAccount via Raft.
func DetachPolicyFromSA(ctx context.Context, d *Deps, policyName, saID string) error {
	if d.IAMPolicy == nil {
		return NewInternal("iam policy admin disabled")
	}
	payload, err := cluster.EncodePolicyAttachToSADeletePayload(saID, policyName)
	if err != nil {
		return err
	}
	return d.IAMPolicy.Propose(ctx, clusterpb.MetaCmdTypePolicyAttachToSADelete, payload)
}

// SimulatePolicy evaluates a hypothetical request.
func SimulatePolicy(ctx context.Context, d *Deps, req PolicySimulateRequest) (PolicySimulateResult, error) {
	if d.IAMPolicy == nil {
		return PolicySimulateResult{}, NewInternal("iam policy admin disabled")
	}
	return d.IAMPolicy.Simulate(ctx, req.SAID, req.Action, req.Resource)
}
