package admin

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// CreateGroup proposes a GroupPut command via Raft (empty policies; attach happens
// separately via AttachPolicyToGroup).
func CreateGroup(ctx context.Context, d *Deps, name string) error {
	if d.IAMGroup == nil {
		return NewInternal("iam group admin disabled")
	}
	payload, err := cluster.EncodeGroupPutPayload(name, []string{})
	if err != nil {
		return err
	}
	return d.IAMGroup.Propose(ctx, clusterpb.MetaCmdTypeGroupPut, payload)
}

// DeleteGroup proposes a GroupDelete command via Raft.
func DeleteGroup(ctx context.Context, d *Deps, name string) error {
	if d.IAMGroup == nil {
		return NewInternal("iam group admin disabled")
	}
	payload, err := cluster.EncodeGroupDeletePayload(name)
	if err != nil {
		return err
	}
	return d.IAMGroup.Propose(ctx, clusterpb.MetaCmdTypeGroupDelete, payload)
}

// AddGroupMember proposes a GroupMemberPut command via Raft.
func AddGroupMember(ctx context.Context, d *Deps, group, saID string) error {
	if d.IAMGroup == nil {
		return NewInternal("iam group admin disabled")
	}
	if group == "" || saID == "" {
		return NewInvalid("group and sa_id are required")
	}
	payload, err := cluster.EncodeGroupMemberPutPayload(group, saID)
	if err != nil {
		return err
	}
	return d.IAMGroup.Propose(ctx, clusterpb.MetaCmdTypeGroupMemberPut, payload)
}

// RemoveGroupMember proposes a GroupMemberDelete command via Raft.
func RemoveGroupMember(ctx context.Context, d *Deps, group, saID string) error {
	if d.IAMGroup == nil {
		return NewInternal("iam group admin disabled")
	}
	if group == "" || saID == "" {
		return NewInvalid("group and sa_id are required")
	}
	payload, err := cluster.EncodeGroupMemberDeletePayload(group, saID)
	if err != nil {
		return err
	}
	return d.IAMGroup.Propose(ctx, clusterpb.MetaCmdTypeGroupMemberDelete, payload)
}

// AttachPolicyToGroup proposes a PolicyAttachToGroupPut command via Raft.
func AttachPolicyToGroup(ctx context.Context, d *Deps, group, policy string) error {
	if d.IAMGroup == nil {
		return NewInternal("iam group admin disabled")
	}
	if group == "" || policy == "" {
		return NewInvalid(fmt.Sprintf("group and policy are required (got group=%q policy=%q)", group, policy))
	}
	payload, err := cluster.EncodePolicyAttachToGroupPutPayload(group, policy)
	if err != nil {
		return err
	}
	return d.IAMGroup.Propose(ctx, clusterpb.MetaCmdTypePolicyAttachToGroupPut, payload)
}

// DetachPolicyFromGroup proposes a PolicyAttachToGroupDelete command via Raft.
func DetachPolicyFromGroup(ctx context.Context, d *Deps, group, policy string) error {
	if d.IAMGroup == nil {
		return NewInternal("iam group admin disabled")
	}
	if group == "" || policy == "" {
		return NewInvalid(fmt.Sprintf("group and policy are required (got group=%q policy=%q)", group, policy))
	}
	payload, err := cluster.EncodePolicyAttachToGroupDeletePayload(group, policy)
	if err != nil {
		return err
	}
	return d.IAMGroup.Propose(ctx, clusterpb.MetaCmdTypePolicyAttachToGroupDelete, payload)
}
