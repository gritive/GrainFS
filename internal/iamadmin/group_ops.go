package iamadmin

import (
	"context"
	"fmt"
)

// RunGroupCreate creates a new group with the given name (empty policies).
func RunGroupCreate(ctx context.Context, opts GroupCreateOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.GroupCreate(ctx, opts.Name); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "group %q created\n", opts.Name)
	return nil
}

// RunGroupDelete deletes the named group via Raft.
func RunGroupDelete(ctx context.Context, opts GroupDeleteOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.GroupDelete(ctx, opts.Name); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "group %q deleted\n", opts.Name)
	return nil
}

// RunGroupMemberAdd adds saID as a member of the named group.
func RunGroupMemberAdd(ctx context.Context, opts GroupMemberAddOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.GroupMemberAdd(ctx, opts.GroupName, opts.SAID); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "added %q to group %q\n", opts.SAID, opts.GroupName)
	return nil
}

// RunGroupMemberRemove removes saID from the named group.
func RunGroupMemberRemove(ctx context.Context, opts GroupMemberRemoveOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.GroupMemberRemove(ctx, opts.GroupName, opts.SAID); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "removed %q from group %q\n", opts.SAID, opts.GroupName)
	return nil
}

// RunGroupPolicyAttach attaches a policy to a group.
func RunGroupPolicyAttach(ctx context.Context, opts GroupPolicyAttachOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.GroupPolicyAttach(ctx, opts.GroupName, opts.PolicyName); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "policy %q attached to group %q\n", opts.PolicyName, opts.GroupName)
	return nil
}

// RunGroupPolicyDetach detaches a policy from a group.
func RunGroupPolicyDetach(ctx context.Context, opts GroupPolicyDetachOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.GroupPolicyDetach(ctx, opts.GroupName, opts.PolicyName); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "policy %q detached from group %q\n", opts.PolicyName, opts.GroupName)
	return nil
}
