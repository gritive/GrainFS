package iamadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// RunMountSACreate creates a new MountSA with the given name and numeric UID.
func RunMountSACreate(ctx context.Context, opts MountSACreateOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	item, err := c.MountSACreate(ctx, opts.Name, opts.UID, opts.CreatedBy)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return json.NewEncoder(opts.Stdout).Encode(item)
	}
	fmt.Fprintf(opts.Stdout, "mount-sa %q created (uid=%d)\n", item.Name, item.UID)
	return nil
}

// RunMountSAList lists all MountSAs.
func RunMountSAList(ctx context.Context, opts MountSAListOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	items, err := c.MountSAList(ctx)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return json.NewEncoder(opts.Stdout).Encode(items)
	}
	if len(items) == 0 {
		fmt.Fprintln(opts.Stdout, "(no mount service accounts)")
		return nil
	}
	for _, item := range items {
		printMountSAItem(opts.Stdout, item)
	}
	return nil
}

// RunMountSAGet retrieves a single MountSA.
func RunMountSAGet(ctx context.Context, opts MountSAGetOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	item, err := c.MountSAGet(ctx, opts.Name)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return json.NewEncoder(opts.Stdout).Encode(item)
	}
	printMountSAItem(opts.Stdout, item)
	return nil
}

// RunMountSADelete deletes a MountSA by name.
func RunMountSADelete(ctx context.Context, opts MountSADeleteOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.MountSADelete(ctx, opts.Name); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "mount-sa %q deleted\n", opts.Name)
	return nil
}

// RunMountSAPolicyAttach attaches a policy to a MountSA.
func RunMountSAPolicyAttach(ctx context.Context, opts MountSAPolicyAttachOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.MountSAPolicyAttach(ctx, opts.MountSA, opts.PolicyName); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "policy %q attached to mount-sa %q\n", opts.PolicyName, opts.MountSA)
	return nil
}

// RunMountSAPolicyDetach detaches a policy from a MountSA.
func RunMountSAPolicyDetach(ctx context.Context, opts MountSAPolicyDetachOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.MountSAPolicyDetach(ctx, opts.MountSA, opts.PolicyName); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "policy %q detached from mount-sa %q\n", opts.PolicyName, opts.MountSA)
	return nil
}

func printMountSAItem(w io.Writer, item MountSAItem) {
	createdBy := item.CreatedBy
	if createdBy == "" {
		createdBy = "-"
	}
	fmt.Fprintf(w, "name=%-24s uid=%-8d created_by=%s\n", item.Name, item.UID, createdBy)
}
