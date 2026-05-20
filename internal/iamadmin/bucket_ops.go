package iamadmin

import (
	"context"
	"fmt"
	"text/tabwriter"
)

// RunBucketCreate creates a bucket, optionally attaching an SA and policy.
func RunBucketCreate(ctx context.Context, opts BucketCreateOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.BucketCreate(ctx, opts.Name, opts.AttachSA, opts.AttachPolicy); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "Created bucket %s\n", opts.Name)
	return nil
}

// RunBucketDelete removes a bucket.
func RunBucketDelete(ctx context.Context, opts BucketDeleteOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.BucketDelete(ctx, opts.Name, opts.Force); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "Deleted bucket %s\n", opts.Name)
	return nil
}

// RunBucketList prints all user-facing buckets.
func RunBucketList(ctx context.Context, opts BucketListOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	items, err := c.BucketList(ctx)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return emitJSON(opts.Stdout, items)
	}
	tw := tabwriter.NewWriter(opts.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tHAS_UPSTREAM")
	for _, b := range items {
		upstream := "no"
		if b.HasUpstream {
			upstream = "yes"
		}
		fmt.Fprintf(tw, "%s\t%s\n", b.Name, upstream)
	}
	return tw.Flush()
}

// RunBucketPolicyPut uploads a raw JSON policy document to the named bucket.
func RunBucketPolicyPut(ctx context.Context, opts BucketPolicyPutOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	return c.BucketPolicyPut(ctx, opts.Bucket, opts.Policy)
}

// RunBucketPolicyDelete removes the bucket policy.
func RunBucketPolicyDelete(ctx context.Context, opts BucketPolicyDeleteOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	return c.BucketPolicyDelete(ctx, opts.Bucket)
}
