package bucketadmin

import "context"

// RunPolicyGet prints the bucket's IAM policy document verbatim. Both --text
// and --json modes are identical (passthrough), matching the legacy CLI.
func RunPolicyGet(ctx context.Context, opts PolicyGetOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	raw, err := c.PolicyGetRaw(ctx, opts.Bucket)
	if err != nil {
		return err
	}
	WriteRawWithNewline(opts.Stdout, raw)
	return nil
}

// RunPolicySet sends the policy document bytes verbatim. The caller (cmd)
// is responsible for reading from file/stdin and passing valid JSON bytes;
// the server validates the policy. No re-marshal, so field ordering and
// any extensions in the user's document are preserved.
func RunPolicySet(ctx context.Context, opts PolicySetOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	return c.PolicySet(ctx, opts.Bucket, opts.Policy)
}

// RunPolicyDelete removes the IAM policy attached to a bucket.
// The legacy CLI prints no output on success.
func RunPolicyDelete(ctx context.Context, opts PolicyDeleteOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	return c.PolicyDelete(ctx, opts.Bucket)
}
