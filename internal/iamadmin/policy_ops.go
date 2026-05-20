package iamadmin

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/gritive/GrainFS/internal/iam/policy"
)

// RunPolicyPut reads the policy doc from opts.FilePath and uploads it.
func RunPolicyPut(ctx context.Context, opts PolicyPutOptions) error {
	doc, err := os.ReadFile(opts.FilePath)
	if err != nil {
		return fmt.Errorf("read policy file: %w", err)
	}
	// Validate locally before sending to catch obvious errors.
	if _, err := policy.Parse(doc); err != nil {
		return fmt.Errorf("invalid policy: %w", err)
	}
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	return c.PolicyPut(ctx, opts.Name, doc)
}

// RunPolicyGet fetches and prints the raw policy document.
func RunPolicyGet(ctx context.Context, opts PolicyGetOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	raw, err := c.PolicyGet(ctx, opts.Name)
	if err != nil {
		return err
	}
	WriteRawWithNewline(opts.Stdout, raw)
	return nil
}

// RunPolicyList prints the names of all policies.
func RunPolicyList(ctx context.Context, opts PolicyListOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	names, err := c.PolicyList(ctx)
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return emitJSON(opts.Stdout, names)
	}
	for _, n := range names {
		fmt.Fprintln(opts.Stdout, n)
	}
	return nil
}

// RunPolicyDelete deletes the named policy; the server refuses built-in names.
func RunPolicyDelete(ctx context.Context, opts PolicyDeleteOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := c.PolicyDelete(ctx, opts.Name); err != nil {
		return err
	}
	fmt.Fprintf(opts.Stdout, "Deleted policy %s\n", opts.Name)
	return nil
}

// RunPolicyAttach attaches a policy to an SA (or group). Before attaching it
// fetches the policy doc and warns if it uses Resource:* (unless --i-know).
func RunPolicyAttach(ctx context.Context, opts PolicyAttachOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()

	// Fetch the policy doc to warn on Resource:*.
	raw, err := c.PolicyGet(ctx, opts.PolicyName)
	if err != nil {
		return err
	}
	warnResourceStar(opts.Stderr, opts.PolicyName, raw, opts.IKnow)

	if opts.SAID != "" {
		return c.PolicyAttachToSA(ctx, opts.PolicyName, opts.SAID)
	}
	return fmt.Errorf("attach requires --sa; to attach a policy to a group use 'grainfs iam group policy attach'")
}

// RunPolicyDetach detaches a policy from an SA.
func RunPolicyDetach(ctx context.Context, opts PolicyDetachOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if opts.SAID != "" {
		return c.PolicyDetachFromSA(ctx, opts.PolicyName, opts.SAID)
	}
	return fmt.Errorf("detach requires --sa; to detach a policy from a group use 'grainfs iam group policy detach'")
}

// RunPolicyValidate reads the policy file and parses it locally — no UDS dial.
// Returns a non-nil error if the document is invalid.
func RunPolicyValidate(_ context.Context, opts PolicyValidateOptions) error {
	doc, err := os.ReadFile(opts.FilePath)
	if err != nil {
		return fmt.Errorf("read policy file: %w", err)
	}
	if _, err := policy.Parse(doc); err != nil {
		return fmt.Errorf("invalid policy: %w", err)
	}
	fmt.Fprintln(opts.Stdout, "Policy is valid.")
	return nil
}

// RunPolicySimulate sends a simulate request to the admin server.
func RunPolicySimulate(ctx context.Context, opts PolicySimulateOptions) error {
	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.PolicySimulate(ctx, PolicySimulateRequest{
		SAID:     opts.SAID,
		Action:   opts.Action,
		Resource: opts.Resource,
	})
	if err != nil {
		return err
	}
	if opts.JSONOut {
		return emitJSON(opts.Stdout, resp)
	}
	renderPolicySimulateText(opts.Stdout, resp)
	return nil
}

func renderPolicySimulateText(w io.Writer, r PolicySimulateResponse) {
	fmt.Fprintf(w, "Effect:         %s\n", r.Effect)
	fmt.Fprintf(w, "MatchedPolicy:  %s\n", r.MatchedPolicy)
	fmt.Fprintf(w, "MatchedSID:     %s\n", r.MatchedSID)
	fmt.Fprintf(w, "Reason:         %s\n", r.Reason)
}

// warnResourceStar parses the policy doc and warns on stderr if any statement
// uses Resource:* (wildcard). Suppressed if iKnow is true.
func warnResourceStar(w io.Writer, name string, docJSON []byte, iKnow bool) {
	if iKnow {
		return
	}
	doc, err := policy.Parse(docJSON)
	if err != nil {
		return // parse error will surface elsewhere
	}
	for _, st := range doc.Statement {
		for _, res := range []string(st.Resource) {
			if res == "*" {
				fmt.Fprintf(w,
					"WARNING: policy %q uses Resource:* — grants access to ALL buckets, current and future. "+
						"For per-bucket scoping, write a custom policy. "+
						"Suppress this warning with --i-know.\n",
					name)
				return
			}
		}
	}
}
