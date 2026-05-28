package iamadmin

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
)

// RunExplain maps a request-shaped CLI form to the policy simulator request.
func RunExplain(ctx context.Context, opts ExplainOptions) error {
	req, err := explainS3Request(opts.S3Verb, opts.S3URI)
	if err != nil {
		return err
	}
	req.SAID = opts.SAID

	c, err := dialClient(opts.BaseOptions)
	if err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	resp, err := c.PolicySimulate(ctx, req)
	if err != nil {
		return err
	}
	out := ExplainResponse{Request: req, Simulation: resp}
	if opts.JSONOut {
		return emitJSON(opts.Stdout, out)
	}
	renderExplainText(opts.Stdout, out)
	return nil
}

func explainS3Request(verb, rawURI string) (PolicySimulateRequest, error) {
	action, listBucket, err := s3VerbAction(verb)
	if err != nil {
		return PolicySimulateRequest{}, err
	}
	if !strings.HasPrefix(rawURI, "s3://") {
		return PolicySimulateRequest{}, fmt.Errorf("s3 URI must start with s3://")
	}
	u, err := url.Parse(rawURI)
	if err != nil {
		return PolicySimulateRequest{}, fmt.Errorf("parse s3 URI: %w", err)
	}
	if u.Scheme != "s3" || u.Host == "" {
		return PolicySimulateRequest{}, fmt.Errorf("s3 URI must be s3://bucket[/key]")
	}
	key := strings.TrimPrefix(u.EscapedPath(), "/")
	resource := "arn:aws:s3:::" + u.Host
	if !listBucket {
		if key == "" {
			return PolicySimulateRequest{}, fmt.Errorf("s3 %s requires an object key", strings.ToLower(verb))
		}
		resource += "/" + key
	}
	return PolicySimulateRequest{Action: action, Resource: resource}, nil
}

func s3VerbAction(verb string) (action string, listBucket bool, err error) {
	switch strings.ToLower(strings.TrimSpace(verb)) {
	case "get", "head":
		return "s3:GetObject", false, nil
	case "put":
		return "s3:PutObject", false, nil
	case "delete", "rm":
		return "s3:DeleteObject", false, nil
	case "list", "ls":
		return "s3:ListBucket", true, nil
	default:
		return "", false, fmt.Errorf("unsupported S3 verb %q", verb)
	}
}

func renderExplainText(w io.Writer, r ExplainResponse) {
	fmt.Fprintf(w, "Action:         %s\n", r.Request.Action)
	fmt.Fprintf(w, "Resource:       %s\n", r.Request.Resource)
	renderPolicySimulateText(w, r.Simulation)
}
