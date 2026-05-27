package clusteradmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// InviteCreateResult is the 2xx response body of POST
// /v1/cluster/invite/create. Mirrors serveruntime.InviteCreateResponse on the
// wire without importing it; clusteradmin is the cmd-facing surface and must
// not pull in serveruntime.
type InviteCreateResult struct {
	Bundle   string `json:"bundle"`
	InviteID string `json:"invite_id"`
}

// InviteNotLeaderError wraps the 409 response from POST
// /v1/cluster/invite/create. The handler returns a plain {error,leader_id}
// JSON body, so adminapi.Transport lands the raw body in Error.Message; we
// re-parse it here so cmd never has to know about the JSON-in-JSON shape.
// Mirrors JoinConflictError in this package.
type InviteNotLeaderError struct {
	Message  string
	LeaderID string
}

func (e *InviteNotLeaderError) Error() string {
	if e.LeaderID != "" {
		return e.Message + " (leader: " + e.LeaderID + ")"
	}
	return e.Message
}

// InviteCreate sends POST /v1/cluster/invite/create through the admin
// transport. ttl is the invite lifetime (0 lets the server apply its default).
//
// On 2xx the typed result carries the operator bundle token. On 409 (not
// leader) the body is wrapped in *InviteNotLeaderError so callers can branch
// via errors.As — matches the Join/RemovePeer/TransferLeader typed-error
// pattern in this package.
func (c *Client) InviteCreate(ctx context.Context, ttl time.Duration) (*InviteCreateResult, error) {
	req := map[string]int64{"ttl_nanos": int64(ttl)}
	var resp InviteCreateResult
	if err := c.Post(ctx, "/v1/cluster/invite/create", req, &resp); err != nil {
		var apiErr *adminapi.Error
		if errors.As(err, &apiErr) && apiErr.Status == 409 {
			// parseErrorBody lifts "error" → Message and lands the raw body in
			// Details; leader_id only lives in Details, so parse that.
			var body struct {
				Error    string `json:"error"`
				LeaderID string `json:"leader_id"`
			}
			if json.Unmarshal(apiErr.Details, &body) == nil && body.Error != "" {
				return nil, &InviteNotLeaderError{Message: body.Error, LeaderID: body.LeaderID}
			}
		}
		return nil, fmt.Errorf("invite create request: %w", err)
	}
	return &resp, nil
}

// InviteCreateOptions configures RunInviteCreate. Endpoint is the admin UDS
// path; TTL is the invite lifetime; Out receives the printed bundle.
type InviteCreateOptions struct {
	Endpoint string
	TTL      time.Duration
	Out      io.Writer
}

// RunInviteCreate is the thin-runner entry point for `grainfs cluster invite
// create`. It calls the admin route and prints the operator bundle token; the
// operator copies it into GRAINFS_INVITE_BUNDLE on the joining node.
func RunInviteCreate(ctx context.Context, opts InviteCreateOptions) error {
	res, err := NewClient(opts.Endpoint).InviteCreate(ctx, opts.TTL)
	if err != nil {
		return err
	}
	fmt.Fprintf(opts.Out, "Invite minted: id=%s\n", res.InviteID)
	fmt.Fprintln(opts.Out, "Set this on the joining node as GRAINFS_INVITE_BUNDLE:")
	fmt.Fprintln(opts.Out, res.Bundle)
	return nil
}
