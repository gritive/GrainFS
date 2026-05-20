package clusteradmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// JoinRequest is the POST /v1/cluster/join body. Mirrors
// serveruntime.JoinRequest on the wire without importing it; clusteradmin is
// the cmd-facing surface and must not pull in serveruntime.
type JoinRequest struct {
	PeerAddr string `json:"peer_addr"`
	Force    bool   `json:"force,omitempty"`
}

// JoinResult is the 2xx response body of POST /v1/cluster/join. Mirrors
// serveruntime.JoinResponse. Status values include "restart_initiated",
// "already_member", "self".
type JoinResult struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// JoinConflictError wraps a 409 response from POST /v1/cluster/join. The
// server returns a plain {status,message} JSON body (no {code,error}
// envelope), so adminapi.Transport's parseErrorBody lands the raw body in
// Error.Message; we re-parse it here so cmd never has to know about the
// JSON-in-JSON shape.
type JoinConflictError struct {
	Status  string
	Message string
}

func (e *JoinConflictError) Error() string {
	if e.Message != "" {
		return e.Status + ": " + e.Message
	}
	return e.Status
}

// JoinViaUDS sends POST /v1/cluster/join through the admin transport.
//
// On 2xx the typed JoinResult carries the server's status/message verbatim
// (printed by cmd). On 409 the response body is wrapped in
// *JoinConflictError so callers can branch via errors.As — matches the
// RemovePeer/TransferLeader typed-error pattern in this package.
func (c *Client) JoinViaUDS(ctx context.Context, peerAddr string, force bool) (*JoinResult, error) {
	req := JoinRequest{PeerAddr: peerAddr, Force: force}
	var resp JoinResult
	if err := c.Post(ctx, "/v1/cluster/join", req, &resp); err != nil {
		var apiErr *adminapi.Error
		if errors.As(err, &apiErr) && apiErr.Status == 409 {
			var body JoinResult
			if json.Unmarshal([]byte(apiErr.Message), &body) == nil && body.Status != "" {
				return nil, &JoinConflictError{Status: body.Status, Message: body.Message}
			}
		}
		return nil, fmt.Errorf("join request: %w", err)
	}
	return &resp, nil
}
