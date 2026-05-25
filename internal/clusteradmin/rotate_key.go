package clusteradmin

import (
	"context"
)

// RotationStatus mirrors RotationStatusResponse on the server side. The
// rotate-key socket has stricter file-mode (0600) than admin.sock (0660),
// so it carries PSK material; the client lives in the same package as
// admin client to share dial / dispatch helpers but talks to a separate
// UDS endpoint (<data-dir>/rotate.sock).
type RotationStatus struct {
	State      string `json:"state"`
	RotationID string `json:"rotation_id,omitempty"`
	OldSPKI    string `json:"old_spki,omitempty"`
	NewSPKI    string `json:"new_spki,omitempty"`
	Error      string `json:"error,omitempty"`
}

// RotateKeyStatus fetches GET /v1/rotate-key/status. Returns the typed
// status; callers branch on Error for in-band server rejections.
func (c *Client) RotateKeyStatus(ctx context.Context) (*RotationStatus, error) {
	var out RotationStatus
	if err := c.Get(ctx, "/v1/rotate-key/status", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RotateKeyBegin issues POST /v1/rotate-key/begin with the provided new
// PSK (64 hex chars / 32 bytes). The server validates and runs the begin
// state-machine transition.
func (c *Client) RotateKeyBegin(ctx context.Context, newKey string) (*RotationStatus, error) {
	var out RotationStatus
	if err := c.Post(ctx, "/v1/rotate-key/begin", map[string]string{"new_key": newKey}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RotateKeyAbort issues POST /v1/rotate-key/abort with the operator-
// supplied reason. Empty reason becomes "operator" on the server.
func (c *Client) RotateKeyAbort(ctx context.Context, reason string) (*RotationStatus, error) {
	var out RotationStatus
	if err := c.Post(ctx, "/v1/rotate-key/abort", map[string]string{"reason": reason}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
