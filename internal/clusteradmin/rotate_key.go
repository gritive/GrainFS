package clusteradmin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// RotationStatus mirrors RotationStatusResponse on the server side. The
// rotate-key socket has stricter file-mode (0600) than admin.sock (0660),
// so it carries PSK material; the client lives in the same package as
// admin client to share dial / dispatch helpers but talks to a separate
// UDS endpoint (<data-dir>/rotate.sock).
type RotationStatus struct {
	Phase      int    `json:"phase"`
	RotationID string `json:"rotation_id,omitempty"`
	OldSPKI    string `json:"old_spki,omitempty"`
	NewSPKI    string `json:"new_spki,omitempty"`
	Error      string `json:"error,omitempty"`
}

// RotateKeyStatus fetches GET /v1/rotate-key/status. Returns the typed
// status; callers branch on Error for in-band server rejections.
func (c *Client) RotateKeyStatus(ctx context.Context) (*RotationStatus, error) {
	return c.rotateKeyGet(ctx, "/v1/rotate-key/status")
}

// RotateKeyBegin issues POST /v1/rotate-key/begin with the provided new
// PSK (64 hex chars / 32 bytes). The server validates and runs the begin
// state-machine transition.
func (c *Client) RotateKeyBegin(ctx context.Context, newKey string) (*RotationStatus, error) {
	body, _ := json.Marshal(map[string]string{"new_key": newKey})
	return c.rotateKeyPost(ctx, "/v1/rotate-key/begin", body)
}

// RotateKeyAbort issues POST /v1/rotate-key/abort with the operator-
// supplied reason. Empty reason becomes "operator" on the server.
func (c *Client) RotateKeyAbort(ctx context.Context, reason string) (*RotationStatus, error) {
	body, _ := json.Marshal(map[string]string{"reason": reason})
	return c.rotateKeyPost(ctx, "/v1/rotate-key/abort", body)
}

func (c *Client) rotateKeyGet(ctx context.Context, path string) (*RotationStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, wrapDialError(err, c.sockPath)
	}
	defer resp.Body.Close()
	return decodeRotation(resp)
}

func (c *Client) rotateKeyPost(ctx context.Context, path string, body []byte) (*RotationStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, wrapDialError(err, c.sockPath)
	}
	defer resp.Body.Close()
	return decodeRotation(resp)
}

func decodeRotation(resp *http.Response) (*RotationStatus, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("rotate-key endpoint returned %d: %s", resp.StatusCode, string(body))
	}
	var out RotationStatus
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("parse rotate-key response: %w", err)
	}
	return &out, nil
}
