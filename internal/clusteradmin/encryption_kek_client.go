package clusteradmin

import (
	"context"
	"encoding/json"
	"fmt"
)

// KEKVersionStatus is the wire shape returned by GET /v1/encrypt/kek/status.
// Mirrors EncryptionKEKHandler's kekVersionStatus; kept here so the CLI
// and any non-handler callers have a typed home.
type KEKVersionStatus struct {
	Version uint32 `json:"version"`
	Status  string `json:"status"`
}

// KEKStatus is the wire shape returned by GET /v1/encrypt/kek/status.
type KEKStatus struct {
	ActiveVersion uint32             `json:"active_version"`
	Versions      []KEKVersionStatus `json:"versions"`
}

// EncryptKEKRotate issues POST /v1/encrypt/kek/rotate with the fixed
// confirm payload "rotate-now". The server validates the confirm string;
// the CLI must pass --i-know before calling this.
func (c *Client) EncryptKEKRotate(ctx context.Context) error {
	return c.Post(ctx, "/v1/encrypt/kek/rotate", map[string]any{
		"confirm": "rotate-now",
	}, nil)
}

// EncryptKEKRetire issues POST /v1/encrypt/kek/retire with version and
// confirm (expected: "delete-permanently-<version>"). The CLI validates the
// confirm string before calling this.
func (c *Client) EncryptKEKRetire(ctx context.Context, version uint32, confirm string) error {
	return c.Post(ctx, "/v1/encrypt/kek/retire", map[string]any{
		"version": version,
		"confirm": confirm,
	}, nil)
}

// EncryptKEKPrune issues POST /v1/encrypt/kek/prune with version and
// confirm (expected: "delete-permanently-<version>"). The CLI validates the
// confirm string before calling this.
func (c *Client) EncryptKEKPrune(ctx context.Context, version uint32, confirm string) error {
	return c.Post(ctx, "/v1/encrypt/kek/prune", map[string]any{
		"version": version,
		"confirm": confirm,
	}, nil)
}

// EncryptKEKStatus fetches GET /v1/encrypt/kek/status and returns the typed
// response.
func (c *Client) EncryptKEKStatus(ctx context.Context) (*KEKStatus, error) {
	body, err := c.GetRaw(ctx, "/v1/encrypt/kek/status")
	if err != nil {
		return nil, err
	}
	var s KEKStatus
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("parse kek status: %w", err)
	}
	return &s, nil
}
