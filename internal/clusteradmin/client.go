// Package clusteradmin is the admin-plane HTTP client for grainfs cluster
// operations. It is consumed by the CLI (cmd/grainfs) and tests; the server
// side lives in internal/server. Keeping this in internal/ rather than cmd/
// means the wire shapes, pre-flight rules, and rendering helpers are unit-
// testable in isolation and stay reusable from non-CLI callers.
package clusteradmin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client speaks to a single grainfs server's admin endpoints. Endpoint is
// expected to point at the local node (the membership endpoint is
// localhost-only); HTTPClient is reused across calls.
type Client struct {
	Endpoint   string
	HTTPClient *http.Client
}

// NewClient constructs a Client that trims trailing slashes on the endpoint
// and uses a per-call context for timeouts.
func NewClient(endpoint string) *Client {
	return &Client{
		Endpoint:   strings.TrimRight(endpoint, "/"),
		HTTPClient: http.DefaultClient,
	}
}

// Status mirrors the JSON shape returned by /api/cluster/status. Fields
// not present (e.g. local mode) decode to zero values; callers should branch
// on Mode.
type Status struct {
	Mode      string   `json:"mode"`
	NodeID    string   `json:"node_id,omitempty"`
	State     string   `json:"state,omitempty"`
	Term      uint64   `json:"term,omitempty"`
	LeaderID  string   `json:"leader_id,omitempty"`
	Peers     []string `json:"peers,omitempty"`
	DownNodes []string `json:"down_nodes,omitempty"`
}

// Event mirrors eventstore.Event without importing the server package, which
// would cycle internal/server -> internal/clusteradmin -> cmd back into
// internal/server through wiring.
type Event struct {
	Timestamp int64          `json:"ts"`
	Type      string         `json:"type"`
	Action    string         `json:"action"`
	Bucket    string         `json:"bucket,omitempty"`
	Key       string         `json:"key,omitempty"`
	User      string         `json:"user,omitempty"`
	Size      int64          `json:"size,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// RemovePeerError carries the structured fields the server returns for
// 404/409/503 responses so the CLI can render contextual messages
// (leader hint, quorum math) without re-parsing JSON.
type RemovePeerError struct {
	StatusCode  int
	Message     string
	LeaderID    string
	VotersAfter int
	AliveAfter  int
	NewQuorum   int
}

func (e *RemovePeerError) Error() string {
	if e.LeaderID != "" {
		return fmt.Sprintf("server: %s (leader=%s)", e.Message, e.LeaderID)
	}
	return "server: " + e.Message
}

// Status fetches /api/cluster/status. ctx controls the deadline; pass a
// context.WithTimeout to bound the call.
func (c *Client) Status(ctx context.Context) (*Status, error) {
	body, err := c.getJSON(ctx, "/api/cluster/status")
	if err != nil {
		return nil, err
	}
	var s Status
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("parse status: %w", err)
	}
	return &s, nil
}

// RemovePeer issues POST /api/cluster/remove-peer. On non-2xx responses the
// returned error is *RemovePeerError so callers can branch on status code
// and surface server-supplied context.
func (c *Client) RemovePeer(ctx context.Context, id string, force bool) error {
	body, _ := json.Marshal(map[string]any{"id": id, "force": force})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Endpoint+"/api/cluster/remove-peer", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		return nil
	}

	parsed := map[string]any{}
	_ = json.Unmarshal(raw, &parsed)
	rpe := &RemovePeerError{StatusCode: resp.StatusCode}
	if msg, ok := parsed["error"].(string); ok && msg != "" {
		rpe.Message = msg
	} else {
		rpe.Message = fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	if leader, ok := parsed["leader_id"].(string); ok {
		rpe.LeaderID = leader
	}
	rpe.VotersAfter = intField(parsed, "voters_after")
	rpe.AliveAfter = intField(parsed, "alive_after")
	rpe.NewQuorum = intField(parsed, "new_quorum")
	return rpe
}

// EventLog fetches /api/eventlog with the given since (lookback duration)
// and limit. The server endpoint is localhost-only.
func (c *Client) EventLog(ctx context.Context, since time.Duration, limit int) ([]Event, error) {
	url := fmt.Sprintf("%s/api/eventlog?since=%d&limit=%d", c.Endpoint, int64(since.Seconds()), limit)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("eventlog endpoint returned %d: %s", resp.StatusCode, string(body))
	}
	var out []Event
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("parse events: %w", err)
	}
	return out, nil
}

func (c *Client) getJSON(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Endpoint+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", c.Endpoint, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s returned %d: %s", path, resp.StatusCode, string(body))
	}
	return body, nil
}

func intField(m map[string]any, key string) int {
	v, ok := m[key]
	if !ok {
		return 0
	}
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	}
	return 0
}
