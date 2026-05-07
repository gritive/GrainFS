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
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
)

// Client speaks to a single grainfs server's admin endpoints.
//
// Production CLI callers pass a UDS path (e.g. "<data-dir>/admin.sock"); the
// client dispatches on prefix:
//   - bare path        — UDS dialer (CLI-facing form)
//   - "unix:<path>"    — UDS dialer (legacy form)
//   - "http(s)://..."  — direct HTTP dial (test injection)
//
// HTTP routes are /v1/cluster/* on the admin UDS server. The legacy
// dashboard routes /api/cluster/* on the data-plane HTTP server are not
// touched by this client.
type Client struct {
	httpClient *http.Client
	baseURL    string
	sockPath   string // populated for UDS transport; "" for HTTP transport
}

// NewClient constructs a Client honoring the endpoint scheme. See Client doc.
func NewClient(endpoint string) *Client {
	ep := strings.TrimSpace(endpoint)
	if strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") {
		return &Client{
			httpClient: &http.Client{},
			baseURL:    strings.TrimRight(ep, "/"),
		}
	}
	sock := strings.TrimPrefix(ep, "unix:")
	return &Client{
		httpClient: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", sock)
				},
			},
		},
		baseURL:  "http://unix",
		sockPath: sock,
	}
}

// Status mirrors the JSON shape returned by /v1/cluster/status. Fields
// not present (e.g. local mode) decode to zero values; callers should branch
// on Mode.
type Status struct {
	Mode              string                    `json:"mode"`
	NodeID            string                    `json:"node_id,omitempty"`
	State             string                    `json:"state,omitempty"`
	Term              uint64                    `json:"term,omitempty"`
	LeaderID          string                    `json:"leader_id,omitempty"`
	Peers             []string                  `json:"peers,omitempty"`
	DownNodes         []string                  `json:"down_nodes,omitempty"`
	PeerAddrs         map[string]string         `json:"peer_addrs,omitempty"`
	PeerStates        map[string]string         `json:"peer_states,omitempty"`
	PeerSnapshot      []cluster.PeerLivenessRow `json:"peer_snapshot,omitempty"`
	BucketAssignments map[string]string         `json:"bucket_assignments,omitempty"`
	ShardGroups       []ShardGroup              `json:"shard_groups,omitempty"`
}

type ShardGroup struct {
	ID      string   `json:"id"`
	PeerIDs []string `json:"peer_ids"`
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

// Status fetches /v1/cluster/status. ctx controls the deadline; pass a
// context.WithTimeout to bound the call.
func (c *Client) Status(ctx context.Context) (*Status, error) {
	body, err := c.getJSON(ctx, "/v1/cluster/status")
	if err != nil {
		return nil, err
	}
	var s Status
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("parse status: %w", err)
	}
	return &s, nil
}

// StatusRaw fetches /v1/cluster/status and returns the response body
// unchanged. Use this for `cluster status --format json` to preserve
// forward-compatibility: new server fields (not yet in the typed Status
// struct) round-trip without loss.
//
// For text output, prefer Status() which returns a typed struct.
func (c *Client) StatusRaw(ctx context.Context) ([]byte, error) {
	return c.getJSON(ctx, "/v1/cluster/status")
}

// RemovePeer issues POST /v1/cluster/remove-peer. On non-2xx responses the
// returned error is *RemovePeerError so callers can branch on status code
// and surface server-supplied context.
func (c *Client) RemovePeer(ctx context.Context, id string, force bool) error {
	body, _ := json.Marshal(map[string]any{"id": id, "force": force})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/cluster/remove-peer", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return wrapDialError(err, c.sockPath)
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

// EventLog fetches /v1/cluster/eventlog with the given since (lookback
// duration) and limit. The server endpoint is gated by UDS file mode.
func (c *Client) EventLog(ctx context.Context, since time.Duration, limit int) ([]Event, error) {
	url := fmt.Sprintf("%s/v1/cluster/eventlog?since=%d&limit=%d", c.baseURL, int64(since.Seconds()), limit)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, wrapDialError(err, c.sockPath)
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

// TransferLeaderResult mirrors the 200 response of /v1/cluster/transfer-leader.
type TransferLeaderResult struct {
	OldLeader  string `json:"old_leader"`
	Term       uint64 `json:"term"`
	TargetHint string `json:"target_hint,omitempty"`
}

// TransferLeader issues POST /v1/cluster/transfer-leader. On non-2xx the
// returned error is *TransferLeaderError so callers can branch on Retry.
func (c *Client) TransferLeader(ctx context.Context) (*TransferLeaderResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/cluster/transfer-leader", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, wrapDialError(err, c.sockPath)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		var out TransferLeaderResult
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, fmt.Errorf("parse transfer-leader: %w", err)
		}
		return &out, nil
	}
	parsed := map[string]any{}
	_ = json.Unmarshal(raw, &parsed)
	tle := &TransferLeaderError{StatusCode: resp.StatusCode}
	if msg, ok := parsed["error"].(string); ok && msg != "" {
		tle.Message = msg
	} else {
		tle.Message = fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	if leader, ok := parsed["leader_id"].(string); ok {
		tle.LeaderID = leader
	}
	if retry, ok := parsed["retry"].(bool); ok {
		tle.Retry = retry
	}
	return nil, tle
}

// Health mirrors GET /v1/cluster/health response. Server-side derivation
// of Issues so the dashboard and CLI render the same diagnostics.
type Health struct {
	Mode     string          `json:"mode"`
	Degraded bool            `json:"degraded"`
	LeaderID string          `json:"leader_id,omitempty"`
	Term     uint64          `json:"term,omitempty"`
	Quorum   QuorumInfo      `json:"quorum"`
	Peers    []PeerHealthRow `json:"peers,omitempty"`
	Issues   []string        `json:"issues,omitempty"`
}

type QuorumInfo struct {
	VotersTotal int  `json:"voters_total"`
	AliveCount  int  `json:"alive_count"`
	Required    int  `json:"required"`
	Healthy     bool `json:"healthy"`
}

type PeerHealthRow struct {
	PeerID   string `json:"peer_id"`
	State    string `json:"state"`
	RaftAddr string `json:"raft_addr,omitempty"`
}

// Health fetches GET /v1/cluster/health (typed parse).
func (c *Client) Health(ctx context.Context) (*Health, error) {
	body, err := c.getJSON(ctx, "/v1/cluster/health")
	if err != nil {
		return nil, err
	}
	var h Health
	if err := json.Unmarshal(body, &h); err != nil {
		return nil, fmt.Errorf("parse health: %w", err)
	}
	return &h, nil
}

// BalancerStatus mirrors the JSON shape produced by
// /v1/cluster/balancer/status (snake_case fields per balancer_api.go).
type BalancerStatus struct {
	Available    bool                 `json:"available"`
	Active       bool                 `json:"active"`
	ImbalancePct float64              `json:"imbalance_pct"`
	Nodes        []BalancerNodeStatus `json:"nodes"`
}

type BalancerNodeStatus struct {
	NodeID         string  `json:"node_id"`
	DiskUsedPct    float64 `json:"disk_used_pct"`
	DiskAvailBytes uint64  `json:"disk_avail_bytes"`
	RequestsPerSec float64 `json:"requests_per_sec"`
	JoinedAt       string  `json:"joined_at,omitempty"`
	UpdatedAt      string  `json:"updated_at,omitempty"`
}

// BalancerStatus fetches GET /v1/cluster/balancer/status (typed parse).
func (c *Client) BalancerStatus(ctx context.Context) (*BalancerStatus, error) {
	body, err := c.getJSON(ctx, "/v1/cluster/balancer/status")
	if err != nil {
		return nil, err
	}
	var b BalancerStatus
	if err := json.Unmarshal(body, &b); err != nil {
		return nil, fmt.Errorf("parse balancer status: %w", err)
	}
	return &b, nil
}

func (c *Client) getJSON(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, wrapDialError(err, c.sockPath)
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
