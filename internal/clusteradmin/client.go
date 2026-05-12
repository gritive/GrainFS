// Package clusteradmin is the admin-plane HTTP client for grainfs cluster
// operations. It is consumed by the CLI (cmd/grainfs) and tests; the server
// side lives in internal/server. Keeping this in internal/ rather than cmd/
// means the wire shapes, pre-flight rules, and rendering helpers are unit-
// testable in isolation and stay reusable from non-CLI callers.
package clusteradmin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/cluster"
)

// Client speaks to a single grainfs server's admin endpoints. Transport
// plumbing (UDS/HTTP dispatch, JSON marshal, error envelope) lives in
// adminapi; this type only wires endpoint methods.
type Client struct {
	*adminapi.Transport
}

// NewClient honors the endpoint scheme. The signature returns *Client without
// error to match many existing CLI call sites; an unconstructable Transport
// (impossible after the F1 resolution — empty endpoint is now accepted and
// fails at request time) would surface at the first method call.
func NewClient(endpoint string) *Client {
	tp, _ := adminapi.NewTransport(endpoint)
	return &Client{Transport: tp}
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

type PlacementOptions struct {
	Bucket string
	Key    string
	Limit  int
}

type PlacementReport struct {
	DesiredPolicyBasis    string                 `json:"desired_policy_basis"`
	Bucket                string                 `json:"bucket,omitempty"`
	Key                   string                 `json:"key,omitempty"`
	ObjectCount           int                    `json:"object_count"`
	Bytes                 int64                  `json:"bytes"`
	ActualProfileCounts   map[string]int         `json:"actual_profile_counts"`
	PendingUpgradeCount   int                    `json:"pending_upgrade_count"`
	DowngradeSkippedCount int                    `json:"downgrade_skipped_count"`
	UnknownLayoutCount    int                    `json:"unknown_layout_count"`
	RepairNeededCount     int                    `json:"repair_needed_count"`
	Details               []PlacementReportEntry `json:"details,omitempty"`
}

type PlacementReportEntry struct {
	Bucket           string   `json:"bucket"`
	Key              string   `json:"key"`
	VersionID        string   `json:"version_id"`
	PlacementGroupID string   `json:"placement_group_id"`
	ActualECData     uint8    `json:"actual_ec_data"`
	ActualECParity   uint8    `json:"actual_ec_parity"`
	DesiredECData    int      `json:"desired_ec_data"`
	DesiredECParity  int      `json:"desired_ec_parity"`
	LayoutState      string   `json:"layout_state"`
	NodeIDs          []string `json:"node_ids,omitempty"`
	Size             int64    `json:"size"`
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

// Status fetches /v1/cluster/status. ctx controls the deadline; pass a
// context.WithTimeout to bound the call.
func (c *Client) Status(ctx context.Context) (*Status, error) {
	body, err := c.GetRaw(ctx, "/v1/cluster/status")
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
	return c.GetRaw(ctx, "/v1/cluster/status")
}

func (c *Client) Placement(ctx context.Context, opts PlacementOptions) (*PlacementReport, error) {
	q := url.Values{}
	if opts.Bucket != "" {
		q.Set("bucket", opts.Bucket)
	}
	if opts.Key != "" {
		q.Set("key", opts.Key)
	}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	path := "/v1/cluster/placement"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var report PlacementReport
	if err := c.Get(ctx, path, &report); err != nil {
		return nil, err
	}
	return &report, nil
}

// RemovePeer issues POST /v1/cluster/remove-peer. On non-2xx responses the
// returned error is *RemovePeerError so callers can branch on status code
// and surface server-supplied context.
func (c *Client) RemovePeer(ctx context.Context, id string, force bool) error {
	body := map[string]any{"id": id, "force": force}
	if err := c.Post(ctx, "/v1/cluster/remove-peer", body, nil); err != nil {
		if ae, ok := asAdminError(err); ok {
			return parseRemovePeerError(ae)
		}
		return err
	}
	return nil
}

// EventLog fetches /v1/cluster/eventlog with the given since (lookback
// duration) and limit. The server endpoint is gated by UDS file mode.
func (c *Client) EventLog(ctx context.Context, since time.Duration, limit int) ([]Event, error) {
	path := fmt.Sprintf("/v1/cluster/eventlog?since=%d&limit=%d", int64(since.Seconds()), limit)
	var out []Event
	if err := c.Get(ctx, path, &out); err != nil {
		return nil, err
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
	var out TransferLeaderResult
	if err := c.Post(ctx, "/v1/cluster/transfer-leader", struct{}{}, &out); err != nil {
		if ae, ok := asAdminError(err); ok {
			return nil, parseTransferLeaderError(ae)
		}
		return nil, err
	}
	return &out, nil
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
	var h Health
	if err := c.Get(ctx, "/v1/cluster/health", &h); err != nil {
		return nil, err
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
	var b BalancerStatus
	if err := c.Get(ctx, "/v1/cluster/balancer/status", &b); err != nil {
		return nil, err
	}
	return &b, nil
}
