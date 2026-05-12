// Package adminapi owns the JSON wire schema for the GrainFS admin HTTP API.
package adminapi

import (
	"encoding/json"
	"errors"
)

// Error is the JSON envelope returned by admin HTTP handlers on failures.
// Generic transport-level error envelope; per-endpoint typed wrappers live in
// the owning admin client package (e.g. clusteradmin.RemovePeerError).
//
// Details holds the raw JSON of the structured "details" object (or, for
// legacy flat-shape responses, the raw body bytes). Callers decode into an
// endpoint-specific typed struct via json.Unmarshal so transport stays
// schema-agnostic while per-endpoint code remains strongly typed.
type Error struct {
	Code    string          `json:"code"`
	Message string          `json:"error"`
	Details json.RawMessage `json:"details,omitempty"`

	// Status mirrors the HTTP status code. Not serialized.
	Status int `json:"-"`

	// cause carries the wrapped transport-level error so errors.Is/As can
	// still see through the typed envelope.
	cause error `json:"-"`
}

// Error implements error. Returns Message if set, otherwise Code.
func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

// Unwrap exposes the wrapped underlying error.
func (e *Error) Unwrap() error { return e.cause }

// WithCause returns a copy of e with cause set.
func (e *Error) WithCause(cause error) *Error {
	copy := *e
	copy.cause = cause
	return &copy
}

// IsCode reports whether err is a *Error with the given code.
func IsCode(err error, code string) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	return e.Code == code
}

// ClusterPeerInfo is the JSON form of one peer's health state.
type ClusterPeerInfo struct {
	ID                  string `json:"id"`
	Healthy             bool   `json:"healthy"`
	LastFailure         string `json:"last_failure,omitempty"`
	CooldownRemainingMs int64  `json:"cooldown_remaining_ms"`
}

// ListClusterPeersResp aggregates peer health for GET /v1/cluster/peers.
type ListClusterPeersResp struct {
	Peers []ClusterPeerInfo `json:"peers"`
}

// ScrubReq is the JSON body for POST /v1/scrub.
type ScrubReq struct {
	Bucket    string `json:"bucket"`
	KeyPrefix string `json:"key_prefix,omitempty"`
	Scope     string `json:"scope,omitempty"`
	DryRun    bool   `json:"dry_run,omitempty"`
}

// ScrubResp identifies the resulting cluster-wide session.
type ScrubResp struct {
	SessionID string `json:"session_id"`
	Created   bool   `json:"created"`
}

// VlogBreakdownResp is the JSON body of GET /v1/resource/vlog/breakdown.
type VlogBreakdownResp struct {
	TotalVlogBytes int64               `json:"total_vlog_bytes"`
	LimitBytes     int64               `json:"limit_bytes"`
	Ratio          float64             `json:"ratio"`
	Level          string              `json:"level"`
	Categories     []VlogCategoryBytes `json:"categories"`
	GCFailures     map[string]int32    `json:"consecutive_gc_failures_per_category"`
	SmokeReport    VlogSmokeReport     `json:"smoke_report"`
}

// VlogCategoryBytes is one line item of the breakdown.
type VlogCategoryBytes struct {
	Category  string `json:"category"`
	VlogBytes int64  `json:"vlog_bytes"`
}

// VlogSmokeReport mirrors the resource watcher smoke report on the wire.
type VlogSmokeReport struct {
	Live  []string `json:"live"`
	Stale []string `json:"stale"`
}

// DashboardTokenResp describes the dashboard URL and token shown to operators.
type DashboardTokenResp struct {
	URL          string `json:"url"`
	Host         string `json:"host"`
	Token        string `json:"token"`
	Path         string `json:"path"`
	PublicURLSet bool   `json:"public_url_set"`
}

// WriteAtVolumeReq is the JSON body for WriteAtVolume.
type WriteAtVolumeReq struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
	Data   []byte `json:"data"`
}

// WriteAtVolumeResp reports how many bytes were written.
type WriteAtVolumeResp struct {
	Bytes int64 `json:"bytes"`
}

// ReadAtVolumeReq is the JSON body for ReadAtVolume.
type ReadAtVolumeReq struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}

// ReadAtVolumeResp carries the read bytes.
type ReadAtVolumeResp struct {
	Data []byte `json:"data"`
}

// ScrubVolumeReq triggers a scrub session over a single volume's blocks.
type ScrubVolumeReq struct {
	Name   string `json:"name"`
	Scope  string `json:"scope,omitempty"`
	DryRun bool   `json:"dry_run,omitempty"`
}

// ScrubVolumeResp identifies the resulting session.
type ScrubVolumeResp struct {
	SessionID string `json:"session_id"`
	Created   bool   `json:"created"`
}

// ScrubJobInfo is the JSON form of one scrub session.
type ScrubJobInfo struct {
	SessionID    string   `json:"session_id"`
	Bucket       string   `json:"bucket"`
	KeyPrefix    string   `json:"key_prefix"`
	Scope        string   `json:"scope"`
	DryRun       bool     `json:"dry_run"`
	Status       string   `json:"status"`
	StartedAt    int64    `json:"started_at"`
	DoneAt       int64    `json:"done_at,omitempty"`
	Checked      int64    `json:"checked"`
	Healthy      int64    `json:"healthy"`
	Detected     int64    `json:"detected"`
	Repaired     int64    `json:"repaired"`
	Unrepairable int64    `json:"unrepairable"`
	Skipped      int64    `json:"skipped"`
	OwnedHere    bool     `json:"owned_here"`
	Partial      bool     `json:"partial,omitempty"`
	PeerFailures []string `json:"peer_failures,omitempty"`
}

// ListScrubJobsResp aggregates active sessions.
type ListScrubJobsResp struct {
	Jobs []ScrubJobInfo `json:"jobs"`
}

// VolumeInfo is the JSON representation of a volume in admin responses.
type VolumeInfo struct {
	Name            string   `json:"name"`
	Size            int64    `json:"size"`
	BlockSize       int      `json:"block_size"`
	AllocatedBlocks int64    `json:"allocated_blocks"`
	AllocatedBytes  int64    `json:"allocated_bytes"`
	SnapshotCount   int32    `json:"snapshot_count"`
	Health          string   `json:"health"`
	HealthReasons   []string `json:"health_reasons"`
}

// ListVolumesResp is returned by GET /v1/volumes.
type ListVolumesResp struct {
	Volumes []VolumeInfo `json:"volumes"`
}

// CreateVolumeReq is the body for POST /v1/volumes.
type CreateVolumeReq struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// StatResp is returned by GET /v1/volumes/<name>/stat.
type StatResp struct {
	Volume          VolumeInfo       `json:"volume"`
	RecentIncidents []map[string]any `json:"recent_incidents,omitempty"`
}

// DeleteResp is the response of DELETE /v1/volumes/<name>.
type DeleteResp struct {
	Deleted bool `json:"deleted"`
}

// ResizeReq is the body for POST /v1/volumes/<name>/resize.
type ResizeReq struct {
	Size int64 `json:"size"`
}

// ResizeResp is the response of resize.
type ResizeResp struct {
	Name    string `json:"name"`
	OldSize int64  `json:"old_size"`
	NewSize int64  `json:"new_size"`
	Changed bool   `json:"changed"`
}

// RecalculateResp is the response of POST /v1/volumes/<name>/recalculate.
type RecalculateResp struct {
	Volume string `json:"volume"`
	Before int64  `json:"before"`
	After  int64  `json:"after"`
	Fixed  bool   `json:"fixed"`
}

// CloneReq is the body for POST /v1/volumes/clone.
type CloneReq struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
}

// SnapshotInfo is one entry in the snapshot list response.
type SnapshotInfo struct {
	ID         string `json:"id"`
	CreatedAt  string `json:"created_at"`
	BlockCount int64  `json:"block_count"`
}

// CreateSnapshotResp is the response of POST /v1/volumes/<name>/snapshots.
type CreateSnapshotResp struct {
	ID         string `json:"id"`
	BlockCount int64  `json:"block_count"`
}

// --- Cluster wire types ---

// Status mirrors the JSON body of GET /v1/cluster/status. The server emits
// this shape from a map[string]any handler; clients decode through this view.
type Status struct {
	Mode              string            `json:"mode"`
	NodeID            string            `json:"node_id,omitempty"`
	State             string            `json:"state,omitempty"`
	Term              uint64            `json:"term,omitempty"`
	LeaderID          string            `json:"leader_id,omitempty"`
	Peers             []string          `json:"peers,omitempty"`
	DownNodes         []string          `json:"down_nodes,omitempty"`
	PeerAddrs         map[string]string `json:"peer_addrs,omitempty"`
	PeerStates        map[string]string `json:"peer_states,omitempty"`
	PeerSnapshot      []PeerLivenessRow `json:"peer_snapshot,omitempty"`
	BucketAssignments map[string]string `json:"bucket_assignments,omitempty"`
	ShardGroups       []ShardGroup      `json:"shard_groups,omitempty"`
}

// PeerLivenessRow is one row of the cluster peer liveness snapshot. Mirrors
// cluster.PeerLivenessRow but with plain string enum fields (cluster.* uses
// typed enums like PeerIdentityState/PeerLivenessState). The wire form uses
// plain strings so the schema is closed.
type PeerLivenessRow struct {
	PeerID        string `json:"peer_id"`
	RaftAddr      string `json:"raft_addr,omitempty"`
	IdentityState string `json:"identity_state"`
	LivenessState string `json:"liveness_state"`
	Reason        string `json:"reason,omitempty"`
}

// ShardGroup describes a shard placement group on the wire.
type ShardGroup struct {
	ID      string   `json:"id"`
	PeerIDs []string `json:"peer_ids"`
}

// Health mirrors GET /v1/cluster/health.
type Health struct {
	Mode     string          `json:"mode"`
	Degraded bool            `json:"degraded"`
	LeaderID string          `json:"leader_id,omitempty"`
	Term     uint64          `json:"term,omitempty"`
	Quorum   QuorumInfo      `json:"quorum"`
	Peers    []PeerHealthRow `json:"peers,omitempty"`
	Issues   []string        `json:"issues,omitempty"`
}

// QuorumInfo summarises voter health for Health.Quorum.
type QuorumInfo struct {
	VotersTotal int  `json:"voters_total"`
	AliveCount  int  `json:"alive_count"`
	Required    int  `json:"required"`
	Healthy     bool `json:"healthy"`
}

// PeerHealthRow is one row of Health.Peers.
type PeerHealthRow struct {
	PeerID   string `json:"peer_id"`
	State    string `json:"state"`
	RaftAddr string `json:"raft_addr,omitempty"`
}

// PlacementReport mirrors GET /v1/cluster/placement.
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

// PlacementReportEntry is one row of PlacementReport.Details.
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

// BalancerStatus mirrors GET /v1/cluster/balancer/status.
type BalancerStatus struct {
	Available    bool                 `json:"available"`
	Active       bool                 `json:"active"`
	ImbalancePct float64              `json:"imbalance_pct"`
	Nodes        []BalancerNodeStatus `json:"nodes"`
}

// BalancerNodeStatus is one row of BalancerStatus.Nodes. JoinedAt/UpdatedAt
// are RFC3339 strings emitted by the server boundary; empty means unknown.
type BalancerNodeStatus struct {
	NodeID         string  `json:"node_id"`
	DiskUsedPct    float64 `json:"disk_used_pct"`
	DiskAvailBytes uint64  `json:"disk_avail_bytes"`
	RequestsPerSec float64 `json:"requests_per_sec"`
	JoinedAt       string  `json:"joined_at,omitempty"`
	UpdatedAt      string  `json:"updated_at,omitempty"`
}

// Event mirrors one entry of GET /v1/cluster/eventlog. Mirrors eventstore.Event
// without importing the server package, which would cycle internal/server ->
// internal/clusteradmin -> cmd back into internal/server through wiring.
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

// TransferLeaderResult mirrors the 200 response of POST /v1/cluster/transfer-leader.
type TransferLeaderResult struct {
	OldLeader  string `json:"old_leader"`
	Term       uint64 `json:"term"`
	TargetHint string `json:"target_hint,omitempty"`
}
