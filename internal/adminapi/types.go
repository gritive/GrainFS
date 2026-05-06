// Package adminapi owns the JSON wire schema for the GrainFS admin HTTP API.
package adminapi

// Error is the JSON envelope returned by admin HTTP handlers on failures.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"error"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string { return e.Message }

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
