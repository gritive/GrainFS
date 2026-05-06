// Package volumeadmin contains the volume CLI's business logic. cmd/grainfs
// is a thin cobra wrapper that builds Options from flags and calls Run*
// here; all HTTP, formatting, and DX1 error rendering lives in this package.
package volumeadmin

import (
	"io"
	"time"
)

// VolumeInfo mirrors internal/server/admin.VolumeInfo. Kept locally so the
// CLI does not depend on the server admin package.
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

// ListVolumesResp is the response shape of GET /v1/volumes.
type ListVolumesResp struct {
	Volumes []VolumeInfo `json:"volumes"`
}

// CreateVolumeReq is the body for POST /v1/volumes.
type CreateVolumeReq struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// VolumeStatResp is the response of GET /v1/volumes/<name>/stat.
type VolumeStatResp struct {
	Volume          VolumeInfo       `json:"volume"`
	RecentIncidents []map[string]any `json:"recent_incidents"`
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

// SnapshotCreateResp is the response of POST /v1/volumes/<name>/snapshots.
type SnapshotCreateResp struct {
	ID         string `json:"id"`
	BlockCount int64  `json:"block_count"`
}

// WriteAtReq is the body for POST /v1/volumes/<name>/write-at.
type WriteAtReq struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
	Data   []byte `json:"data"`
}

// WriteAtResp reports how many bytes were written.
type WriteAtResp struct {
	Bytes int64 `json:"bytes"`
}

// ReadAtReq is the body for POST /v1/volumes/<name>/read-at.
type ReadAtReq struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}

// ReadAtResp carries the bytes read.
type ReadAtResp struct {
	Data []byte `json:"data"`
}

// ScrubTriggerReq triggers a scrub session.
type ScrubTriggerReq struct {
	Name   string `json:"name"`
	Scope  string `json:"scope,omitempty"`
	DryRun bool   `json:"dry_run,omitempty"`
}

// ScrubTriggerResp identifies the resulting session.
type ScrubTriggerResp struct {
	SessionID string `json:"session_id"`
	Created   bool   `json:"created"`
}

// ScrubJobInfo mirrors server admin.ScrubJobInfo including the partial-peer-
// failure fields (OwnedHere, Partial, PeerFailures) that the previous cmd
// implementation silently dropped.
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

// BaseOptions carries the fields shared by every Run* function: where to
// connect, how to render output, and where to write.
type BaseOptions struct {
	Endpoint string
	DataDir  string
	Timeout  time.Duration

	JSONOut  bool
	RawBytes bool

	Stdout io.Writer
	Stderr io.Writer
}

// ListOptions configures RunList.
type ListOptions struct{ BaseOptions }

// CreateOptions configures RunCreate.
type CreateOptions struct {
	BaseOptions
	Name string
	Size int64
}

// InfoOptions configures RunInfo.
type InfoOptions struct {
	BaseOptions
	Name string
}

// StatOptions configures RunStat.
type StatOptions struct {
	BaseOptions
	Name string
}

// DeleteOptions configures RunDelete.
type DeleteOptions struct {
	BaseOptions
	Name  string
	Force bool
}

// ResizeOptions configures RunResize.
type ResizeOptions struct {
	BaseOptions
	Name string
	Size int64
}

// RecalculateOptions configures RunRecalculate.
type RecalculateOptions struct {
	BaseOptions
	Name string
}

// CloneOptions configures RunClone.
type CloneOptions struct {
	BaseOptions
	Src string
	Dst string
}

// RollbackOptions configures RunRollback.
type RollbackOptions struct {
	BaseOptions
	Name       string
	SnapshotID string
}

// WriteAtOptions configures RunWriteAt.
type WriteAtOptions struct {
	BaseOptions
	Name    string
	Offset  int64
	Content []byte
}

// ReadAtOptions configures RunReadAt.
type ReadAtOptions struct {
	BaseOptions
	Name   string
	Offset int64
	Length int64
}

// SnapshotCreateOptions configures RunSnapshotCreate.
type SnapshotCreateOptions struct {
	BaseOptions
	Volume string
}

// SnapshotListOptions configures RunSnapshotList.
type SnapshotListOptions struct {
	BaseOptions
	Volume string
}

// SnapshotDeleteOptions configures RunSnapshotDelete.
type SnapshotDeleteOptions struct {
	BaseOptions
	Volume     string
	SnapshotID string
}

// ScrubOptions configures RunScrub.
type ScrubOptions struct {
	BaseOptions
	Name   string
	Scope  string
	DryRun bool
	Detach bool

	// PollInterval overrides the follow-loop tick (default 1s). Tests set
	// this to a few ms to avoid sleeping a real second.
	PollInterval time.Duration
}

// ScrubStatusOptions configures RunScrubStatus.
type ScrubStatusOptions struct {
	BaseOptions
	SessionID string
}

// ScrubListOptions configures RunScrubList.
type ScrubListOptions struct{ BaseOptions }

// ScrubCancelOptions configures RunScrubCancel.
type ScrubCancelOptions struct {
	BaseOptions
	SessionID string
}
