// Package admin provides transport-agnostic handlers for GrainFS administrative
// operations (volume lifecycle, dashboard token issuance). The same handler
// functions are wired into both the Unix-socket admin server (used by the
// `grainfs` CLI) and the data-plane `/ui/api/*` routes (used by the web UI).
package admin

import (
	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/volume"
)

// DirectorAPI is the slim interface admin handlers need from the scrub
// director. Implemented by *scrubber.Director; defined here so handler
// tests can substitute a mock.
type DirectorAPI interface {
	Trigger(req scrubber.TriggerReq) (string, bool)
	Sessions() []scrubber.Session
	GetSession(id string) (scrubber.Session, bool)
	CancelSession(id string) error
	ApplyFromFSM(entry scrubber.ScrubTriggerEntry)
}

// Deps bundles the shared dependencies required by every admin handler.
// Caller is responsible for constructing this struct at process startup.
type Deps struct {
	Manager   *volume.Manager
	Incident  incident.StateStore // List(ctx, limit) — optional, nil OK
	Director  DirectorAPI         // optional; nil disables scrub admin endpoints
	Token     *dashboard.TokenStore
	PublicURL string // e.g. "https://node1:9000"; empty means use localhost fallback
	NodeID    string
}

// Error is the domain error type returned by admin handlers. The HTTP adapter
// maps Code to status code and serializes Message + Details into the response
// body. Code values are: "not_found" / "conflict" / "invalid" / "unsupported"
// / "unauthorized" / "internal".
type Error struct {
	Code    string `json:"code"`
	Message string `json:"error"`
	Details any    `json:"details,omitempty"`
}

func (e *Error) Error() string { return e.Message }

func NewNotFound(msg string) *Error { return &Error{Code: "not_found", Message: msg} }
func NewInvalid(msg string) *Error  { return &Error{Code: "invalid", Message: msg} }
func NewInternal(msg string) *Error { return &Error{Code: "internal", Message: msg} }
func NewConflict(msg string, details any) *Error {
	return &Error{Code: "conflict", Message: msg, Details: details}
}
func NewUnsupported(msg string, details any) *Error {
	return &Error{Code: "unsupported", Message: msg, Details: details}
}

// WriteAtVolumeReq is the JSON body for WriteAtVolume.
type WriteAtVolumeReq struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
	Data   []byte `json:"data"` // base64-encoded in JSON
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
	Scope  string `json:"scope,omitempty"`   // "full" (default) | "live"
	DryRun bool   `json:"dry_run,omitempty"` // observe-only: detect, no repair
}

// ScrubVolumeResp identifies the resulting session.
type ScrubVolumeResp struct {
	SessionID string `json:"session_id"`
	Created   bool   `json:"created"` // false = duplicate request, returned existing session
}

// ScrubJobInfo is the JSON form of one Director session.
type ScrubJobInfo struct {
	SessionID    string `json:"session_id"`
	Bucket       string `json:"bucket"`
	KeyPrefix    string `json:"key_prefix"`
	Scope        string `json:"scope"`
	DryRun       bool   `json:"dry_run"`
	Status       string `json:"status"` // running | done | cancelled
	StartedAt    int64  `json:"started_at"`
	DoneAt       int64  `json:"done_at,omitempty"`
	Checked      int64  `json:"checked"`
	Healthy      int64  `json:"healthy"`
	Detected     int64  `json:"detected"`
	Repaired     int64  `json:"repaired"`
	Unrepairable int64  `json:"unrepairable"`
	Skipped      int64  `json:"skipped"`
}

// ListScrubJobsResp aggregates the active session list.
type ListScrubJobsResp struct {
	Jobs []ScrubJobInfo `json:"jobs"`
}

// VolumeInfo is the JSON representation of a volume in admin responses.
type VolumeInfo struct {
	Name            string `json:"name"`
	Size            int64  `json:"size"`
	BlockSize       int    `json:"block_size"`
	AllocatedBlocks int64  `json:"allocated_blocks"`
	AllocatedBytes  int64  `json:"allocated_bytes"`
	SnapshotCount   int32  `json:"snapshot_count"`
}

func toVolumeInfo(v *volume.Volume) VolumeInfo {
	return VolumeInfo{
		Name:            v.Name,
		Size:            v.Size,
		BlockSize:       v.BlockSize,
		AllocatedBlocks: v.AllocatedBlocks,
		AllocatedBytes:  v.AllocatedBytes(),
		SnapshotCount:   v.SnapshotCount,
	}
}
