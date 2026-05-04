// Package admin provides transport-agnostic handlers for GrainFS administrative
// operations (volume lifecycle, dashboard token issuance). The same handler
// functions are wired into both the Unix-socket admin server (used by the
// `grainfs` CLI) and the data-plane `/ui/api/*` routes (used by the web UI).
package admin

import (
	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/volume"
)

// Deps bundles the shared dependencies required by every admin handler.
// Caller is responsible for constructing this struct at process startup.
type Deps struct {
	Manager   *volume.Manager
	Incident  incident.StateStore // List(ctx, limit) — optional, nil OK
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
