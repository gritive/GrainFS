// Package volumeadmin contains the volume CLI's business logic. cmd/grainfs
// is a thin cobra wrapper that builds Options from flags and calls Run*
// here; all HTTP, formatting, and DX1 error rendering lives in this package.
package volumeadmin

import (
	"io"
	"time"

	"github.com/gritive/GrainFS/internal/adminapi"
)

type VolumeInfo = adminapi.VolumeInfo
type ListVolumesResp = adminapi.ListVolumesResp
type CreateVolumeReq = adminapi.CreateVolumeReq
type VolumeStatResp = adminapi.StatResp
type DeleteResp = adminapi.DeleteResp
type ResizeReq = adminapi.ResizeReq
type ResizeResp = adminapi.ResizeResp
type RecalculateResp = adminapi.RecalculateResp
type CloneReq = adminapi.CloneReq
type SnapshotInfo = adminapi.SnapshotInfo
type SnapshotCreateResp = adminapi.CreateSnapshotResp
type WriteAtReq = adminapi.WriteAtVolumeReq
type WriteAtResp = adminapi.WriteAtVolumeResp
type ReadAtReq = adminapi.ReadAtVolumeReq
type ReadAtResp = adminapi.ReadAtVolumeResp
type ScrubTriggerReq = adminapi.ScrubVolumeReq
type ScrubTriggerResp = adminapi.ScrubVolumeResp
type ScrubJobInfo = adminapi.ScrubJobInfo
type ListScrubJobsResp = adminapi.ListScrubJobsResp

// BaseOptions carries the fields shared by every Run* function: where to
// connect, how to render output, and where to write.
type BaseOptions struct {
	Endpoint string
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
