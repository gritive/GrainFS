package nfsadmin

import (
	"io"
	"time"

	"github.com/gritive/GrainFS/internal/adminapi"
)

type NfsExportInfo = adminapi.NfsExportInfo
type NfsExportUpsertReq = adminapi.NfsExportUpsertReq
type ListNfsExportsResp = adminapi.ListNfsExportsResp

type BaseOptions struct {
	Endpoint string
	Timeout  time.Duration
	JSONOut  bool
	Quiet    bool
	Stdout   io.Writer
	Stderr   io.Writer
}

type AddExportOptions struct {
	BaseOptions
	Bucket   string
	ReadOnly bool
	DryRun   bool
}

type UpdateExportOptions struct {
	BaseOptions
	Bucket   string
	ReadOnly bool
	DryRun   bool
}

type RemoveExportOptions struct {
	BaseOptions
	Bucket string
	DryRun bool
}

type ListExportOptions struct {
	BaseOptions
}
