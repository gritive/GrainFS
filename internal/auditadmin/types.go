// Package auditadmin is the admin-plane client for grainfs audit query operations.
// Consumed by the CLI (cmd/grainfs) and tests; the server side lives in
// internal/server/admin.
package auditadmin

import (
	"io"
	"time"
)

// BaseOptions mirrors iamadmin.BaseOptions for shape parity across admin packages.
type BaseOptions struct {
	Endpoint string
	JSONOut  bool
	Timeout  time.Duration
	Stdout   io.Writer
	Stderr   io.Writer
}

// QueryOptions holds options for the raw-SQL audit query subcommand.
type QueryOptions struct {
	BaseOptions
	SQL   string
	Limit int
}

// RecentDeniesOptions holds options for the recent-denies subcommand.
type RecentDeniesOptions struct {
	BaseOptions
	Limit int
}

// BySAOptions holds options for the by-sa subcommand.
type BySAOptions struct {
	BaseOptions
	SAID  string
	Limit int
}

// ByRequestIDOptions holds options for the by-request-id subcommand.
type ByRequestIDOptions struct {
	BaseOptions
	RequestID string
}

// QueryRequest is the wire type sent to POST /v1/audit/query.
type QueryRequest struct {
	SQL   string `json:"sql"`
	Limit int    `json:"limit,omitempty"`
}

// QueryResponse is the wire type returned by POST /v1/audit/query.
type QueryResponse struct {
	Columns []string   `json:"columns"`
	Rows    [][]string `json:"rows"`
}
