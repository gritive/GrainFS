// Package iamadmin is the admin-plane client for grainfs IAM operations.
// Consumed by the CLI (cmd/grainfs) and tests; the server side lives in
// internal/server. Wire shapes and rendering helpers stay here so they
// are unit-testable in isolation and reusable from non-CLI callers.
package iamadmin

import (
	"io"
	"time"
)

// BaseOptions matches the shape used by volumeadmin/clusteradmin/nfsadmin
// so cmd helpers can populate it uniformly. RawBytes is unused for IAM but
// kept for shape parity.
type BaseOptions struct {
	Endpoint string
	JSONOut  bool
	RawBytes bool
	Timeout  time.Duration
	Stdout   io.Writer
	Stderr   io.Writer
}

// --- ServiceAccount ---

type SACreateOptions struct {
	BaseOptions
	Name        string
	Description string
}

type SAListOptions struct {
	BaseOptions
}

type SAGetOptions struct {
	BaseOptions
	SAID string
}

type SADeleteOptions struct {
	BaseOptions
	SAID string
}

type SACreateResponse struct {
	SAID      string `json:"sa_id"`
	Name      string `json:"name"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

type SAListItem struct {
	SAID        string    `json:"sa_id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
	NumKeys     int       `json:"num_keys"`
	NumGrants   int       `json:"num_grants"`
}

type SAGetResponse struct {
	SAID        string    `json:"sa_id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
	CreatedBy   string    `json:"created_by"`
}

// --- AccessKey ---

type KeyCreateOptions struct {
	BaseOptions
	SAID    string
	Buckets []string
}

type KeyRevokeOptions struct {
	BaseOptions
	SAID      string
	AccessKey string
}
