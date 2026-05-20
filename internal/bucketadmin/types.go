// Package bucketadmin is the admin-plane client for grainfs bucket operations
// (bucket lifecycle, upstream credentials, policy, versioning). Consumed by
// the CLI (cmd/grainfs) and tests; server side lives in internal/server.
// Mirrors the iamadmin/clusteradmin/nfsadmin template.
package bucketadmin

import (
	"io"
	"time"
)

// BaseOptions matches iamadmin/volumeadmin/clusteradmin/nfsadmin shape.
// RawBytes is unused for bucket but kept for parity.
type BaseOptions struct {
	Endpoint string
	JSONOut  bool
	RawBytes bool
	Timeout  time.Duration
	Stdout   io.Writer
	Stderr   io.Writer
}

// --- Bucket lifecycle ---

type CreateOptions struct {
	BaseOptions
	Name       string
	AttachSA   string // empty → no attach
	AttachRole string // a.k.a. "policy"; must be non-empty if AttachSA is non-empty
}

type ListOptions struct {
	BaseOptions
}

type InfoOptions struct {
	BaseOptions
	Name string
}

type DeleteOptions struct {
	BaseOptions
	Name      string
	Force     bool
	Recursive bool
}

type CreateResponse struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// ListItem mirrors adminapi.BucketInfo for the list view. Only Name and
// HasUpstream are actually rendered by `grainfs bucket list`; the other
// fields are accepted for forward-compat.
type ListItem struct {
	Name        string `json:"name"`
	HasUpstream bool   `json:"has_upstream"`
}

// InfoResponse mirrors adminapi.BucketInfo for the info view.
// ObjectCount is a pointer so the renderer can distinguish "0 objects"
// from "unknown" (server omits the field when it cannot count).
type InfoResponse struct {
	Name        string `json:"name"`
	ObjectCount *int64 `json:"object_count,omitempty"`
	HasUpstream bool   `json:"has_upstream"`
	Versioning  string `json:"versioning,omitempty"`
}

// --- Upstream credentials ---

type UpstreamPutOptions struct {
	BaseOptions
	Bucket       string
	Scheme       string // s3 | r2
	Endpoint     string
	AccessKey    string
	SecretKey    string
	Region       string
	RemoteBucket string
}

type UpstreamGetOptions struct {
	BaseOptions
	Bucket string
}

type UpstreamListOptions struct {
	BaseOptions
}

type UpstreamDeleteOptions struct {
	BaseOptions
	Bucket string
}

// --- Bucket policy ---

type PolicyGetOptions struct {
	BaseOptions
	Bucket string
}

type PolicySetOptions struct {
	BaseOptions
	Bucket string
	Policy []byte // raw JSON document, sent verbatim
}

type PolicyDeleteOptions struct {
	BaseOptions
	Bucket string
}

// --- Versioning ---

type VersioningGetOptions struct {
	BaseOptions
	Bucket string
}

type VersioningEnableOptions struct {
	BaseOptions
	Bucket string
}

type VersioningSuspendOptions struct {
	BaseOptions
	Bucket string
}

type VersioningStatus struct {
	Status string `json:"status"` // "Enabled" | "Suspended" | ""
}
