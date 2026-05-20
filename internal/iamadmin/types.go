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

// --- Policy ---

type PolicyPutOptions struct {
	BaseOptions
	Name     string
	FilePath string
}

type PolicyGetOptions struct {
	BaseOptions
	Name string
}

type PolicyListOptions struct {
	BaseOptions
}

type PolicyDeleteOptions struct {
	BaseOptions
	Name string
}

// PolicyAttachOptions holds options for attaching a policy to an SA or group.
// Exactly one of SAID or GroupID should be set.
type PolicyAttachOptions struct {
	BaseOptions
	PolicyName string
	SAID       string
	GroupID    string
	IKnow      bool // suppress Resource:* warning
}

type PolicyDetachOptions struct {
	BaseOptions
	PolicyName string
	SAID       string
	GroupID    string
}

// PolicyValidateOptions performs local validation only — no UDS dial.
type PolicyValidateOptions struct {
	BaseOptions
	FilePath string
}

type PolicySimulateOptions struct {
	BaseOptions
	SAID     string
	Action   string
	Resource string
}

// PolicySimulateRequest is the wire type sent to POST /v1/iam/policy/simulate.
type PolicySimulateRequest struct {
	SAID     string `json:"sa_id"`
	Action   string `json:"action"`
	Resource string `json:"resource"`
}

// PolicySimulateResponse is the wire type received from POST /v1/iam/policy/simulate.
type PolicySimulateResponse struct {
	Effect        string `json:"effect"`
	MatchedPolicy string `json:"matched_policy"`
	MatchedSID    string `json:"matched_sid"`
	Reason        string `json:"reason"`
}

// --- Group ---

type GroupCreateOptions struct {
	BaseOptions
	Name string
}

type GroupDeleteOptions struct {
	BaseOptions
	Name string
}

type GroupMemberAddOptions struct {
	BaseOptions
	GroupName string
	SAID      string
}

type GroupMemberRemoveOptions struct {
	BaseOptions
	GroupName string
	SAID      string
}

type GroupPolicyAttachOptions struct {
	BaseOptions
	GroupName  string
	PolicyName string
}

type GroupPolicyDetachOptions struct {
	BaseOptions
	GroupName  string
	PolicyName string
}

// --- Bucket (iam bucket subtree) ---

type BucketCreateOptions struct {
	BaseOptions
	Name         string
	AttachSA     string // optional; must be paired with AttachPolicy
	AttachPolicy string // optional; must be paired with AttachSA
}

type BucketDeleteOptions struct {
	BaseOptions
	Name  string
	Force bool
}

type BucketListOptions struct {
	BaseOptions
}

type BucketPolicyPutOptions struct {
	BaseOptions
	Bucket string
	Policy []byte // raw JSON; sent verbatim
}

type BucketPolicyDeleteOptions struct {
	BaseOptions
	Bucket string
}

// BucketListItem is one entry in the bucket list response.
type BucketListItem struct {
	Name        string `json:"name"`
	HasUpstream bool   `json:"has_upstream"`
}
