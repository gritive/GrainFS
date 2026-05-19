package iam

import (
	"context"
	"time"
)

// Proposer abstracts the meta-FSM Propose interface so admin API
// handlers can be unit-tested without raft. Each method must propose
// the corresponding MetaCmd payload through Raft and return only after
// the command has been committed (not just enqueued).
type Proposer interface {
	ProposeSACreate(ctx context.Context, sa ServiceAccount) error
	ProposeSADelete(ctx context.Context, saID string) error
	ProposeKeyCreate(ctx context.Context, k AccessKey) error
	ProposeKeyCreateScoped(ctx context.Context, k AccessKey) error
	ProposeKeyRevoke(ctx context.Context, accessKey string) error
	ProposeBucketUpstreamPut(ctx context.Context, u BucketUpstream) error
	ProposeBucketUpstreamDelete(ctx context.Context, bucket string) error
	ProposeBucketUpstreamCutover(ctx context.Context, bucket string) error
}

// KeyStatus represents whether an AccessKey is usable for SigV4 verification.
type KeyStatus uint8

// KeyStatus values. The zero value is intentionally not used so a
// missing/uninitialized status fails closed (LookupKey rejects).
const (
	KeyStatusActive  KeyStatus = 1
	KeyStatusRevoked KeyStatus = 2
)

// ServiceAccount is the IAM principal. Created by admin; never represents a human directly.
type ServiceAccount struct {
	ID          string // UUID v7
	Name        string // unique within cluster, admin-assigned label
	Description string // free text
	CreatedAt   time.Time
	CreatedBy   string // sa_id of creator; empty for bootstrap default
}

// AccessKey is one credential pair belonging to a ServiceAccount.
// SecretKey holds the plaintext only in-memory after Decrypt; persisted form is SecretKeyEnc.
type AccessKey struct {
	AccessKey    string // public component, used as SigV4 access key id
	SecretKey    string // plaintext, in-memory only (zero if not decrypted)
	SecretKeyEnc []byte // AES-256-GCM ciphertext, persisted form
	SAID         string // owning ServiceAccount ID
	Status       KeyStatus
	CreatedAt    time.Time
	LastUsedAt   time.Time  // best-effort, updated on Verify hit
	ExpiresAt    *time.Time // nil = never expires
	BucketScope  []string   // nil/empty = unrestricted (backward compat); non-empty = subset filter on Layer 0 authz
}

type BucketUpstreamStatus string

const (
	BucketUpstreamStatusActive  BucketUpstreamStatus = "active"
	BucketUpstreamStatusCutover BucketUpstreamStatus = "cutover"
)

// BucketUpstream stores per-bucket pull-through upstream credentials. Created
// and rotated through the admin UDS API; persisted via meta-FSM so all nodes
// see the same record. SecretKey holds plaintext only in-memory after Apply;
// SecretKeyEnc is the AES-256-GCM ciphertext (AAD = "bucket-upstream:"+bucket)
// used for snapshot + raft log persistence.
type BucketUpstream struct {
	Bucket       string
	Endpoint     string // e.g., http://minio:9000
	AccessKey    string
	SecretKey    string // in-memory plaintext; zero before Apply or after snapshot read with no encryptor
	SecretKeyEnc []byte // AES-256-GCM ciphertext, AAD = "bucket-upstream:"+bucket
	CreatedAt    time.Time
	CreatedBy    string // sa_id of admin that issued the put
	Status       BucketUpstreamStatus
}
