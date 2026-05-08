package iam

import (
	"context"
	"time"

	"github.com/gritive/GrainFS/internal/s3auth"
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
	ProposeGrantPut(ctx context.Context, g Grant) error
	ProposeGrantDelete(ctx context.Context, saID, bucket string) error
	ProposeGrantWildcardPut(ctx context.Context, g Grant) error
	ProposeGrantWildcardDelete(ctx context.Context, saID string) error
	ProposeInitFirstSA(ctx context.Context, sa ServiceAccount, k AccessKey, g Grant) error
	ProposeBucketUpstreamPut(ctx context.Context, u BucketUpstream) error
	ProposeBucketUpstreamDelete(ctx context.Context, bucket string) error
}

// Role is the 3-tier permission level for a (SA, Bucket) grant.
type Role uint8

// Role values, ordered by increasing privilege. RoleNone (zero value) is
// "no permission" and is the default for any unset map lookup.
const (
	RoleNone  Role = 0
	RoleRead  Role = 1
	RoleWrite Role = 2
	RoleAdmin Role = 3
)

func (r Role) String() string {
	switch r {
	case RoleRead:
		return "Read"
	case RoleWrite:
		return "Write"
	case RoleAdmin:
		return "Admin"
	default:
		return "None"
	}
}

// KeyStatus represents whether an AccessKey is usable for SigV4 verification.
type KeyStatus uint8

// KeyStatus values. The zero value is intentionally not used so a
// missing/uninitialized status fails closed (LookupKey rejects).
const (
	KeyStatusActive  KeyStatus = 1
	KeyStatusRevoked KeyStatus = 2
)

// WildcardBucket is the sentinel bucket name reserved for the bootstrap default SA.
// Regular SAs cannot receive a wildcard grant via the admin API.
const WildcardBucket = "*"

// SystemBucket is the sentinel for cluster-admin operations (e.g., admin API auth).
const SystemBucket = "__system__"

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

// Grant authorizes a ServiceAccount to perform Role-level operations on a Bucket.
type Grant struct {
	SAID      string
	Bucket    string // exact bucket name; WildcardBucket only for bootstrap default SA
	Role      Role
	CreatedAt time.Time
	CreatedBy string // sa_id of creator
}

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
}

// RoleAllows reports whether `role` permits `action` on the target bucket.
// Bucket-lifecycle operations (CreateBucket, DeleteBucket) and policy
// mutations (PutBucketPolicy, DeleteBucketPolicy) require Admin. Object
// writes (PutObject, DeleteObject, CopyObject) require Write or higher.
// Reads (GetObject, HeadObject, ListBucket, ListMultipartUploads,
// GetBucketPolicy) require Read or higher. UnknownAction always denies.
func RoleAllows(role Role, action s3auth.S3Action) bool {
	switch action {
	case s3auth.GetObject, s3auth.HeadObject, s3auth.ListBucket, s3auth.ListMultipartUploads,
		s3auth.GetBucketPolicy:
		return role >= RoleRead
	case s3auth.PutObject, s3auth.DeleteObject, s3auth.CopyObject:
		return role >= RoleWrite
	case s3auth.CreateBucket, s3auth.DeleteBucket,
		s3auth.PutBucketPolicy, s3auth.DeleteBucketPolicy:
		return role >= RoleAdmin
	case s3auth.UnknownAction:
		return false
	default:
		return false
	}
}
