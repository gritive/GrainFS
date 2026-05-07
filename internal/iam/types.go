package iam

import "time"

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
}

// Grant authorizes a ServiceAccount to perform Role-level operations on a Bucket.
type Grant struct {
	SAID      string
	Bucket    string // exact bucket name; WildcardBucket only for bootstrap default SA
	Role      Role
	CreatedAt time.Time
	CreatedBy string // sa_id of creator
}
