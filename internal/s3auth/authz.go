package s3auth

// ACLGrant is a bitmask for S3 canned ACL values.
// Wire format: stored as uint32 in Protobuf field 12 of ECObjectMeta.
type ACLGrant uint8

const (
	ACLPrivate         ACLGrant = 0
	ACLPublicRead      ACLGrant = 1 << 0
	ACLPublicReadWrite ACLGrant = 1 << 1
	// ACLAuthRead is intentionally omitted — requires cross-account support.
	// Reserve ACLGrant(4) for future use when multi-tenancy is added (Phase 14+).
)

// S3Action is an enum of S3 API actions.
type S3Action uint16

const (
	UnknownAction S3Action = iota
	GetObject
	HeadObject
	ListBucket
	PutObject
	CreateBucket
	DeleteObject
	DeleteBucket
	CopyObject
	ListMultipartUploads
	// Phase 5d: policy CRUD becomes IAM-gated. Read maps to GetBucketPolicy
	// (Read+ on bucket); Put/Delete map to Admin-only operations. Append
	// only — never renumber existing values; the on-disk MetaCmd numbering
	// pins these enum values.
	GetBucketPolicy
	PutBucketPolicy
	DeleteBucketPolicy
	GetBucketVersioning
	PutBucketVersioning
	ListBucketVersions
	GetObjectRetention
	PutObjectRetention
	GetBucketObjectLockConfiguration
)

// Principal identifies the caller of an S3 request.
type Principal struct {
	AccessKey string
}

// ResourceRef identifies the target of an S3 request.
type ResourceRef struct {
	Bucket string
	Key    string // empty = bucket-level
}

// PermCheckInput is the unified input for permission checks.
type PermCheckInput struct {
	Principal Principal
	Resource  ResourceRef
	Action    S3Action
	ObjectACL ACLGrant // 0 if metadata not yet loaded
}

// ParseACLHeader converts an x-amz-acl header value to ACLGrant.
// Unknown or empty values fall back to ACLPrivate.
func ParseACLHeader(s string) ACLGrant {
	switch s {
	case "public-read":
		return ACLPublicRead
	case "public-read-write":
		return ACLPublicReadWrite
	default:
		return ACLPrivate
	}
}

// PolicyActionString returns the canonical "s3:Xxx" string for a given S3Action.
// Unknown or unmapped actions return "s3:Unknown".
func (a S3Action) PolicyActionString() string {
	switch a {
	case GetObject:
		return "s3:GetObject"
	case HeadObject:
		return "s3:HeadObject"
	case ListBucket:
		return "s3:ListBucket"
	case PutObject:
		return "s3:PutObject"
	case CreateBucket:
		return "s3:CreateBucket"
	case DeleteObject:
		return "s3:DeleteObject"
	case DeleteBucket:
		return "s3:DeleteBucket"
	case CopyObject:
		return "s3:CopyObject"
	case ListMultipartUploads:
		return "s3:ListMultipartUploads"
	case GetBucketPolicy:
		return "s3:GetBucketPolicy"
	case PutBucketPolicy:
		return "s3:PutBucketPolicy"
	case DeleteBucketPolicy:
		return "s3:DeleteBucketPolicy"
	case GetBucketVersioning:
		return "s3:GetBucketVersioning"
	case PutBucketVersioning:
		return "s3:PutBucketVersioning"
	case ListBucketVersions:
		return "s3:ListBucketVersions"
	case GetObjectRetention:
		return "s3:GetObjectRetention"
	case PutObjectRetention:
		return "s3:PutObjectRetention"
	case GetBucketObjectLockConfiguration:
		return "s3:GetBucketObjectLockConfiguration"
	default:
		return "s3:Unknown"
	}
}

// isReadAction reports whether action is a read-only S3 operation.
func isReadAction(action S3Action) bool {
	return action == GetObject ||
		action == HeadObject ||
		action == ListBucket ||
		action == GetBucketVersioning ||
		action == ListBucketVersions ||
		action == GetObjectRetention ||
		action == GetBucketObjectLockConfiguration
}

// IsAuthorizedByACL checks whether accessKey may perform action given the object's ACL.
//
// ACLPublicReadWrite: any caller (including anonymous) permitted for all actions.
// ACLPublicRead: any caller permitted for read-only actions; writes require authentication.
// ACLPrivate: authenticated callers only (accessKey != "").
//
// Known limitation: Phase 13 is single-tenant. All authenticated callers are treated
// as owners. Multi-tenant support requires ecObjectMeta.OwnerKey (Phase 14+).
func IsAuthorizedByACL(acl ACLGrant, accessKey string, action S3Action) bool {
	if acl&ACLPublicReadWrite != 0 {
		return true
	}
	if acl&ACLPublicRead != 0 && isReadAction(action) {
		return true
	}
	return accessKey != ""
}
