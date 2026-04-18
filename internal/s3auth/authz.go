package s3auth

import "context"

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

// Authorizer evaluates whether a request is permitted.
type Authorizer interface {
	Allow(ctx context.Context, in PermCheckInput) bool
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

// isReadAction reports whether action is a read-only S3 operation.
func isReadAction(action S3Action) bool {
	return action == GetObject || action == HeadObject || action == ListBucket
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
