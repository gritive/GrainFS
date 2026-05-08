package iam

import (
	"github.com/gritive/GrainFS/internal/s3auth"
)

// CheckAccess returns true iff the SA's grants permit `action` on `bucket`.
//
// Lookup order (delegated to Store.LookupGrant):
//  1. Explicit grant on (sa_id, bucket) — used as-is, even if it is more
//     restrictive than a wildcard grant. Explicit always wins on the
//     target bucket. (Least-privilege wins for buckets the admin chose
//     to scope explicitly.)
//  2. Wildcard grant on sa_id — applied when no explicit grant for
//     bucket exists.
//  3. RoleNone → deny.
//
// The matrix decision is delegated to RoleAllows.
func CheckAccess(s *Store, saID, bucket string, action s3auth.S3Action) bool {
	if saID == "" {
		return false
	}
	role := s.LookupGrant(saID, bucket)
	return RoleAllows(role, action)
}
