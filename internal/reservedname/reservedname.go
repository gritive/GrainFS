// Package reservedname holds bucket-name reservation predicates used by both
// internal/server and internal/s3auth. It MUST stay as a leaf package (no
// internal/server or internal/s3auth import) so both can depend on it without
// causing an import cycle.
package reservedname

import "strings"

// IsInternalBucket reports whether bucket is reserved for GrainFS internal use.
// Per spec line 468: any name with prefix "_grainfs" is reserved.
// Anonymous access to these buckets is unconditionally denied (see s3auth.Authorize).
func IsInternalBucket(bucket string) bool {
	return strings.HasPrefix(bucket, "_grainfs")
}

// DefaultBucketName is the auto-created bucket that allows anonymous access
// regardless of iam.anon-enabled. Tested in tests/e2e/phase0_quickstart_test.go.
//
// This constant is the single source of truth shared by the two trust-boundary
// sites that gate anon access to /default:
//   - internal/server/authn_middleware.go (F#41-ext anon fast-path)
//   - internal/s3auth/authorizer.go (D#2 implicit-anon emitter)
//
// If one site case-folds or renames and the other doesn't, asymmetric anon
// access opens — hence the shared const.
const DefaultBucketName = "default"

// IsReservedDefaultName reports whether the bucket name is the literal "default"
// — the implicit-anon bucket per spec line 469. Operator may delete/create it
// but cannot rename or alias.
func IsReservedDefaultName(bucket string) bool {
	return bucket == DefaultBucketName
}

// IsReservedBucketName reports whether name is reserved from being created or
// deleted by the public API. Returns true for both:
//   - prefix "_grainfs" (covers _grainfs, _grainfs-audit, _grainfs-test, ...)
//   - exact "default"
//
// Names like "default-2026", "mydefault", "analytics" are NOT reserved.
//
// The internal cluster bootstrap path may create reserved buckets by bypassing
// this check (T32 will seed _grainfs).
func IsReservedBucketName(name string) bool {
	return IsInternalBucket(name) || IsReservedDefaultName(name)
}
