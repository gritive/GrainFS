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

// IsReservedDefaultName reports whether the bucket name is the literal "default"
// — the implicit-anon bucket per spec line 469. Operator may delete/create it
// but cannot rename or alias.
func IsReservedDefaultName(bucket string) bool {
	return bucket == "default"
}
