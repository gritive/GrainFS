package nfs4server

import "strings"

// extractBucketAndKey splits an NFS path into bucket and object key.
//
// Phase 0b (this implementation): wrapper that always returns the legacy
// "__grainfs_nfs4" literal as bucket. The path is stripped of its leading slash
// to form the key. The legacy storage constant has been removed; this literal
// is the last bridge before Phase 3 introduces real per-path multi-bucket routing.
//
// Phase 3 will replace this with multi-bucket routing logic per design doc
// (whitekid-devel-design-20260514-042559-nfs-multi-export.md). At that
// point this helper becomes the routing primitive for per-export dispatch.
func extractBucketAndKey(path string) (bucket, key string) {
	key = strings.TrimPrefix(path, "/")
	return "__grainfs_nfs4", key
}
