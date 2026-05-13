package nfs4server

import "strings"

// extractBucketAndKey splits an NFS path into bucket and object key.
//
// Phase 0a (this implementation): wrapper that always returns the legacy
// single nfs4Bucket as bucket. The path is stripped of its leading slash
// to form the key, matching existing nfs4Bucket-based code.
//
// Phase 3 will replace this with multi-bucket routing logic per design doc
// (whitekid-devel-design-20260514-042559-nfs-multi-export.md). At that
// point this helper becomes the routing primitive for per-export dispatch.
func extractBucketAndKey(path string) (bucket, key string) {
	key = strings.TrimPrefix(path, "/")
	return nfs4Bucket, key
}
