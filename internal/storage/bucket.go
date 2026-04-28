package storage

import "strings"

// VFSBucketPrefix is the bucket-name prefix reserved for the VFS layer's
// internal buckets (one per volume, e.g. "__grainfs_vfs_default").
// VFS buckets are owned exclusively by the VFS layer; S3 versioning has
// no meaning for them, so backend writes use a fixed versionID and the
// VFS layer can rely on in-place overwrite semantics.
const VFSBucketPrefix = "__grainfs_vfs_"

// IsVFSBucket reports whether bucket belongs to the VFS internal namespace.
func IsVFSBucket(bucket string) bool {
	return strings.HasPrefix(bucket, VFSBucketPrefix)
}
