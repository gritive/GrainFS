package storage

import "strings"

// VFSBucketPrefix is the bucket-name prefix reserved for the VFS layer's
// internal buckets (one per volume, e.g. "__grainfs_vfs_default").
// VFS buckets are owned exclusively by the VFS layer; S3 versioning has
// no meaning for them, so backend writes use a fixed versionID and the
// VFS layer can rely on in-place overwrite semantics.
const VFSBucketPrefix = "__grainfs_vfs_"

// NFS4BucketName is the single internal bucket used by the NFSv4 server.
const NFS4BucketName = "__grainfs_nfs4"

// IsVFSBucket reports whether bucket belongs to the VFS internal namespace.
func IsVFSBucket(bucket string) bool {
	return strings.HasPrefix(bucket, VFSBucketPrefix)
}

// internalBucketPrefix is the common prefix for all GrainFS-internal buckets.
// Internal buckets are not exposed through the S3 API, but ETag is still
// computed and stored — it is the corruption-detection oracle relied on by
// scrub paths (volume scrub, future EC migration). Treat internal-bucket
// classification as a routing/access concern only, never as a hash-skip
// shortcut. See TODOS.md "Hash 정책 분기 가드".
const internalBucketPrefix = "__grainfs_"

// IsInternalBucket reports whether bucket is an internal GrainFS bucket.
func IsInternalBucket(bucket string) bool {
	return strings.HasPrefix(bucket, internalBucketPrefix)
}
