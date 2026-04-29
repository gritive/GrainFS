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

// IsInternalBucket reports whether bucket is an internal GrainFS bucket
// (VFS or NFSv4). Internal buckets never expose ETag to S3 clients, so
// MD5 hashing during PutObject can be skipped.
func IsInternalBucket(bucket string) bool {
	return bucket == NFS4BucketName || strings.HasPrefix(bucket, VFSBucketPrefix)
}
