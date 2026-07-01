package storage

import "strings"

// ValidBucketName reports whether name is a valid user-facing bucket name.
// Rules (AWS-compatible): 3–63 chars, lowercase letters/digits/dots/hyphens only,
// start and end with letter or digit, no path separators. Internal __grainfs_*
// buckets are also rejected.
func ValidBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	if IsInternalBucket(name) {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '-') {
			return false
		}
	}
	isAlnum := func(c byte) bool {
		return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
	}
	return isAlnum(name[0]) && isAlnum(name[len(name)-1])
}

// internalBucketPrefix is the common prefix for all GrainFS-internal buckets.
// Internal buckets are not exposed through the S3 API, but ETag is still
// computed and stored — it is the corruption-detection oracle relied on by
// scrub paths. Treat internal-bucket classification as a routing/access
// concern only, never as a hash-skip shortcut. See TODOS.md "Hash 정책 분기 가드".
const internalBucketPrefix = "__grainfs_"

// IsInternalBucket reports whether bucket is an internal GrainFS bucket.
func IsInternalBucket(bucket string) bool {
	return strings.HasPrefix(bucket, internalBucketPrefix)
}
