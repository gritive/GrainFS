package server

import "github.com/gritive/GrainFS/internal/s3auth"

// s3ActionEnum maps an HTTP method + path context to an S3Action enum value.
// path is required to distinguish sub-resource operations (e.g., ?delete).
// hasPolicyQuery and other subresource booleans distinguish bucket/object
// subresources from regular bucket/object operations.
func s3ActionEnum(
	method, path string,
	hasKey, hasPolicyQuery, hasVersioningQuery, hasVersionsQuery, hasRetentionQuery, hasObjectLockQuery, hasLifecycleQuery bool,
) s3auth.S3Action {
	if hasPolicyQuery && !hasKey {
		switch method {
		case "GET":
			return s3auth.GetBucketPolicy
		case "PUT":
			return s3auth.PutBucketPolicy
		case "DELETE":
			return s3auth.DeleteBucketPolicy
		}
	}
	if hasLifecycleQuery && !hasKey {
		switch method {
		case "GET":
			return s3auth.GetBucketLifecycleConfiguration
		case "PUT":
			return s3auth.PutBucketLifecycleConfiguration
		case "DELETE":
			return s3auth.DeleteBucketLifecycleConfiguration
		}
	}
	// TODO: similar bucket-subresource gaps may exist for ?tagging / ?acl /
	// ?cors / ?notification / ?logging — surface in their own surgical fixes
	// when manifested.
	if hasVersioningQuery && !hasKey {
		switch method {
		case "GET":
			return s3auth.GetBucketVersioning
		case "PUT":
			return s3auth.PutBucketVersioning
		}
	}
	if hasVersionsQuery && !hasKey && method == "GET" {
		return s3auth.ListBucketVersions
	}
	if hasRetentionQuery && hasKey {
		switch method {
		case "GET":
			return s3auth.GetObjectRetention
		case "PUT":
			return s3auth.PutObjectRetention
		}
	}
	if hasObjectLockQuery && !hasKey && method == "GET" {
		return s3auth.GetBucketObjectLockConfiguration
	}
	switch method {
	case "GET":
		if hasKey {
			return s3auth.GetObject
		}
		return s3auth.ListBucket
	case "HEAD":
		if hasKey {
			return s3auth.HeadObject
		}
		return s3auth.ListBucket
	case "PUT":
		if hasKey {
			return s3auth.PutObject
		}
		return s3auth.CreateBucket
	case "DELETE":
		if hasKey {
			return s3auth.DeleteObject
		}
		return s3auth.DeleteBucket
	case "POST":
		return s3auth.PutObject // multipart upload
	default:
		return s3auth.UnknownAction
	}
}
