package server

import "github.com/gritive/GrainFS/internal/s3auth"

// s3ActionEnum maps an HTTP method + path context to an S3Action enum value.
// path is required to distinguish sub-resource operations (e.g., ?delete).
// hasPolicyQuery distinguishes bucket-policy CRUD (?policy) from regular
// bucket operations so authz can require Admin for Put/Delete and Read
// for Get on the policy resource.
func s3ActionEnum(method, path string, hasKey, hasPolicyQuery bool) s3auth.S3Action {
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
