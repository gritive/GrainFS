package server

import (
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// hasUnsupportedACLHeaders reports whether a PutObject or CopyObject request
// carries ACL headers that GrainFS does not implement.
//
// Two surfaces are rejected (fail-closed → 501 NotImplemented):
//   - Any x-amz-grant-* explicit ACL grant header (requires cross-account /
//     multi-tenant identity support that is not implemented).
//   - An x-amz-acl canned ACL value not in the supported set
//     {private, public-read, public-read-write}: the recognized-but-unsupported
//     AWS values (authenticated-read, bucket-owner-read,
//     bucket-owner-full-control, aws-exec-read) and unknown garbage are both
//     rejected here. See s3auth.ValidateCannedACL for the authoritative list.
//
// Silently downgrading unsupported canned values to private would give the
// caller false confidence that their requested access-control semantics were
// honored; returning 501 prevents that trust violation (mirrors the
// SSE-C/KMS and Object Lock fail-closed conventions).
func hasUnsupportedACLHeaders(c *app.RequestContext) bool {
	if hasAnyHeader(c,
		"x-amz-grant-full-control",
		"x-amz-grant-read",
		"x-amz-grant-read-acp",
		"x-amz-grant-write",
		"x-amz-grant-write-acp",
	) {
		return true
	}
	return !s3auth.ValidateCannedACL(string(c.GetHeader("x-amz-acl")))
}

// writeACLNotImplemented rejects a request that carries an unsupported ACL
// header with 501 NotImplemented, matching the SSE-C/KMS and Object Lock
// fail-closed convention.
func writeACLNotImplemented(c *app.RequestContext) {
	writeXMLError(c, consts.StatusNotImplemented, "NotImplemented",
		"ACL value is not supported; supported canned ACLs: private, public-read, public-read-write")
}
