// Package server — AppendObject HTTP entry.
//
// PUT /<bucket>/<key> with the S3 Express `x-amz-write-offset-bytes` header is
// routed here instead of the standard PutObject path. The header carries the
// caller's view of the current object size; the backend rejects with
// InvalidWriteOffset when it disagrees (Red 19).
//
// Body is read into memory up to a hard 64 MiB cap so the ClusterCoordinator
// can re-issue the request after a stale-placement retry (Task 21) without
// needing to buffer streams of arbitrary size.
package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
)

const (
	appendOffsetHeader = "x-amz-write-offset-bytes"
	// appendBodyMaxBytes matches the AppendObject spec v3 hard cap. Keeping the
	// body in memory simplifies stale-placement retry inside ClusterCoordinator.
	appendBodyMaxBytes = 64 << 20
)

// appendObject handles a PUT carrying the x-amz-write-offset-bytes header.
// Returns true when this request was handled (the standard PutObject path
// falls through only when the header is absent).
func (s *Server) appendObject(ctx context.Context, c *app.RequestContext, bucket, key string) bool {
	rawOff := string(c.GetHeader(appendOffsetHeader))
	if rawOff == "" {
		return false
	}
	off, err := strconv.ParseInt(rawOff, 10, 64)
	if err != nil || off < 0 {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "x-amz-write-offset-bytes is invalid")
		return true
	}

	// AppendObject is not defined on versioning-enabled buckets — S3 Express
	// rejects with 501. GetBucketVersioning may return ErrUnsupportedOperation
	// on backends that don't track versioning (LocalBackend); that's treated
	// as Unversioned.
	if state, vErr := s.ops.GetBucketVersioning(bucket); vErr == nil && state == "Enabled" {
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "AppendObject is not supported on versioning-enabled buckets")
		return true
	}

	body, err := putObjectBody(c)
	if err != nil {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", err.Error())
		return true
	}
	if len(body) > appendBodyMaxBytes {
		writeXMLError(c, consts.StatusBadRequest, "EntityTooLarge", fmt.Sprintf("AppendObject body exceeds %d byte limit", appendBodyMaxBytes))
		return true
	}

	ap, ok := s.backend.(storage.AppendObjecter)
	if !ok {
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "backend does not support AppendObject")
		return true
	}
	obj, err := ap.AppendObject(ctx, bucket, key, off, bytes.NewReader(body))
	switch {
	case errors.Is(err, storage.ErrAppendOffsetMismatch):
		writeXMLError(c, consts.StatusBadRequest, "InvalidWriteOffset", "the write offset does not match the object size")
	case errors.Is(err, storage.ErrAppendNotSupported):
		writeXMLError(c, consts.StatusBadRequest, "InvalidRequest", "object is not appendable")
	case errors.Is(err, storage.ErrAppendCapExceeded):
		c.Response.Header.Set("Retry-After", "1")
		writeXMLError(c, consts.StatusServiceUnavailable, "SlowDown", "append segment cap reached")
	case errors.Is(err, cluster.ErrForwardBufferFull):
		c.Response.Header.Set("Retry-After", "1")
		writeXMLError(c, consts.StatusServiceUnavailable, "SlowDown", "append forward buffer saturated")
	case err != nil:
		mapError(c, err)
	default:
		c.Header("ETag", fmt.Sprintf("\"%s\"", obj.ETag))
		c.Status(consts.StatusOK)
	}
	return true
}
