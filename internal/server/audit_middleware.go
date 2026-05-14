package server

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam"
)

const auditErrReasonKey = "audit.err_reason"

func (s *Server) auditEnvelopeMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if s.auditOutbox == nil {
			c.Next(ctx)
			return
		}
		bucket := c.Param("bucket")
		if bucket == "" || bucket == audit.BucketName {
			c.Next(ctx)
			return
		}

		key := getKey(c)
		requestID := uuid.NewString()
		c.Header("x-amz-request-id", requestID)
		start := time.Now()
		op := audit.ClassifyS3Operation(string(c.Method()), key != "", string(c.URI().QueryString()), toHTTPRequest(c).Header)
		ev := audit.S3Event{
			Ts:          start.UnixMicro(),
			EventID:     uuid.NewString(),
			RequestID:   requestID,
			SAID:        iam.PrincipalFromContext(ctx),
			SourceIP:    c.ClientIP(),
			UserAgent:   string(c.UserAgent()),
			Method:      string(c.Method()),
			Operation:   op.Operation,
			Bucket:      bucket,
			Key:         key,
			Subresource: op.Subresource,
			AuthStatus:  "allow",
			BytesIn:     int64(len(c.Request.Body())),
			VersionID:   string(c.QueryArgs().Peek("versionId")),
			UploadID:    string(c.QueryArgs().Peek("uploadId")),
		}
		if op.Operation == "CopyObject" {
			ev.CopySourceBucket, ev.CopySourceKey = parseAuditCopySource(string(c.GetHeader("x-amz-copy-source")))
		}

		if err := s.auditOutbox.AppendAttempt(ctx, ev); err != nil {
			writeXMLError(c, consts.StatusServiceUnavailable, "AuditUnavailable", "audit log unavailable")
			c.Abort()
			return
		}

		c.Next(ctx)

		ev.Status = int32(c.Response.StatusCode())
		if ev.Status == 0 {
			ev.Status = int32(http.StatusOK)
		}
		if ev.Status >= 400 {
			ev.AuthStatus = "deny"
			ev.ErrClass = http.StatusText(int(ev.Status))
			if ev.ErrClass == "" {
				ev.ErrClass = "Error"
			}
			if reason, ok := c.Get(auditErrReasonKey); ok {
				if s, ok := reason.(string); ok {
					ev.ErrReason = s
				}
			}
		}
		ev.BytesOut = int64(len(c.Response.Body()))
		ev.LatencyMs = int32(time.Since(start).Milliseconds())
		_ = s.auditOutbox.Finalize(context.Background(), ev)
	}
}

func parseAuditCopySource(raw string) (string, string) {
	raw = strings.TrimPrefix(raw, "/")
	raw = strings.Split(raw, "?")[0]
	bucket, key, ok := strings.Cut(raw, "/")
	if !ok {
		return raw, ""
	}
	return bucket, key
}
