package server

import (
	"context"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam"
)

const auditErrReasonKey = "audit.err_reason"
const auditObjectKeyKey = "audit.object_key"
const auditBytesOutKey = "audit.bytes_out"

const (
	auditString16MaxBytes = 0xffff
	auditString8MaxBytes  = 0xff
)

func (s *Server) auditEnvelopeMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if s.auditOutbox == nil && s.auditEmitter == nil {
			c.Next(ctx)
			return
		}
		bucket := c.Param("bucket")
		if bucket == "" {
			c.Next(ctx)
			return
		}
		if bucket == audit.BucketName && AccessKeyFromContext(ctx) == s.auditInternalAccessKey {
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
			NodeID:      s.auditNodeID,
			RequestID:   requestID,
			SAID:        iam.PrincipalFromContext(ctx),
			SourceIP:    c.ClientIP(),
			UserAgent:   auditString16(string(c.UserAgent())),
			Method:      auditString8(string(c.Method())),
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
		ev = normalizeAuditEvent(ev)

		if s.auditOutbox != nil {
			if err := s.auditOutbox.AppendAttempt(ctx, ev); err != nil {
				writeXMLError(c, consts.StatusServiceUnavailable, "AuditUnavailable", "audit log unavailable")
				c.Abort()
				return
			}
		}

		c.Next(ctx)

		ev.Status = int32(c.Response.StatusCode())
		if key, ok := c.Get(auditObjectKeyKey); ok {
			if s, ok := key.(string); ok && s != "" {
				ev.Key = s
			}
		}
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
			if ev.ErrReason == "" {
				ev.AuthStatus = "error"
			}
		}
		if bytesOut, ok := c.Get(auditBytesOutKey); ok {
			if n, ok := bytesOut.(int64); ok {
				ev.BytesOut = n
			}
		} else {
			ev.BytesOut = int64(len(c.Response.Body()))
		}
		ev.LatencyMs = int32(time.Since(start).Milliseconds())
		ev = normalizeAuditEvent(ev)
		if s.auditOutbox != nil {
			if err := s.auditOutbox.Finalize(context.Background(), ev); err != nil {
				log.Error().Err(err).Str("event_id", ev.EventID).Msg("audit envelope: finalize failed")
			}
		} else {
			s.auditEmitter.EmitS3(ev)
		}
	}
}

func (s *Server) recordAuditAuthFailure(ctx context.Context, c *app.RequestContext, status int, reason string) {
	if s.auditOutbox == nil && s.auditEmitter == nil {
		return
	}
	path := string(c.URI().Path())
	bucket := c.Param("bucket")
	key := getKey(c)
	if bucket == "" {
		bucket, key = s3PathBucketKey(path)
	}
	if bucket == "" || bucket == audit.BucketName {
		return
	}
	requestID := uuid.NewString()
	c.Header("x-amz-request-id", requestID)
	now := time.Now()
	op := audit.ClassifyS3Operation(string(c.Method()), key != "", string(c.URI().QueryString()), toHTTPRequest(c).Header)
	ev := audit.S3Event{
		Ts:          now.UnixMicro(),
		EventID:     uuid.NewString(),
		NodeID:      s.auditNodeID,
		RequestID:   requestID,
		SAID:        iam.PrincipalFromContext(ctx),
		SourceIP:    c.ClientIP(),
		UserAgent:   auditString16(string(c.UserAgent())),
		Method:      auditString8(string(c.Method())),
		Operation:   op.Operation,
		Bucket:      bucket,
		Key:         key,
		Subresource: op.Subresource,
		Status:      int32(status),
		AuthStatus:  "deny",
		ErrClass:    "AccessDenied",
		ErrReason:   reason,
		BytesIn:     int64(len(c.Request.Body())),
	}
	ev = normalizeAuditEvent(ev)
	if s.auditOutbox != nil {
		if err := s.auditOutbox.AppendFinalized(context.Background(), ev); err != nil {
			log.Error().Err(err).Str("event_id", ev.EventID).Msg("audit envelope: auth failure record failed")
		}
	} else {
		s.auditEmitter.EmitS3(ev)
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

func normalizeAuditEvent(ev audit.S3Event) audit.S3Event {
	ev.EventID = auditString16(ev.EventID)
	ev.NodeID = auditString16(ev.NodeID)
	ev.RequestID = auditString16(ev.RequestID)
	ev.SAID = auditString16(ev.SAID)
	ev.SourceIP = auditString16(ev.SourceIP)
	ev.UserAgent = auditString16(ev.UserAgent)
	ev.Method = auditString8(ev.Method)
	ev.Operation = auditString16(ev.Operation)
	ev.Bucket = auditString16(ev.Bucket)
	ev.Key = auditString16(ev.Key)
	ev.Subresource = auditString16(ev.Subresource)
	ev.AuthStatus = auditString16(ev.AuthStatus)
	ev.ErrClass = auditString16(ev.ErrClass)
	ev.ErrReason = auditString16(ev.ErrReason)
	ev.VersionID = auditString16(ev.VersionID)
	ev.UploadID = auditString16(ev.UploadID)
	ev.CopySourceBucket = auditString16(ev.CopySourceBucket)
	ev.CopySourceKey = auditString16(ev.CopySourceKey)
	return ev
}

func auditString16(s string) string {
	return truncateUTF8Bytes(s, auditString16MaxBytes)
}

func auditString8(s string) string {
	return truncateUTF8Bytes(s, auditString8MaxBytes)
}

func truncateUTF8Bytes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := 0
	for idx := range s {
		if idx > max {
			break
		}
		cut = idx
	}
	if cut == 0 {
		_, size := utf8.DecodeRuneInString(s)
		if size > max {
			return ""
		}
		return s[:size]
	}
	return s[:cut]
}
