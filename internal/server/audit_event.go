package server

import (
	"strings"
	"unicode/utf8"

	"github.com/gritive/GrainFS/internal/audit"
)

const (
	auditString16MaxBytes = 0xffff
	auditString8MaxBytes  = 0xff
)

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
	// T51' §6 policy decision metadata.
	ev.MatchedPolicyID = auditString16(ev.MatchedPolicyID)
	ev.MatchedSID = auditString16(ev.MatchedSID)
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
