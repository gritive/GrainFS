package iceberg

import (
	"strings"
	"unicode/utf8"

	"github.com/cloudwego/hertz/pkg/app"
)

// auditString8MaxBytes bounds audit string fields. Copied verbatim from core
// audit_event.go to keep the iceberg package free of a server import.
const auditString8MaxBytes = 0xff

// auditString8 truncates s to auditString8MaxBytes bytes on a UTF-8 boundary.
// Copied verbatim from core audit_event.go:57.
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

// requestIDHertzKey mirrors core request_id.go:33.
const requestIDHertzKey = "grainfs.request_id"

// requestIDFromHertz reads the per-request id stashed on the Hertz context.
// Copied from core request_id.go:67.
func requestIDFromHertz(c *app.RequestContext) string {
	v, ok := c.Get(requestIDHertzKey)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

// hasBearerPrefix / trimBearerPrefix copied verbatim from core
// authn_middleware.go:228-236.
func hasBearerPrefix(s string) bool {
	return len(s) >= 7 && strings.EqualFold(s[:7], "Bearer ")
}

func trimBearerPrefix(s string) string {
	return s[7:]
}
