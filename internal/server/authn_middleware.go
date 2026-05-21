package server

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/reservedname"
	"github.com/gritive/GrainFS/internal/s3auth"
)

// icebergSigV4SkewWindow is the maximum allowed clock drift between a SigV4
// X-Amz-Date and the server's wall clock for iceberg REST callers. Matches
// AWS standard (±15 min). Header-path SigV4 in s3auth.Verify does not enforce
// skew; the iceberg branch tightens that here for the iceberg trust boundary.
const icebergSigV4SkewWindow = 15 * time.Minute

func isBucketFormPost(c *app.RequestContext) bool {
	return string(c.Method()) == "POST" &&
		strings.TrimPrefix(c.Param("key"), "/") == "" &&
		strings.HasPrefix(string(c.GetHeader("Content-Type")), "multipart/form-data")
}

func s3PathBucketKey(path string) (string, string) {
	if !routeIsS3Path(path) {
		return "", ""
	}
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return "", ""
	}
	bucket, key, _ := strings.Cut(path, "/")
	return bucket, key
}

func (s *Server) authMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		path := string(c.URI().Path())
		switch routeAuthnPolicyForPath(path) {
		case routeAuthnAnonymous:
			c.Next(ctx)
			return
		case routeAuthnLocalhost:
			if isLocalhostAddr(c.RemoteAddr().String()) {
				c.Next(ctx)
				return
			}
		}

		r := toHTTPRequest(c)
		if isBucketFormPost(c) {
			ctx = WithAccessKey(ctx, "")
			c.Next(ctx)
			return
		}

		// Anonymous fast path: unsigned GET/HEAD on plain object data may be allowed
		// by object ACL. Subresource requests (?acl, ?tagging, etc.) and all other
		// methods always require authentication, checked via RawQuery == "".
		method := string(c.Method())
		key := strings.TrimPrefix(c.Param("key"), "/")
		bucket := c.Param("bucket")
		if bucket == "" {
			bucket, key = s3PathBucketKey(path)
		}
		if nextCtx, ok := s.authenticateAuditInternalRequest(ctx, c, r, bucket, key, method); ok {
			ctx = nextCtx
			c.Next(ctx)
			return
		}
		isObjectRead := (method == "GET" || method == "HEAD") && key != "" && r.URL.RawQuery == ""
		if isObjectRead && r.Header.Get("Authorization") == "" {
			ctx = WithAccessKey(ctx, "")
			c.Next(ctx)
			return
		}

		isIceberg := routeSurfaceForPath(path) == routeSurfaceIceberg

		// Phase 0 anon fast path (F#41) + default-bucket extension (F#41-ext):
		// when the caller sent no Authorization header, defer the allow/deny
		// decision to the authorizer (s3auth.Authorizer at
		// internal/s3auth/authorizer.go) in either of two cases:
		//
		//   1. iam.anon-enabled=true (Phase 0): authorizer short-circuits with
		//      ReasonAnonEnabled for any bucket; this is the original F#41.
		//   2. bucket=="default" (F#41-ext): authorizer's D#2 implicit-anon
		//      Allow path (authorizer.go:73-81) emits
		//      ReasonDefaultBucketImplicitAnon for any verb on s3://default
		//      regardless of iam.anon-enabled. The startup banner promises
		//      "default remains public" and this guarantee must survive the
		//      Phase 0 → Phase 2 flip (F#26 banner contract). Without (2),
		//      unsigned PUT/LIST on /default returned 403 from
		//      authenticateSignedRequest in Phase 2 before the authorizer
		//      ever ran — banner-vs-behavior mismatch surfaced by T73.
		//
		// Authenticated requests (Authorization header present) are NOT
		// affected by either branch — SigV4 verification still runs, revoked
		// keys still fail, presigned URLs still go through the verifier.
		//
		// Iceberg surface is excluded — iceberg routes have their own anon-aware
		// per-route guards (see icebergGuarded + iceberg_authn.go), and routing
		// anon iceberg through SigV4-bypass here would break the bearer trust
		// boundary.
		//
		// Presigned URLs (X-Amz-Algorithm in query) are NOT anon — they carry
		// SigV4 in the query string. Those must continue through
		// authenticateSignedRequest so revoked-key checks fire. Use the
		// canonical s3auth.HasPresignedAlgorithm helper so the predicate
		// matches the verifier's (handles percent-encoded keys + empty values).
		if !isIceberg &&
			r.Header.Get("Authorization") == "" &&
			!s3auth.HasPresignedAlgorithm(r) &&
			(s.anonEnabled() || bucket == reservedname.DefaultBucketName) {
			ctx = WithAccessKey(ctx, "")
			c.Next(ctx)
			return
		}

		if isIceberg {
			// Bearer requests skip SigV4 entirely — the icebergGuarded middleware
			// on each route performs JWT verification and policy gating.
			if hasBearerPrefix(r.Header.Get("Authorization")) {
				c.Next(ctx)
				return
			}
			if reason := icebergSkewReject(r); reason != "" {
				s.recordAuditAuthFailure(ctx, c, consts.StatusUnauthorized, "authn")
				writeIcebergError(c, 401, "NotAuthorizedException", reason)
				c.Abort()
				return
			}
		}

		nextCtx, failure := s.authenticateSignedRequest(ctx, r)
		if failure != nil {
			s.recordAuditAuthFailure(ctx, c, failure.status, failure.reason)
			if isIceberg {
				// Iceberg REST clients distinguish 401 (auth required) from 403
				// (post-authz forbidden). The S3 verifier returns 403 for every
				// authn failure; remap to 401 + NotAuthorizedException for Iceberg
				// callers. failure.code (S3-XML) is dropped to keep S3-specific
				// codes out of the Iceberg JSON.
				writeIcebergError(c, 401, "NotAuthorizedException", failure.message)
			} else {
				writeXMLError(c, failure.status, failure.code, failure.message)
			}
			c.Abort()
			return
		}
		ctx = nextCtx
		c.Next(ctx)
	}
}

// anonEnabled reports whether iam.anon-enabled=true on the cluster config
// store. Reused on the S3 surface to gate the F#41 anon fast path so that the
// Phase 0 startup banner contract (read/write s3://default with no auth) holds
// across all verbs. Returns false when the config reader is not wired (the key
// defaults to true at registration, but we treat "no reader" as conservatively
// disabled rather than implicitly allowing). bearerCfg is wired by
// WithBearerConfig from the same cfgStore as the iceberg branch.
func (s *Server) anonEnabled() bool {
	if s.bearerCfg == nil {
		return false
	}
	v, ok := s.bearerCfg.GetBool("iam.anon-enabled")
	return ok && v
}

// icebergSkewReject returns a non-empty rejection reason if the request's
// X-Amz-Date is outside the ±15 minute skew window. Returns "" to defer to
// the normal SigV4 verifier (including the "no auth header at all" path —
// missing X-Amz-Date is the verifier's job to reject, not the skew gate's).
func icebergSkewReject(r *http.Request) string {
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		// Also covers presigned URLs where the query carries the date.
		amzDate = r.URL.Query().Get("X-Amz-Date")
	}
	if amzDate == "" {
		return ""
	}
	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return "invalid X-Amz-Date"
	}
	if d := time.Since(t); d > icebergSigV4SkewWindow || d < -icebergSigV4SkewWindow {
		return "request signature outside allowed time window"
	}
	return ""
}

// hasBearerPrefix reports whether s begins with a case-insensitive "bearer "
// prefix (7 bytes). RFC 6750 requires "Bearer" but the OAuth token endpoint
// emits token_type:"bearer" (lowercase), so clients may send either form.
func hasBearerPrefix(s string) bool {
	return len(s) >= 7 && strings.EqualFold(s[:7], "Bearer ")
}

// trimBearerPrefix strips the case-insensitive "Bearer " prefix from s and
// returns the remaining token. Callers must have checked hasBearerPrefix first.
func trimBearerPrefix(s string) string {
	return s[7:]
}

// isLocalhostAddr reports whether addr (typically from c.RemoteAddr().String(),
// in "host:port" or "[host]:port" form) is a loopback address.
// Covers IPv4 loopback, IPv6 loopback, the IPv4-mapped IPv6 loopback
// ([::ffff:127.0.0.1]:PORT), and the literal "localhost" hostname.
func isLocalhostAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr // no port present
	}
	switch host {
	case "127.0.0.1", "::1", "::ffff:127.0.0.1", "localhost":
		return true
	}
	return false
}

// localhostOnly returns a middleware that rejects non-localhost connections with 403.
func localhostOnly() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if !isLocalhostAddr(c.RemoteAddr().String()) {
			c.JSON(consts.StatusForbidden, map[string]string{
				"error": "admin endpoints are restricted to localhost",
			})
			c.Abort()
			return
		}
		c.Next(ctx)
	}
}
