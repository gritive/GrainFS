package s3auth

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// cachedEntry holds the derived signing key for a (accessKey, credentialScope) pair.
// Keyed by "credentialScope:accessKey" — stable for a calendar day.
// Every cache hit still performs HMAC verification; only the 4-round key
// derivation is amortized.
type cachedEntry struct {
	signingKey []byte
	expiresAt  time.Time
}

// CacheStats holds cache hit/miss counters.
type CacheStats struct {
	Hits   int
	Misses int
}

// CachingVerifier wraps a Verifier with an LRU cache of pre-derived signing keys.
// On a cache hit the signing key derivation (4 HMAC rounds) is skipped, but the
// per-request HMAC is always recomputed — so a garbage signature on a warm key
// is still rejected.
type CachingVerifier struct {
	inner  *Verifier
	cache  *lru.Cache[string, cachedEntry]
	ttl    time.Duration
	hits   atomic.Int64
	misses atomic.Int64
}

// NewCachingVerifier creates a CachingVerifier with the given LRU size and TTL.
func NewCachingVerifier(inner *Verifier, size int, ttl time.Duration) *CachingVerifier {
	c, err := lru.New[string, cachedEntry](size)
	if err != nil {
		panic(fmt.Sprintf("lru.New: %v", err))
	}
	return &CachingVerifier{inner: inner, cache: c, ttl: ttl}
}

// Verify checks the request signature.
// On cache hit: signing key derivation (4 HMAC rounds) is skipped, but the
// per-request HMAC is still verified against the cached signing key.
// On cache miss: delegates to inner.Verify(), then caches the derived signing key.
func (cv *CachingVerifier) Verify(r *http.Request) (string, error) {
	accessKey, date, region, service, scope := parseCredentialFull(r)
	var cacheKey string
	if scope != "" && accessKey != "" {
		cacheKey = scope + ":" + accessKey
		if entry, ok := cv.cache.Get(cacheKey); ok && time.Now().Before(entry.expiresAt) {
			if err := cv.inner.VerifyWithSigningKey(r, accessKey, entry.signingKey); err != nil {
				return "", err
			}
			cv.hits.Add(1)
			return accessKey, nil
		}
	}

	cv.misses.Add(1)
	key, err := cv.inner.Verify(r)
	if err != nil {
		return "", err
	}

	if cacheKey != "" && date != "" {
		secretKey := cv.inner.LookupSecret(key)
		if secretKey != "" {
			signingKey := DeriveSigningKey(secretKey, date, region, service)
			ttl := cv.ttl
			if expires := presignedExpiry(r); expires > 0 && expires < ttl {
				ttl = expires
			}
			cv.cache.Add(cacheKey, cachedEntry{
				signingKey: signingKey,
				expiresAt:  time.Now().Add(ttl),
			})
		}
	}
	return key, nil
}

// Stats returns current cache hit/miss counts.
func (cv *CachingVerifier) Stats() CacheStats {
	return CacheStats{
		Hits:   int(cv.hits.Load()),
		Misses: int(cv.misses.Load()),
	}
}

// LookupSecret delegates to the inner Verifier.
func (cv *CachingVerifier) LookupSecret(accessKey string) string {
	return cv.inner.LookupSecret(accessKey)
}

// parseCredentialFull parses credential components from Authorization header or
// presigned URL query params. Returns empty strings on malformed or missing input.
func parseCredentialFull(r *http.Request) (accessKey, date, region, service, scope string) {
	var credential string
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != "" {
		credential = q.Get("X-Amz-Credential")
	} else {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
			return "", "", "", "", ""
		}
		credential = parseAuthHeader(auth)["Credential"]
	}
	// credential = "accessKey/date/region/service/aws4_request"
	parts := strings.SplitN(credential, "/", 5)
	if len(parts) < 5 || parts[0] == "" {
		return "", "", "", "", ""
	}
	return parts[0], parts[1], parts[2], parts[3], parts[1] + "/" + parts[2] + "/" + parts[3] + "/aws4_request"
}

// presignedExpiry returns the remaining duration until a presigned URL expires,
// or 0 if not a presigned URL or expiry cannot be determined.
func presignedExpiry(r *http.Request) time.Duration {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") == "" {
		return 0
	}
	amzDate := q.Get("X-Amz-Date")
	expiresStr := q.Get("X-Amz-Expires")
	if amzDate == "" || expiresStr == "" {
		return 0
	}
	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return 0
	}
	seconds, err := strconv.ParseInt(expiresStr, 10, 64)
	if err != nil {
		return 0
	}
	expiry := t.Add(time.Duration(seconds) * time.Second)
	remaining := time.Until(expiry)
	if remaining <= 0 {
		return 0
	}
	return remaining
}
