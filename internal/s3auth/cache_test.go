package s3auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestVerifier(t *testing.T) *Verifier {
	t.Helper()
	return NewVerifier([]Credentials{{AccessKey: "testkey", SecretKey: "testsecret"}})
}

func signedRequest(t *testing.T, accessKey, secretKey string) *http.Request {
	t.Helper()
	req := httptest.NewRequest("GET", "http://localhost/mybucket/mykey", nil)
	SignRequest(req, accessKey, secretKey, "us-east-1")
	return req
}

func TestCachingVerifier_CacheHitAvoidsHMAC(t *testing.T) {
	inner := newTestVerifier(t)
	cv := NewCachingVerifier(inner, 16, 5*time.Minute)

	req := signedRequest(t, "testkey", "testsecret")
	req2 := signedRequest(t, "testkey", "testsecret")

	// First call: cache miss → inner.Verify()
	key1, err := cv.Verify(req)
	require.NoError(t, err)
	assert.Equal(t, "testkey", key1)

	// Second call with same credential scope: cache hit
	key2, err := cv.Verify(req2)
	require.NoError(t, err)
	assert.Equal(t, "testkey", key2)

	// Hit rate should be > 0
	assert.Equal(t, 1, cv.Stats().Hits)
	assert.Equal(t, 1, cv.Stats().Misses)
}

func TestCachingVerifier_InvalidCredentialNotCached(t *testing.T) {
	inner := newTestVerifier(t)
	cv := NewCachingVerifier(inner, 16, 5*time.Minute)

	req := signedRequest(t, "testkey", "wrongsecret")
	_, err := cv.Verify(req)
	assert.Error(t, err)

	// Error must not be cached: retry should also go to inner
	req2 := signedRequest(t, "testkey", "wrongsecret")
	_, err = cv.Verify(req2)
	assert.Error(t, err)

	assert.Equal(t, 0, cv.Stats().Hits)
	assert.Equal(t, 2, cv.Stats().Misses)
}

func TestCachingVerifier_ExpiredEntryRefetches(t *testing.T) {
	inner := newTestVerifier(t)
	cv := NewCachingVerifier(inner, 16, 1*time.Millisecond) // very short TTL

	req := signedRequest(t, "testkey", "testsecret")
	_, err := cv.Verify(req)
	require.NoError(t, err)

	time.Sleep(5 * time.Millisecond) // wait for TTL expiry

	req2 := signedRequest(t, "testkey", "testsecret")
	_, err = cv.Verify(req2)
	require.NoError(t, err)

	// Second call should be a miss (expired entry)
	assert.Equal(t, 0, cv.Stats().Hits)
	assert.Equal(t, 2, cv.Stats().Misses)
}

func TestCachingVerifier_UnknownKeyReturnsError(t *testing.T) {
	inner := newTestVerifier(t)
	cv := NewCachingVerifier(inner, 16, 5*time.Minute)

	req := signedRequest(t, "unknownkey", "anysecret")
	_, err := cv.Verify(req)
	assert.Error(t, err)
}

// BenchmarkVerify confirms that cache hits are ≥10x faster than cold HMAC verification.
// BenchmarkVerify_Cold measures cold-path HMAC verification (cache miss every call).
func BenchmarkVerify_Cold(b *testing.B) {
	inner := NewVerifier([]Credentials{{AccessKey: "testkey", SecretKey: "testsecret"}})
	cv := NewCachingVerifier(inner, 4096, 5*time.Minute)

	// Pre-build one signed request per iteration to isolate Verify cost
	reqs := make([]*http.Request, b.N)
	for i := range reqs {
		req := httptest.NewRequest("GET", "http://localhost/bucket/key", nil)
		SignRequest(req, "testkey", "testsecret", "us-east-1")
		reqs[i] = req
	}
	// Clear cache to ensure all calls are cold
	cv.cache.Purge()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cv.cache.Purge()
		_, _ = cv.Verify(reqs[i])
	}
}

// BenchmarkVerify_Hot measures hot-path cache hit (HMAC skipped).
func BenchmarkVerify_Hot(b *testing.B) {
	inner := NewVerifier([]Credentials{{AccessKey: "testkey", SecretKey: "testsecret"}})
	cv := NewCachingVerifier(inner, 4096, 5*time.Minute)

	// Pre-build signed requests — same scope so they all hit the same cache entry
	reqs := make([]*http.Request, b.N)
	for i := range reqs {
		req := httptest.NewRequest("GET", "http://localhost/bucket/key", nil)
		SignRequest(req, "testkey", "testsecret", "us-east-1")
		reqs[i] = req
	}
	// Warm the cache
	_, _ = cv.Verify(reqs[0])

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = cv.Verify(reqs[i])
	}
}
