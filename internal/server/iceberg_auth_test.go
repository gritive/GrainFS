package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

// TestIcebergRoute_AnonymousModeRemoved_NoBypass asserts that NO iceberg
// route surface entry uses routeAuthnAnonymous. Regression guard: any future
// commit that flips an iceberg prefix back to anonymous fails CI.
//
// Also asserts the manifest has at least one iceberg entry so the test cannot
// pass vacuously if a future refactor accidentally removes all iceberg routes.
func TestIcebergRoute_AnonymousModeRemoved_NoBypass(t *testing.T) {
	icebergEntries := 0
	for _, entry := range routeSurfaceManifest {
		if entry.surface == routeSurfaceIceberg {
			icebergEntries++
			assert.NotEqualf(t, routeAuthnAnonymous, entry.authn,
				"iceberg route %q/%q must not be anonymous",
				entry.pathPrefix, entry.pathExact)
		}
	}
	require.Greaterf(t, icebergEntries, 0,
		"manifest must declare at least one iceberg route — found none")
}

// TestIcebergSkewReject_MalformedDate covers the parse-error branch.
// A future regression that swapped time.Parse for a permissive parser would
// silently bypass the skew gate.
func TestIcebergSkewReject_MalformedDate(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.com/iceberg/v1/config", nil)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Date", "not-a-date")
	reason := icebergSkewReject(req)
	assert.Equal(t, "invalid X-Amz-Date", reason)
}

// TestIcebergSkewReject_PresignedURLFallback covers the presigned-URL path
// where X-Amz-Date is in the query string (no header). A regression deleting
// the query-param fallback would let presigned URLs bypass skew enforcement
// while still being signed.
func TestIcebergSkewReject_PresignedURLFallback(t *testing.T) {
	stale := time.Now().UTC().Add(-30 * time.Minute).Format("20060102T150405Z")
	u := "http://example.com/iceberg/v1/config?X-Amz-Date=" + stale
	req, err := http.NewRequest(http.MethodGet, u, nil)
	require.NoError(t, err)
	require.Empty(t, req.Header.Get("X-Amz-Date"), "test setup: header must be empty")
	reason := icebergSkewReject(req)
	assert.Equal(t, "request signature outside allowed time window", reason)
}

// TestIcebergSkewReject_MissingDate_Defers covers the no-header AND
// no-query-param path. The skew gate must return "" and let the verifier
// produce the actual error — failing here would cause a 401 for any request
// missing the auth headers entirely, double-counting the rejection.
func TestIcebergSkewReject_MissingDate_Defers(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.com/iceberg/v1/config", nil)
	require.NoError(t, err)
	reason := icebergSkewReject(req)
	assert.Equal(t, "", reason, "skew gate should defer to verifier when no date is present")
}

// TestIcebergSkewReject_WindowBoundaries covers the ±15-min boundary in both
// directions. Catches asymmetric clock-drift bugs (e.g., using time.Since
// without the negative branch) and off-by-one boundary errors.
func TestIcebergSkewReject_WindowBoundaries(t *testing.T) {
	now := time.Now().UTC()
	cases := []struct {
		name      string
		offset    time.Duration
		wantEmpty bool // true = accept (defer), false = reject
	}{
		{"past_within", -14 * time.Minute, true},
		{"future_within", 14 * time.Minute, true},
		{"past_outside", -30 * time.Minute, false},
		{"future_outside", 30 * time.Minute, false},
		{"past_just_outside", -16 * time.Minute, false},
		{"future_just_outside", 16 * time.Minute, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, "http://example.com/iceberg/v1/config", nil)
			require.NoError(t, err)
			req.Header.Set("X-Amz-Date", now.Add(tc.offset).Format("20060102T150405Z"))
			reason := icebergSkewReject(req)
			if tc.wantEmpty {
				assert.Empty(t, reason, "offset %s should accept", tc.offset)
			} else {
				assert.Equal(t, "request signature outside allowed time window", reason,
					"offset %s should reject", tc.offset)
			}
		})
	}
}

// setupIcebergTestServer returns an auth-enabled test server (verifier wired
// via WithAuth) plus its access_key/secret_key pair. Iceberg routes go
// through the SigV4 verifier on this harness.
func setupIcebergTestServer(t *testing.T) (base, ak, sk string) {
	t.Helper()
	base = setupAuthServer(t) // existing helper in auth_test.go; uses WithAuth(testkey/testsecret)
	return base, "testkey", "testsecret"
}

// TestIcebergRoute_NoAuth_Returns401: POST /iceberg/v1/warehouses with no
// Authorization header → 401 + Iceberg JSON ErrorModel.
func TestIcebergRoute_NoAuth_Returns401(t *testing.T) {
	base, _, _ := setupIcebergTestServer(t)

	resp, err := http.Post(base+"/iceberg/v1/warehouses", "application/json", strings.NewReader(`{}`))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, "expected 401")
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")

	var body struct {
		Error struct {
			Type    string `json:"type"`
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	raw, _ := io.ReadAll(resp.Body)
	require.NoError(t, json.Unmarshal(raw, &body), "body: %s", raw)
	assert.Equal(t, "NotAuthorizedException", body.Error.Type)
	assert.Equal(t, 401, body.Error.Code)
}

// TestIcebergRoute_InvalidSignature_Returns401: a request signed with the
// wrong secret is rejected.
func TestIcebergRoute_InvalidSignature_Returns401(t *testing.T) {
	base, ak, _ := setupIcebergTestServer(t)

	req, err := http.NewRequest(http.MethodPost, base+"/iceberg/v1/warehouses", strings.NewReader(`{}`))
	require.NoError(t, err)
	s3auth.SignRequest(req, ak, "WRONG_SECRET", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.NotContains(t, string(body), "WRONG_SECRET", "secret material must not leak")
}

// TestIcebergConfig_RequiresSigV4: /v1/config also requires auth — there is
// no anonymous discovery bypass.
func TestIcebergConfig_RequiresSigV4(t *testing.T) {
	base, _, _ := setupIcebergTestServer(t)
	resp, err := http.Get(base + "/iceberg/v1/config?warehouse=warehouse")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// TestIcebergError_JSON_Shape_OnAuthFailure: response body is a valid Iceberg REST ErrorModel.
func TestIcebergError_JSON_Shape_OnAuthFailure(t *testing.T) {
	base, _, _ := setupIcebergTestServer(t)
	resp, err := http.Post(base+"/iceberg/v1/warehouses", "application/json", strings.NewReader(`{}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")
	var env struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    int    `json:"code"`
		} `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	assert.Equal(t, "NotAuthorizedException", env.Error.Type)
	assert.Equal(t, 401, env.Error.Code)
	assert.NotEmpty(t, env.Error.Message)
}

// TestIcebergRoute_ExpiredSignature_Returns401: X-Amz-Date outside the ±15 min
// skew window is rejected at the iceberg branch of authMiddleware, before the
// SigV4 verifier runs (header-path s3auth.Verify does not enforce skew on its
// own; the iceberg branch tightens that here).
func TestIcebergRoute_ExpiredSignature_Returns401(t *testing.T) {
	base, ak, sk := setupIcebergTestServer(t)

	req, err := http.NewRequest(http.MethodPost, base+"/iceberg/v1/warehouses", strings.NewReader(`{}`))
	require.NoError(t, err)
	s3auth.SignRequestAt(req, ak, sk, "us-east-1", time.Now().UTC().Add(-30*time.Minute))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")
}

// TestIcebergRoute_ValidSigV4_Passes: a properly signed GET /iceberg/v1/config
// with the bootstrapped key passes the auth gate (not 401, not 403). The
// handler's specific response status (200, 501, etc.) is downstream-of-auth
// and intentionally not asserted here.
func TestIcebergRoute_ValidSigV4_Passes(t *testing.T) {
	base, ak, sk := setupIcebergTestServer(t)

	req, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/config?warehouse=warehouse", nil)
	require.NoError(t, err)
	s3auth.SignRequest(req, ak, sk, "us-east-1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.NotEqual(t, http.StatusUnauthorized, resp.StatusCode, "auth gate should pass")
	assert.NotEqual(t, http.StatusForbidden, resp.StatusCode, "auth gate should pass")
}

// TestIcebergRoute_BearerHeader_Returns401: an `Authorization: Bearer <token>`
// header is NOT silently accepted by the iceberg branch. Boundary guard against
// a future OAuth2 PR silently bypassing the SigV4 gate.
func TestIcebergRoute_BearerHeader_Returns401(t *testing.T) {
	base, _, _ := setupIcebergTestServer(t)

	req, err := http.NewRequest(http.MethodPost, base+"/iceberg/v1/warehouses", strings.NewReader(`{}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer some-fake-token")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")
	var env struct {
		Error struct {
			Type string `json:"type"`
			Code int    `json:"code"`
		} `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	assert.Equal(t, "NotAuthorizedException", env.Error.Type)
}

// setupIcebergTestServerNoCache builds a server backed by a CachingVerifier
// with TTL=0 (effectively no caching: same-instant expiresAt fails the
// `time.Now().Before(expiresAt)` check), wired to a closure-backed credential
// store the test can mutate mid-test to simulate KeyRevoke. The no-cache wiring
// is the spec §5.1 #9 deterministic-by-design requirement — CachingVerifier's
// production TTL means a revoked key may transiently pass until the cached
// entry ages out; that staleness behavior is documented and out of scope here.
func setupIcebergTestServerNoCache(t *testing.T) (base, ak, sk string, revoke func()) {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	const accessKey = "noCacheKey"
	const secretKey = "noCacheSecret"

	var mu sync.RWMutex
	live := map[string]string{accessKey: secretKey}

	inner := s3auth.NewVerifier(nil)
	inner.SecretLookup = func(k string) (string, bool) {
		mu.RLock()
		defer mu.RUnlock()
		s, ok := live[k]
		return s, ok
	}
	verifier := s3auth.NewCachingVerifier(inner, 4096, 0) // TTL=0 → no caching

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend, WithVerifier(verifier))
	go srv.Run()
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	revoke = func() {
		mu.Lock()
		delete(live, accessKey)
		mu.Unlock()
	}
	return "http://" + addr, accessKey, secretKey, revoke
}

// TestIcebergRoute_RevokedKey_Returns401: a properly signed request with a
// revoked access key is rejected. Verifier runs with caching disabled (TTL=0)
// so the assertion is deterministic — see spec §5.1 #9 footnote on
// CachingVerifier TTL staleness.
func TestIcebergRoute_RevokedKey_Returns401(t *testing.T) {
	base, ak, sk, revoke := setupIcebergTestServerNoCache(t)

	// Sanity: before revoke, signed request passes the auth gate.
	req, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/config?warehouse=warehouse", nil)
	require.NoError(t, err)
	s3auth.SignRequest(req, ak, sk, "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.NotEqual(t, http.StatusUnauthorized, resp.StatusCode, "pre-revoke should not be 401")

	// Revoke the key, then a freshly signed request must be 401.
	revoke()

	req2, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/config?warehouse=warehouse", nil)
	require.NoError(t, err)
	s3auth.SignRequest(req2, ak, sk, "us-east-1")
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp2.StatusCode)
	assert.Contains(t, resp2.Header.Get("Content-Type"), "application/json")
}
