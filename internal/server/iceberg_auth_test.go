package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// TestIcebergRoute_AnonymousModeRemoved_NoBypass asserts that NO iceberg
// route surface entry uses routeAuthnAnonymous. Regression guard: any future
// commit that flips an iceberg prefix back to anonymous fails CI.
func TestIcebergRoute_AnonymousModeRemoved_NoBypass(t *testing.T) {
	for _, entry := range routeSurfaceManifest {
		if entry.surface == routeSurfaceIceberg {
			assert.NotEqualf(t, routeAuthnAnonymous, entry.authn,
				"iceberg route %q/%q must not be anonymous",
				entry.pathPrefix, entry.pathExact)
		}
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
