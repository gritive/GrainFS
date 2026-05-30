package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// TestIcebergConfigHandler_SchemeReflection drives the actual /v1/config
// handler end-to-end (SigV4 signed) and asserts the published s3.endpoint
// mirrors the request scheme. Direct calls to icebergS3CredOverrides bypass
// the handler's scheme logic, so a refactor that drops the scheme line would
// leave the unit suite green while breaking real clients — this test is the
// regression guard for that path.
func TestIcebergConfigHandler_SchemeReflection(t *testing.T) {
	hh := newIAMTestHelper(t)
	hh.applySACreate(t, "sa-bench")
	hh.applyKeyCreate(t, "AK-bench", "sa-bench", "SK-bench")
	base := setupTestServerWithOptions(t,
		WithIAMStore(hh.store),
		WithAuth([]s3auth.Credentials{{AccessKey: "AK-bench", SecretKey: "SK-bench"}}),
	)

	req, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/config?warehouse=warehouse", nil)
	require.NoError(t, err)
	s3auth.SignRequest(req, "AK-bench", "SK-bench", "us-east-1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var got struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
	}
	require.NoError(t, json.Unmarshal(body, &got))

	// Test server runs on plain HTTP — scheme published in s3.endpoint
	// MUST match. A regression that hardcoded "http://" or "https://"
	// would still pass /v1/config returns-200 tests but break HTTPS
	// callers (downgrade) or HTTP callers (broken handoff).
	require.NotEmpty(t, got.Overrides["s3.endpoint"], "s3.endpoint must be set in overrides")
	require.True(t, strings.HasPrefix(got.Overrides["s3.endpoint"], "http://"),
		"test server is HTTP, s3.endpoint must mirror that, got %q", got.Overrides["s3.endpoint"])
	require.NotContains(t, got.Overrides["s3.endpoint"], "https://",
		"plaintext test server must not be advertised as https")
}

func TestIcebergConfigHandler_HTTPDoesNotPublishS3Secrets(t *testing.T) {
	hh := newIAMTestHelper(t)
	hh.applySACreate(t, "sa-bench")
	hh.applyKeyCreate(t, "AK-bench", "sa-bench", "SK-bench")
	base := setupTestServerWithOptions(t,
		WithIAMStore(hh.store),
		WithAuth([]s3auth.Credentials{{AccessKey: "AK-bench", SecretKey: "SK-bench"}}),
	)

	req, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/config?warehouse=warehouse", nil)
	require.NoError(t, err)
	s3auth.SignRequest(req, "AK-bench", "SK-bench", "us-east-1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var got struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
	}
	require.NoError(t, json.Unmarshal(body, &got))

	require.NotContains(t, got.Overrides, "s3.access-key-id")
	require.NotContains(t, got.Overrides, "s3.secret-access-key")
	require.NotContains(t, got.Overrides, "s3.path-style-access")
	require.True(t, strings.HasPrefix(got.Overrides["s3.endpoint"], "http://"))
}
