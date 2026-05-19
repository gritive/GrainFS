package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// TestIcebergS3CredOverrides_CallerIdentity verifies that /v1/config publishes
// the caller's *own* access/secret pair — not some other SA's keys (privilege
// amplification prevention). Legacy grant-check removed in §2; credential
// forwarding is best-effort for the key that authenticated the request.
func TestIcebergS3CredOverrides_CallerIdentity(t *testing.T) {
	h := newIAMTestHelper(t)

	h.applySACreate(t, "sa-alpha")
	h.applyKeyCreate(t, "ak-alpha", "sa-alpha", "sk-alpha")

	h.applySACreate(t, "sa-beta")
	h.applyKeyCreate(t, "ak-beta", "sa-beta", "sk-beta")

	s := &Server{iamStore: h.store}
	warehouse := "s3://grainfs-tables/warehouse"

	t.Run("alpha caller gets own creds", func(t *testing.T) {
		ctx := WithAccessKey(context.Background(), "ak-alpha")
		got := s.icebergS3CredOverrides(ctx, warehouse)
		require.Equal(t, "ak-alpha", got["s3.access-key-id"],
			"caller must receive their own ak, not another SA's")
		require.Equal(t, "sk-alpha", got["s3.secret-access-key"])
		require.Equal(t, "true", got["s3.path-style-access"])
	})

	t.Run("beta caller gets own creds", func(t *testing.T) {
		ctx := WithAccessKey(context.Background(), "ak-beta")
		got := s.icebergS3CredOverrides(ctx, warehouse)
		require.Equal(t, "ak-beta", got["s3.access-key-id"])
		require.Equal(t, "sk-beta", got["s3.secret-access-key"])
	})

	t.Run("no caller identity means empty overrides", func(t *testing.T) {
		got := s.icebergS3CredOverrides(context.Background(), warehouse)
		require.Empty(t, got)
	})

	t.Run("unknown access key means empty overrides", func(t *testing.T) {
		ctx := WithAccessKey(context.Background(), "ak-never-existed")
		got := s.icebergS3CredOverrides(ctx, warehouse)
		require.Empty(t, got)
	})

	t.Run("malformed warehouse means empty overrides", func(t *testing.T) {
		ctx := WithAccessKey(context.Background(), "ak-alpha")
		got := s.icebergS3CredOverrides(ctx, "not-an-s3-url")
		require.Empty(t, got)
	})
}

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
