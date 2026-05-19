package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/s3auth"
)

// TestIcebergS3CredOverrides_CallerIdentity verifies that /v1/config publishes
// the caller's *own* access/secret pair — not "whichever SA has the highest
// grant on the bucket" — to prevent privilege amplification via the
// REST catalog. A RoleRead caller authenticating the catalog must walk away
// with RoleRead-class data-plane credentials, not the RoleAdmin SA's keys.
func TestIcebergS3CredOverrides_CallerIdentity(t *testing.T) {
	h := newIAMTestHelper(t)

	// Two SAs with distinct authority on the warehouse bucket. Pre-Option B,
	// FirstActiveKeyForBucketGrant would have always returned sa-admin's
	// keys regardless of who called. Post-Option B, each caller gets their
	// own credentials back.
	h.applySACreate(t, "sa-reader")
	h.applyGrantPut(t, "sa-reader", "grainfs-tables", iam.RoleRead)
	h.applyKeyCreate(t, "ak-reader", "sa-reader", "sk-reader")

	h.applySACreate(t, "sa-admin")
	h.applyGrantPut(t, "sa-admin", "grainfs-tables", iam.RoleAdmin)
	h.applyKeyCreate(t, "ak-admin", "sa-admin", "sk-admin")

	// SA with no grant on the warehouse bucket — must get empty overrides.
	h.applySACreate(t, "sa-stranger")
	h.applyKeyCreate(t, "ak-stranger", "sa-stranger", "sk-stranger")

	s := &Server{iamStore: h.store}
	warehouse := "s3://grainfs-tables/warehouse"

	t.Run("RoleRead caller gets own creds", func(t *testing.T) {
		ctx := WithAccessKey(context.Background(), "ak-reader")
		got := s.icebergS3CredOverrides(ctx, warehouse)
		require.Equal(t, "ak-reader", got["s3.access-key-id"],
			"caller must receive their own ak, not the admin SA's")
		require.Equal(t, "sk-reader", got["s3.secret-access-key"])
		require.Equal(t, "true", got["s3.path-style-access"])
	})

	t.Run("RoleAdmin caller gets own creds", func(t *testing.T) {
		ctx := WithAccessKey(context.Background(), "ak-admin")
		got := s.icebergS3CredOverrides(ctx, warehouse)
		require.Equal(t, "ak-admin", got["s3.access-key-id"])
		require.Equal(t, "sk-admin", got["s3.secret-access-key"])
	})

	t.Run("no grant on bucket means empty overrides", func(t *testing.T) {
		ctx := WithAccessKey(context.Background(), "ak-stranger")
		got := s.icebergS3CredOverrides(ctx, warehouse)
		require.Empty(t, got,
			"caller without RoleRead on the warehouse bucket must not get creds back")
	})

	t.Run("no caller identity means empty overrides", func(t *testing.T) {
		// The authn middleware should populate AccessKey on the iceberg
		// routes, but defend against accidental wiring regressions.
		got := s.icebergS3CredOverrides(context.Background(), warehouse)
		require.Empty(t, got)
	})

	t.Run("unknown access key means empty overrides", func(t *testing.T) {
		ctx := WithAccessKey(context.Background(), "ak-never-existed")
		got := s.icebergS3CredOverrides(ctx, warehouse)
		require.Empty(t, got)
	})

	t.Run("malformed warehouse means empty overrides", func(t *testing.T) {
		ctx := WithAccessKey(context.Background(), "ak-admin")
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
	h := newIAMTestHelper(t)
	h.applySACreate(t, "sa-bench")
	h.applyGrantWildcardPut(t, "sa-bench", iam.RoleAdmin)
	h.applyKeyCreate(t, "AK-bench", "sa-bench", "SK-bench")

	base := setupTestServerWithOptions(t,
		WithIAMStore(h.store),
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
	require.NotEmpty(t, got.Overrides["s3.endpoint"], "creds path engaged, s3.endpoint must be set")
	require.True(t, strings.HasPrefix(got.Overrides["s3.endpoint"], "http://"),
		"test server is HTTP, s3.endpoint must mirror that, got %q", got.Overrides["s3.endpoint"])
	require.NotContains(t, got.Overrides["s3.endpoint"], "https://",
		"plaintext test server must not be advertised as https")

	// Sanity: caller-identity is propagated end-to-end through the handler too.
	require.Equal(t, "AK-bench", got.Overrides["s3.access-key-id"])
	require.Equal(t, "SK-bench", got.Overrides["s3.secret-access-key"])
	require.Equal(t, "true", got.Overrides["s3.path-style-access"])
}

// TestIcebergS3CredOverrides_WildcardGrant verifies wildcard (cross-bucket)
// grants satisfy the RoleRead-on-warehouse check.
func TestIcebergS3CredOverrides_WildcardGrant(t *testing.T) {
	h := newIAMTestHelper(t)

	h.applySACreate(t, "sa-wild")
	h.applyGrantWildcardPut(t, "sa-wild", iam.RoleRead)
	h.applyKeyCreate(t, "ak-wild", "sa-wild", "sk-wild")

	s := &Server{iamStore: h.store}
	ctx := WithAccessKey(context.Background(), "ak-wild")

	got := s.icebergS3CredOverrides(ctx, "s3://grainfs-tables/warehouse")
	require.Equal(t, "ak-wild", got["s3.access-key-id"])
	require.Equal(t, "sk-wild", got["s3.secret-access-key"])
}
