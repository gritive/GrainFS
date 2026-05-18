package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam"
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
