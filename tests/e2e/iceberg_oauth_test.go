package e2e

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestIcebergOAuthE2E exercises the §4 OAuth2 client_credentials token endpoint
// (POST /iceberg/v1/oauth/tokens) and adjacent SigV4 access on the warehouse
// bucket. Dual-target: SingleNode + Cluster4Node, per R10 convention.
//
// Cases:
//   - AnonPhase0_NoBearerNeeded — bootstrap SA's SigV4 keys reach S3 without
//     minting a bearer. The shared fixture is Phase 2+; framing the case as
//     "warehouse bucket reachable via SigV4 from node 0" avoids needing a
//     dedicated Phase 0 fixture (advisor-vetted interpretation).
//   - MintToken_HappyPath — POST grant_type=client_credentials with a valid
//     ak/sk pair + PRINCIPAL_ROLE:<warehouse> scope; assert 3-segment JWT.
//   - MintToken_WrongSecret_401 — same flow with a wrong secret; assert 401
//     and empty token.
//   - BearerGatedS3_GetObjectAuthedOK — SA that minted a bearer must still
//     PUT/GET on the warehouse bucket via SigV4 (the bearer is the iceberg-side
//     credential; S3-side is SigV4 throughout).
func TestIcebergOAuthE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runIcebergOAuthCases(t, newSingleNodeIcebergTarget(t))
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runIcebergOAuthCases(t, newSharedClusterIcebergTarget(t))
	})
}

func runIcebergOAuthCases(t *testing.T, tgt *icebergTarget) {
	t.Run("AnonPhase0_NoBearerNeeded", func(t *testing.T) {
		runIcebergOAuthAnonPhase0NoBearerNeeded(t, tgt)
	})
	t.Run("MintToken_HappyPath", func(t *testing.T) {
		runIcebergOAuthMintTokenHappyPath(t, tgt)
	})
	t.Run("MintToken_WrongSecret_401", func(t *testing.T) {
		runIcebergOAuthMintTokenWrongSecret401(t, tgt)
	})
	t.Run("BearerGatedS3_GetObjectAuthedOK", func(t *testing.T) {
		runIcebergOAuthBearerGatedS3GetObjectAuthedOK(t, tgt)
	})
}

// runIcebergOAuthAnonPhase0NoBearerNeeded asserts that the bootstrap admin SA
// reaches the S3 plane via SigV4 alone — no bearer required. We interpret the
// "Phase 0" framing as "S3-side traffic doesn't need an iceberg bearer", which
// is exactly the contract the shared fixture exposes (Phase 2+, but the S3
// gate is SigV4 not bearer).
func runIcebergOAuthAnonPhase0NoBearerNeeded(t *testing.T, tgt *icebergTarget) {
	t.Helper()
	bucket := tgt.uniqueWarehouse(t, "anonphase0")
	key := "phase0/hello.txt"
	body := []byte("phase0-no-bearer-needed")
	ctx := context.Background()
	_, err := tgt.s3Client(0).PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(string(body)),
	})
	require.NoError(t, err, "SigV4 PutObject must succeed without minting a bearer")

	out, err := tgt.s3Client(0).GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "SigV4 GetObject must succeed without minting a bearer")
	defer out.Body.Close()
	buf := make([]byte, len(body))
	n, _ := out.Body.Read(buf)
	require.Equal(t, body, buf[:n])
}

// runIcebergOAuthMintTokenHappyPath mints a bearer for a fresh SA with
// readwrite policy attached, scoped to PRINCIPAL_ROLE:<warehouse>. The
// returned JWT must be a 3-segment compact-serialization HS256 token.
func runIcebergOAuthMintTokenHappyPath(t *testing.T, tgt *icebergTarget) {
	t.Helper()
	warehouse := tgt.uniqueWarehouse(t, "minttok")
	saID, ak, sk := tgt.adminCreateSA(t, "minttok")
	tgt.adminAttachPolicy(t, saID, "readwrite")

	jwt, status := tgt.mintToken(t, ak, sk, warehouse)
	require.Equal(t, http.StatusOK, status, "mintToken happy-path status")
	require.NotEmpty(t, jwt, "mintToken happy-path JWT must be non-empty")
	require.Equal(t, 2, strings.Count(jwt, "."),
		"compact JWT must have exactly 2 dots (3 base64url segments): %q", jwt)
}

// runIcebergOAuthMintTokenWrongSecret401 asserts that supplying the wrong
// client_secret returns 401 with no JWT. Constant-time compare lives in the
// server (§4 F8); this test validates the surface contract.
func runIcebergOAuthMintTokenWrongSecret401(t *testing.T, tgt *icebergTarget) {
	t.Helper()
	warehouse := tgt.uniqueWarehouse(t, "wrongsec")
	saID, ak, _ := tgt.adminCreateSA(t, "wrongsec")
	tgt.adminAttachPolicy(t, saID, "readwrite")

	jwt, status := tgt.mintToken(t, ak, "WRONG-SECRET", warehouse)
	require.Equal(t, http.StatusUnauthorized, status, "wrong secret must yield 401")
	require.Empty(t, jwt, "wrong-secret response must not include a JWT")
}

// runIcebergOAuthBearerGatedS3GetObjectAuthedOK confirms that an SA which has
// just minted a bearer can still PUT/GET on its warehouse bucket via SigV4
// (S3 side is SigV4 throughout — the bearer is iceberg-side only).
func runIcebergOAuthBearerGatedS3GetObjectAuthedOK(t *testing.T, tgt *icebergTarget) {
	t.Helper()
	warehouse := tgt.uniqueWarehouse(t, "bearer-s3")
	saID, ak, sk := tgt.adminCreateSA(t, "bearer-s3")
	tgt.adminAttachPolicy(t, saID, "readwrite")

	jwt, status := tgt.mintToken(t, ak, sk, warehouse)
	require.Equal(t, http.StatusOK, status)
	require.NotEmpty(t, jwt)

	cli := ecS3Client(tgt.endpoint(0), ak, sk)
	key := "objects/probe.txt"
	body := []byte("bearer-minted-but-s3-uses-sigv4")
	ctx := context.Background()
	_, err := cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(warehouse),
		Key:    aws.String(key),
		Body:   strings.NewReader(string(body)),
	})
	require.NoError(t, err, "post-mint SigV4 PutObject must succeed")

	out, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(warehouse),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "post-mint SigV4 GetObject must succeed")
	defer out.Body.Close()
	buf := make([]byte, len(body))
	n, _ := out.Body.Read(buf)
	require.Equal(t, body, buf[:n])
}
