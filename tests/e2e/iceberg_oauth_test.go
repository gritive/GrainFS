package e2e

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

// Iceberg OAuth specs exercise the §4 OAuth2 client_credentials token endpoint
// (POST /iceberg/v1/oauth/tokens) and adjacent SigV4 access on the warehouse
// bucket. Dual-target: SingleNode + Cluster3Node, per R10 convention.
//
// Cases:
//   - S3SigV4_NoBearerNeeded_PutGetRoundtrip — SigV4-authenticated PUT/GET on
//     a warehouse bucket succeeds without ever minting a bearer, proving the
//     S3 plane is not bearer-gated for SigV4 callers.
//   - MintToken_HappyPath — POST grant_type=client_credentials with a valid
//     ak/sk pair + PRINCIPAL_ROLE:<warehouse> scope; assert 3-segment JWT.
//   - MintToken_WrongSecret_401 — same flow with a wrong secret; assert 401
//     and empty token.
//   - PostMintToken_SigV4StillWorks — SA that minted a bearer must still
//     PUT/GET on the warehouse bucket via SigV4 (the bearer is the iceberg-side
//     credential; S3-side is SigV4 throughout). Proves minting a bearer does
//     not break the SigV4 path — the test never sends the bearer.
var _ = ginkgo.Describe("Iceberg OAuth", func() {
	describeIcebergOAuthContext("SingleNode", func(t testing.TB) *icebergTarget {
		return newSingleNodeIcebergTarget(t)
	})

	describeIcebergOAuthContext("Cluster3Node", func(t testing.TB) *icebergTarget {
		return newSharedClusterIcebergTarget(t)
	})
})

func describeIcebergOAuthContext(name string, factory func(testing.TB) *icebergTarget) {
	ginkgo.Context(name, func() {
		var tgt *icebergTarget

		ginkgo.BeforeEach(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		ginkgo.It("allows SigV4 S3 round trips without bearer tokens", func() {
			runIcebergOAuthS3SigV4NoBearerNeededPutGetRoundtrip(ginkgo.GinkgoTB(), tgt)
		})

		ginkgo.It("mints bearer tokens with valid client credentials", func() {
			runIcebergOAuthMintTokenHappyPath(ginkgo.GinkgoTB(), tgt)
		})

		ginkgo.It("rejects token minting with a wrong client secret", func() {
			runIcebergOAuthMintTokenWrongSecret401(ginkgo.GinkgoTB(), tgt)
		})

		ginkgo.It("keeps SigV4 S3 access working after minting a bearer token", func() {
			runIcebergOAuthPostMintTokenSigV4StillWorks(ginkgo.GinkgoTB(), tgt)
		})
	})
}

// runIcebergOAuthS3SigV4NoBearerNeededPutGetRoundtrip asserts that a
// SigV4-authenticated S3 PUT/GET succeeds on a warehouse bucket without ever
// minting a bearer token. This isolates the SigV4 path: bearer is not
// required for S3 access when SigV4 succeeds.
//
// The spec-original "AnonPhase0" anon-PUT-to-default-bucket assertion is
// covered separately by Task 71 (Phase 0 magical-moment quickstart) which
// boots a fresh iam.anon-enabled=true fixture; this case does not duplicate
// that scope.
func runIcebergOAuthS3SigV4NoBearerNeededPutGetRoundtrip(t testing.TB, tgt *icebergTarget) {
	t.Helper()
	bucket := tgt.uniqueWarehouse(t, "sigv4-nobearer")
	key := "sigv4/hello.txt"
	body := []byte("sigv4-no-bearer-needed")
	ctx := context.Background()
	_, err := tgt.s3Client(0).PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	require.NoError(t, err, "SigV4 PutObject must succeed without minting a bearer")

	out, err := tgt.s3Client(0).GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "SigV4 GetObject must succeed without minting a bearer")
	ginkgo.DeferCleanup(out.Body.Close)
	got, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

// runIcebergOAuthMintTokenHappyPath mints a bearer for a fresh SA with
// readwrite policy attached, scoped to PRINCIPAL_ROLE:<warehouse>. The
// returned JWT must be a 3-segment compact-serialization HS256 token.
func runIcebergOAuthMintTokenHappyPath(t testing.TB, tgt *icebergTarget) {
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
func runIcebergOAuthMintTokenWrongSecret401(t testing.TB, tgt *icebergTarget) {
	t.Helper()
	warehouse := tgt.uniqueWarehouse(t, "wrongsec")
	saID, ak, _ := tgt.adminCreateSA(t, "wrongsec")
	tgt.adminAttachPolicy(t, saID, "readwrite")

	jwt, status := tgt.mintToken(t, ak, "WRONG-SECRET", warehouse)
	require.Equal(t, http.StatusUnauthorized, status, "wrong secret must yield 401")
	require.Empty(t, jwt, "wrong-secret response must not include a JWT")
}

// runIcebergOAuthPostMintTokenSigV4StillWorks confirms that an SA which has
// just minted a bearer can still PUT/GET on its warehouse bucket via SigV4
// (S3 side is SigV4 throughout — the bearer is iceberg-side only). The
// bearer is intentionally never sent; the test value is proving that the
// act of minting does not break the SigV4 path.
func runIcebergOAuthPostMintTokenSigV4StillWorks(t testing.TB, tgt *icebergTarget) {
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
		Body:   bytes.NewReader(body),
	})
	require.NoError(t, err, "post-mint SigV4 PutObject must succeed")

	out, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(warehouse),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "post-mint SigV4 GetObject must succeed")
	ginkgo.DeferCleanup(out.Body.Close)
	got, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	require.Equal(t, body, got)
}
