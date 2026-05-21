package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// The Iceberg client shape specs pin the wire-level JSON response shape that
// the DuckDB iceberg extension parses (see duckdb/duckdb_iceberg#18483).
//
// DuckDB's iceberg extension is strict about the OAuth2 token response:
// it requires token_type == "bearer" (lowercase), even though RFC 6749 §5.1
// says token_type is case-insensitive. We pin the lowercase emission so a
// future regression doesn't silently break DuckDB clients.
//
// Cases:
//   - TokenResponseLowercaseBearer — POST /iceberg/v1/oauth/tokens emits
//     {"token_type":"bearer", ...} in lowercase form.
//   - CreateSecretParameterCompatibility — the OAUTH2_SERVER_URI path that
//     DuckDB's CREATE SECRET passes verbatim (catalog_base + /v1/oauth/tokens)
//     accepts form-encoded client_credentials and returns a 3-segment JWT.
var _ = ginkgo.Describe("Iceberg client response shape", func() {
	describeIcebergClientShapeContext("SingleNode", func(t testing.TB) *icebergTarget {
		return newSingleNodeIcebergTarget(t)
	})

	describeIcebergClientShapeContext("Cluster3Node", func(t testing.TB) *icebergTarget {
		return newSharedClusterIcebergTarget(t)
	})
})

func describeIcebergClientShapeContext(name string, factory func(testing.TB) *icebergTarget) {
	ginkgo.Context(name, func() {
		var tgt *icebergTarget

		ginkgo.BeforeEach(func() {
			tgt = factory(ginkgo.GinkgoTB())
		})

		ginkgo.It("emits lowercase bearer token responses", func() {
			runIcebergClientShapeTokenResponseLowercaseBearer(ginkgo.GinkgoTB(), tgt)
		})

		ginkgo.It("accepts DuckDB CREATE SECRET token parameters", func() {
			runIcebergClientShapeCreateSecretParameterCompatibility(ginkgo.GinkgoTB(), tgt)
		})
	})
}

// runIcebergClientShapeTokenResponseLowercaseBearer hits the OAuth token
// endpoint directly (bypassing mintToken so we can inspect token_type) and
// asserts the response body has token_type:"bearer" verbatim in lowercase.
// DuckDB iceberg extension issue duckdb/duckdb_iceberg#18483 documents the
// strict lowercase requirement.
func runIcebergClientShapeTokenResponseLowercaseBearer(t testing.TB, tgt *icebergTarget) {
	t.Helper()
	wh := tgt.uniqueWarehouse(t, "bearershape")
	saID, ak, sk := tgt.adminCreateSA(t, "bearer")
	tgt.adminAttachPolicy(t, saID, "readonly")

	form := url.Values{
		"grant_type":    []string{"client_credentials"},
		"client_id":     []string{ak},
		"client_secret": []string{sk},
		"scope":         []string{"PRINCIPAL_ROLE:" + wh},
	}
	endpoint := tgt.endpoint(0) + "/iceberg/v1/oauth/tokens"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		endpoint, strings.NewReader(form.Encode()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(resp.Body.Close)
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))

	var body struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}
	gomega.Expect(json.NewDecoder(resp.Body).Decode(&body)).To(gomega.Succeed())
	gomega.Expect(body.AccessToken).NotTo(gomega.BeEmpty(), "access_token must be non-empty")
	gomega.Expect(body.TokenType).To(gomega.Equal("bearer"),
		"DuckDB duckdb_iceberg#18483: token_type MUST be lowercase \"bearer\"")
	gomega.Expect(body.ExpiresIn).To(gomega.BeNumerically(">", 0), "expires_in must be positive")
}

// runIcebergClientShapeCreateSecretParameterCompatibility verifies the
// OAUTH2_SERVER_URI path that DuckDB's CREATE SECRET passes verbatim — the
// server must accept form-encoded client_credentials at exactly the
// /iceberg/v1/oauth/tokens path and return a compact-serialization JWT.
func runIcebergClientShapeCreateSecretParameterCompatibility(t testing.TB, tgt *icebergTarget) {
	t.Helper()
	wh := tgt.uniqueWarehouse(t, "createsec")
	saID, ak, sk := tgt.adminCreateSA(t, "createsec")
	tgt.adminAttachPolicy(t, saID, "readwrite")

	jwt, status := tgt.mintToken(t, ak, sk, wh)
	gomega.Expect(status).To(gomega.Equal(http.StatusOK), "CREATE SECRET-compatible token URL must mint 200")
	gomega.Expect(jwt).NotTo(gomega.BeEmpty(), "CREATE SECRET-compatible token URL must return a JWT")
	gomega.Expect(strings.Count(jwt, ".")).To(gomega.Equal(2),
		"JWT must be exactly 3 base64url segments separated by dots: %q", jwt)
}
