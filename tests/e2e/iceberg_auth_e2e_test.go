package e2e

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

var _ = ginkgo.Describe("Iceberg auth", func() {
	ginkgo.Context("NoAuthRejected", func() {
		ginkgo.It("rejects unsigned namespace requests on a single node", func() {
			runIcebergAuthNoAuthRejectedSingleNode(ginkgo.GinkgoTB())
		})

		ginkgo.It("boots the cluster auth fixture", func() {
			runIcebergAuthClusterFixture(ginkgo.GinkgoTB())
		})
	})

	ginkgo.Context("AfterBootstrapAccepts", func() {
		ginkgo.It("accepts signed Iceberg requests after bootstrap on a single node", func() {
			runIcebergAuthAfterBootstrapAcceptsSingleNode(ginkgo.GinkgoTB())
		})

		ginkgo.It("boots the cluster auth fixture", func() {
			runIcebergAuthClusterFixture(ginkgo.GinkgoTB())
		})
	})

	ginkgo.Context("AuthFailuresRejected", func() {
		ginkgo.It("rejects repeated unsigned Iceberg requests on a single node", func() {
			runIcebergAuthFailuresRejectedSingleNode(ginkgo.GinkgoTB())
		})

		ginkgo.It("boots the cluster auth fixture", func() {
			runIcebergAuthClusterFixture(ginkgo.GinkgoTB())
		})
	})
})

func runIcebergAuthNoAuthRejectedSingleNode(t testing.TB) {
	t.Helper()
	srv := startIAMTestServer(t)
	ginkgo.DeferCleanup(srv.Stop)

	resp, err := http.Post(srv.S3URL+"/iceberg/v1/namespaces", "application/json",
		bytes.NewReader([]byte(`{"namespace":["x"]}`)))
	require.NoError(t, err)
	ginkgo.DeferCleanup(resp.Body.Close)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func runIcebergAuthAfterBootstrapAcceptsSingleNode(t testing.TB) {
	t.Helper()
	srv := startIAMTestServer(t)
	ginkgo.DeferCleanup(srv.Stop)

	client := newIcebergSigV4Client(t, srv.BootstrapAK, srv.BootstrapSK, "us-east-1")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		srv.S3URL+"/iceberg/v1/config?warehouse=warehouse", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	ginkgo.DeferCleanup(resp.Body.Close)
	require.NotEqual(t, http.StatusUnauthorized, resp.StatusCode,
		"signed request should pass authn (got 401)")
	require.NotEqual(t, http.StatusForbidden, resp.StatusCode,
		"signed request should not be 403 either")
}

func runIcebergAuthFailuresRejectedSingleNode(t testing.TB) {
	t.Helper()
	srv := startIAMTestServer(t)
	ginkgo.DeferCleanup(srv.Stop)
	for i := 0; i < 3; i++ {
		resp, err := http.Post(srv.S3URL+"/iceberg/v1/warehouses", "application/json",
			bytes.NewReader([]byte(`{}`)))
		require.NoError(t, err)
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		_ = resp.Body.Close()
	}
}

func runIcebergAuthClusterFixture(t testing.TB) {
	t.Helper()
	_ = newSharedClusterS3Target(t)
}
