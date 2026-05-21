package e2e

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Iceberg auth", func() {
	ginkgo.Context("NoAuthRejected", func() {
		ginkgo.It("rejects unsigned namespace requests on a single node", func() {
			runIcebergAuthNoAuthRejectedSingleNode(ginkgo.GinkgoTB())
		})
	})

	ginkgo.Context("AfterBootstrapAccepts", func() {
		ginkgo.It("accepts signed Iceberg requests after bootstrap on a single node", func() {
			runIcebergAuthAfterBootstrapAcceptsSingleNode(ginkgo.GinkgoTB())
		})
	})

	ginkgo.Context("AuthFailuresRejected", func() {
		ginkgo.It("rejects repeated unsigned Iceberg requests on a single node", func() {
			runIcebergAuthFailuresRejectedSingleNode(ginkgo.GinkgoTB())
		})
	})
})

func runIcebergAuthNoAuthRejectedSingleNode(t testing.TB) {
	t.Helper()
	srv := startIAMTestServer(t)
	ginkgo.DeferCleanup(srv.Stop)

	resp, err := http.Post(srv.S3URL+"/iceberg/v1/namespaces", "application/json",
		bytes.NewReader([]byte(`{"namespace":["x"]}`)))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(resp.Body.Close)
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusUnauthorized))
}

func runIcebergAuthAfterBootstrapAcceptsSingleNode(t testing.TB) {
	t.Helper()
	srv := startIAMTestServer(t)
	ginkgo.DeferCleanup(srv.Stop)

	client := newIcebergSigV4Client(t, srv.BootstrapAK, srv.BootstrapSK, "us-east-1")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		srv.S3URL+"/iceberg/v1/config?warehouse=warehouse", nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	resp, err := client.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(resp.Body.Close)
	gomega.Expect(resp.StatusCode).NotTo(gomega.Equal(http.StatusUnauthorized),
		"signed request should pass authn (got 401)")
	gomega.Expect(resp.StatusCode).NotTo(gomega.Equal(http.StatusForbidden),
		"signed request should not be 403 either")
}

func runIcebergAuthFailuresRejectedSingleNode(t testing.TB) {
	t.Helper()
	srv := startIAMTestServer(t)
	ginkgo.DeferCleanup(srv.Stop)
	for i := 0; i < 3; i++ {
		resp, err := http.Post(srv.S3URL+"/iceberg/v1/warehouses", "application/json",
			bytes.NewReader([]byte(`{}`)))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusUnauthorized))
		_ = resp.Body.Close()
	}
}
