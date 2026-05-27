package e2e

import (
	"net/http"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.AfterSuite(func() {
	closeE2EIdleConnections()
})

var _ = ginkgo.BeforeEach(func() {
	closeE2EIdleConnections()
})

var _ = ginkgo.AfterEach(func() {
	closeE2EIdleConnections()
})

func closeE2EIdleConnections() {
	if transport, ok := http.DefaultTransport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	closeIdleConnections(e2eS3HTTPClient)
	closeIdleConnections(e2eRawHTTPClient)
}

type idleConnectionCloser interface {
	CloseIdleConnections()
}

func closeIdleConnections(client *http.Client) {
	if client == nil || client.Transport == nil {
		return
	}
	if transport, ok := client.Transport.(idleConnectionCloser); ok {
		transport.CloseIdleConnections()
	}
}

func TestE2EGinkgo(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "E2E Ginkgo")
}

var _ = ginkgo.BeforeSuite(func() {
	initSharedS3Targets(ginkgo.GinkgoTB())
})
