// Request-ID middleware e2e (auth-redesign §5 T41).
//
// Verifies every public response carries X-GrainFS-Request-Id and that the
// header is dual-written to x-amz-request-id. /metrics is anonymous so no
// SigV4 signing is needed; the middleware runs first in the chain and is
// independent of route surface, so /metrics is a sufficient probe.
package e2e

import (
	"io"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const reqIDHeader = "X-GrainFS-Request-Id"

var _ = ginkgo.Describe("Request ID", func() {
	for _, tc := range []struct {
		name string
		mk   func() s3Target
	}{
		{name: "SingleNode", mk: newSingleNodeS3Target},
		{name: "Cluster4Node", mk: func() s3Target { return newSharedClusterS3Target(ginkgo.GinkgoTB()) }},
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			var tgt s3Target

			ginkgo.BeforeEach(func() {
				tgt = tc.mk()
			})

			runRequestIDCases(func() s3Target { return tgt })
		})
	}
})

func runRequestIDCases(getTgt func() s3Target) {
	ginkgo.It("generates a request ID when absent", func() {
		endpoint := getTgt().endpoint(0)
		req, err := http.NewRequest(http.MethodGet, endpoint+"/metrics", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		resp, err := http.DefaultClient.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		})

		rid := resp.Header.Get(reqIDHeader)
		gomega.Expect(rid).NotTo(gomega.BeEmpty(), "server must generate X-GrainFS-Request-Id when absent")
		parsed, err := uuid.Parse(rid)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "generated rid must be a valid UUID")
		gomega.Expect(parsed.Version()).To(gomega.Equal(uuid.Version(7)), "generated rid must be UUIDv7")

		// Dual-write to x-amz-request-id must match.
		gomega.Expect(resp.Header.Get("x-amz-request-id")).To(gomega.Equal(rid),
			"x-amz-request-id must mirror X-GrainFS-Request-Id")
	})

	ginkgo.It("preserves an incoming request ID", func() {
		endpoint := getTgt().endpoint(0)
		const incoming = "client-rid-e2e-12345"
		req, err := http.NewRequest(http.MethodGet, endpoint+"/metrics", nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		req.Header.Set(reqIDHeader, incoming)

		resp, err := http.DefaultClient.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		})

		gomega.Expect(resp.Header.Get(reqIDHeader)).To(gomega.Equal(incoming),
			"server must preserve client-supplied X-GrainFS-Request-Id")
		gomega.Expect(resp.Header.Get("x-amz-request-id")).To(gomega.Equal(incoming),
			"x-amz-request-id must mirror the preserved rid")
	})

	ginkgo.It("generates a unique ID per request", func() {
		t := ginkgo.GinkgoTB()
		endpoint := getTgt().endpoint(0)
		// Two back-to-back unmarked requests must receive distinct rids.
		first := newRequestIDProbe(t, endpoint)
		second := newRequestIDProbe(t, endpoint)
		gomega.Expect(second).NotTo(gomega.Equal(first), "consecutive generated rids must differ")
	})
}

func newRequestIDProbe(t testing.TB, endpoint string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, endpoint+"/metrics", nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	resp, err := http.DefaultClient.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	})
	rid := resp.Header.Get(reqIDHeader)
	gomega.Expect(rid).NotTo(gomega.BeEmpty())
	return rid
}
