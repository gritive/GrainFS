package e2e

import (
	"io"
	"net/http"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Object browser", func() {
	ginkgo.Context("SingleNode", func() {
		ginkgo.It("serves the object browser UI", func() {
			resp, err := http.Get(testServerURL + "/ui/")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer resp.Body.Close()

			gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			html := string(body)

			// Verify it's the object browser, not just a status dashboard
			gomega.Expect(strings.Contains(html, "Object Browser")).To(gomega.BeTrue())

			// Verify volume management tab exists
			gomega.Expect(strings.Contains(html, "Volumes")).To(gomega.BeTrue())
		})
	})
	ginkgo.Context("Cluster4Node", func() {
		ginkgo.It("initializes the shared cluster fixture", func() {
			_ = newSharedClusterS3Target(ginkgo.GinkgoTB())
		})
	})
})
