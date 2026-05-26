package e2e

import (
	"io"
	"net/http"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Object browser", func() {
	ginkgo.It("serves the object browser UI", func() {
		tgt := newSingleNodeS3Target()
		resp, err := http.Get(tgt.endpoint(0) + "/ui/")
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
