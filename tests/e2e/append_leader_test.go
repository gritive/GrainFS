package e2e

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Append leader helpers", func() {
	ginkgo.Context("SingleNode", func() {
		ginkgo.It("finds the single-group owner", func() {
			gomega.Expect(findOwnerForSingleGroup(nil)).To(gomega.Equal(-1))
			gomega.Expect(findOwnerForSingleGroup(&e2eCluster{leaderIdx: 2})).To(gomega.Equal(2))
		})
	})
})
