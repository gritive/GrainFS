package e2e

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

var _ = ginkgo.Describe("Append leader helpers", func() {
	ginkgo.Context("SingleNode", func() {
		ginkgo.It("finds the single-group owner", func() {
			t := ginkgo.GinkgoTB()
			require.Equal(t, -1, findOwnerForSingleGroup(nil))
			require.Equal(t, 2, findOwnerForSingleGroup(&e2eCluster{leaderIdx: 2}))
		})
	})
})
