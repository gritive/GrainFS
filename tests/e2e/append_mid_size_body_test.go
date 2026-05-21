package e2e

import (
	"bytes"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
)

// TestAppendMidSizeBodyE2E proves AppendObject accepts 8 MiB bodies after
// DefaultMaxForwardBodyBytes was raised from 5 MiB to 64 MiB (matching the
// HTTP layer appendBodyMaxBytes). See design 2026-05-19 § Follow-up 3.
func runAppendMidSizeBodySpecs() {
	ginkgo.Context("MidSizeBody SingleNode", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newSingleNodeS3Target()
		})
		ginkgo.It("accepts an 8MiB append body", func() {
			runMidSizeAppendCase(ginkgo.GinkgoTB(), tgt)
		})
	})
	ginkgo.Context("MidSizeBody Cluster4Node", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newSharedClusterS3Target(ginkgo.GinkgoTB())
		})
		ginkgo.It("accepts an 8MiB append body", func() {
			runMidSizeAppendCase(ginkgo.GinkgoTB(), tgt)
		})
	})
}

func runMidSizeAppendCase(t testing.TB, tgt s3Target) {
	t.Helper()
	bucket := "append-mid-" + tgt.name
	tgt.createBkt(t, bucket)
	key := "obj-8mib"
	body := bytes.Repeat([]byte("m"), 8*1024*1024)
	require.NoError(t, putAppend(tgt.pickNode(0), bucket, key, 0, body))
	lastNode := tgt.nodes - 1
	if lastNode < 0 {
		lastNode = 0
	}
	got := getObject(t, tgt.pickNode(lastNode), bucket, key)
	require.Equal(t, body, got)
}
