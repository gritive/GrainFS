package e2e

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aws/smithy-go"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestAppendSizeCapE2E pins the design § Follow-up 2 contract:
//   - RejectAtCap: append over cap returns 400 EntityTooLarge.
//   - ConcurrentRaceAtCap: concurrent appends near cap — FSM is the
//     single source of truth, exactly the first to fit succeeds; the
//     remainder must surface EntityTooLarge or InvalidWriteOffset.
//
// Both subtests run on a dedicated single-node fixture and on a dedicated
// 4-node cluster. Per-test dedicated single fixture is needed because the
// package-global single fixture cannot accept per-test --append-size-cap-bytes
// without races across tests; newDedicatedSingleNodeS3Target spawns its own
// grainfs process with the cap arg threaded through.
func runAppendSizeCapSpecs() {
	smallCap := int64(4 * 1024)
	capArg := []string{"--append-size-cap-bytes", fmt.Sprintf("%d", smallCap)}

	ginkgo.Context("SizeCap SingleNode", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newDedicatedSingleNodeS3Target(ginkgo.GinkgoTB(), capArg)
		})
		runSizeCapCases(func() s3Target { return tgt }, smallCap)
	})

	ginkgo.Context("SizeCap Cluster4Node", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newClusterS3TargetWithExtraArgs(ginkgo.GinkgoTB(), 4, capArg)
		})
		runSizeCapCases(func() s3Target { return tgt }, smallCap)
	})
}

func runSizeCapCases(getTgt func() s3Target, smallCap int64) {
	ginkgo.It("rejects an append over the cap", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		bucket := tgt.uniqueBucket(t, "reject")
		key := "obj-over"
		body := bytes.Repeat([]byte("x"), int(smallCap-1))
		gomega.Expect(putAppend(tgt.pickNode(0), bucket, key, 0, body)).To(gomega.Succeed())
		err := putAppend(tgt.pickNode(0), bucket, key, smallCap-1, []byte("yz"))
		gomega.Expect(err).To(gomega.HaveOccurred())
		var apiErr smithy.APIError
		gomega.Expect(errors.As(err, &apiErr)).To(gomega.BeTrue())
		gomega.Expect(apiErr.ErrorCode()).To(gomega.Equal("EntityTooLarge"),
			"over-cap append must surface EntityTooLarge, got %s", apiErr.ErrorCode())
	})

	ginkgo.It("serializes concurrent appends near the cap", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		bucket := tgt.uniqueBucket(t, "race")
		key := "obj-race"
		prefill := bytes.Repeat([]byte("x"), int(smallCap-4))
		gomega.Expect(putAppend(tgt.pickNode(0), bucket, key, 0, prefill)).To(gomega.Succeed())

		var wg sync.WaitGroup
		var successes atomic.Int64
		var rejects atomic.Int64
		for i := 0; i < tgt.nodes; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				err := putAppend(tgt.pickNode(i), bucket, key, smallCap-4, []byte("abcd"))
				if err == nil {
					successes.Add(1)
					return
				}
				var apiErr smithy.APIError
				if errors.As(err, &apiErr) {
					code := apiErr.ErrorCode()
					if code == "EntityTooLarge" || code == "InvalidWriteOffset" {
						rejects.Add(1)
						return
					}
				}
				t.Errorf("node %d: unexpected err %v", i, err)
			}(i)
		}
		wg.Wait()
		gomega.Expect(successes.Load()).To(gomega.Equal(int64(1)), "exactly one append must win")
		gomega.Expect(rejects.Load()).To(gomega.Equal(int64(tgt.nodes-1)),
			"all losers must surface EntityTooLarge or InvalidWriteOffset")
	})
}
