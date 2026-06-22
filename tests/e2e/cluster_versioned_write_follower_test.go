// Integration coverage for a versioned write on a FOLLOWER: enable versioning on
// the leader, then PUT on a follower, and the object must be versioned (a
// VersionId). This is the multipart-off-raft control-plane path that the mutating
// S3 edge now linearizes (GetBucketVersioningLinearized: ReadIndex + WaitApplied)
// so a follower observes leader-enabled versioning rather than its lagging local
// group-0 replica.
//
// Scope note: this is a happy-path integration test, NOT a deterministic
// regression of the stale window. The follower-stale read is a TRANSIENT apply
// lag (observed up to ~90s, but a follower often catches up within the test's
// retry window), so this spec passes with or without the barrier when there is
// no lag. Deterministically reproducing the window would need a group-0
// SetBucketVersioning apply-delay seam, and a delay long enough to outlast the
// PUT retry window collides with the 30s propose deadline (the same trap that
// blocked the phantom-winner e2e). The barrier's correctness rests on reusing the
// gate-validated ReadIndex+WaitApplied primitive object reads already use
// (exec_policy.go ResolveRead); this spec guards the end-to-end wiring.
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Cluster versioned write on a follower", func() {
	ginkgo.It("a follower observes leader-enabled versioning and writes a versioned object", func() {
		runVersionedWriteOnFollowerSpec(ginkgo.GinkgoTB())
	})
})

func runVersionedWriteOnFollowerSpec(t testing.TB) {
	const bucket = "ver-follower"
	c := startE2ECluster(t, e2eClusterOptions{Nodes: 3, Mode: ClusterModeDynamicJoin})
	ctx := context.Background()
	gomega.Expect(adminCreateBucketWithPolicyAttachAny(c.dataDirs, c.saID, bucket, 60*time.Second)).To(gomega.Succeed())

	// Enable versioning on the leader (node 0).
	gomega.Eventually(func() error {
		_, err := c.S3Client(0).PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket:                  aws.String(bucket),
			VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
		})
		return err
	}, 60*time.Second, 500*time.Millisecond).Should(gomega.Succeed())

	// PUT on each follower (nodes 1 and 2). The linearizable edge read makes the
	// follower observe Enabled, so the object is versioned (non-empty VersionId).
	// A stale local read would write it non-versioned (empty VersionId).
	for _, node := range []int{1, 2} {
		key := fmt.Sprintf("obj-%d.bin", node)
		gomega.Eventually(func() (string, error) {
			out, err := c.S3Client(node).PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(key),
				Body: bytes.NewReader([]byte("v1")),
			})
			if err != nil {
				return "", err
			}
			return aws.ToString(out.VersionId), nil
		}, 60*time.Second, 1*time.Second).ShouldNot(gomega.BeEmpty(),
			"PUT on follower node %d must be versioned (non-empty VersionId)", node)
	}
}
