// Regression e2e for the follower-stale bucket-versioning read: enabling
// versioning on the leader and then writing on a FOLLOWER must produce a
// versioned object (a VersionId). Before the fix, a just-joined follower read
// its lagging local group-0 replica and silently wrote the object non-versioned
// (no VersionId). The mutating S3 edge now linearizes the versioning read
// (ReadIndex + WaitApplied), so the follower observes Enabled deterministically.
//
// This is the deterministic, seam-free coverage that the blocked phantom-winner
// e2e (TODOS Item 3) needed — a follower can now do a versioned write.
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
