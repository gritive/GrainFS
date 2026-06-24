// Admin non-force bucket delete must reject a non-empty bucket whose versioning is
// NOT Enabled — never-versioned (objects in the latest-only quorum-meta tree) and
// Suspended (preserved versions in the per-version tree). A regression silently
// deletes a non-empty bucket (data loss). Reached via the admin UDS /
// `grainfs bucket delete` (the S3 data-plane DeleteBucket is always AccessDenied),
// so this drives the CLI, not the S3 DeleteBucket API.
package e2e

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Bucket delete non-Enabled emptiness", ginkgo.Label("bucket"), func() {
	for _, tc := range []struct {
		name string
		mk   func() s3Target
	}{
		{name: "SingleNode", mk: newSingleNodeS3Target},
		{name: "Cluster4Node", mk: func() s3Target { return newSharedClusterS3Target(ginkgo.GinkgoTB()) }},
	} {
		tc := tc
		ginkgo.Context(tc.name, func() {
			ginkgo.It("rejects non-force delete of a non-empty never-versioned bucket", func() {
				ctx := context.Background()
				tgt := tc.mk()
				tb := ginkgo.GinkgoTB()
				name := createSpecBucket(tgt, "nvdel")

				_, err := tgt.pickNode(0).PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(name), Key: aws.String("file.txt"), Body: stringReader("data"),
				})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				out, code := runCLI(tb, "", "bucket", "delete", name, "--endpoint", tgt.adminSockPath())
				gomega.Expect(code).NotTo(gomega.Equal(0), "non-force delete of a non-empty bucket must fail; got success:\n"+out)
				gomega.Expect(strings.ToLower(out)).To(gomega.ContainSubstring("not empty"))

				_, herr := tgt.pickNode(0).HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)})
				gomega.Expect(herr).NotTo(gomega.HaveOccurred(), "bucket must survive a rejected non-force delete")

				// Best-effort cleanup: delete the object (leaves a hard-delete
				// tombstone, which the emptiness check excludes), then the non-force
				// delete succeeds. Avoids --force, whose cluster purge-vs-recheck
				// completeness is a captured follow-up.
				_, _ = tgt.pickNode(0).DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(name), Key: aws.String("file.txt")})
				_, _ = runCLI(tb, "", "bucket", "delete", name, "--endpoint", tgt.adminSockPath())
			})

			ginkgo.It("rejects non-force delete of a Suspended bucket holding a preserved version", func() {
				ctx := context.Background()
				tgt := tc.mk()
				tb := ginkgo.GinkgoTB()
				client := tgt.pickNode(0)
				name := createSpecBucket(tgt, "suspdel")

				putVer := func(status s3types.BucketVersioningStatus) {
					_, e := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
						Bucket:                  aws.String(name),
						VersioningConfiguration: &s3types.VersioningConfiguration{Status: status},
					})
					gomega.Expect(e).NotTo(gomega.HaveOccurred())
				}
				putVer(s3types.BucketVersioningStatusEnabled)
				_, err := client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(name), Key: aws.String("v.txt"), Body: stringReader("data"),
				})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				putVer(s3types.BucketVersioningStatusSuspended)

				out, code := runCLI(tb, "", "bucket", "delete", name, "--endpoint", tgt.adminSockPath())
				gomega.Expect(code).NotTo(gomega.Equal(0), "Suspended bucket with a preserved version must reject non-force delete:\n"+out)
				gomega.Expect(strings.ToLower(out)).To(gomega.ContainSubstring("not empty"))

				// Best-effort cleanup: empty the per-version tree via S3, then delete.
				// `--force` does NOT purge a Suspended bucket's preserved per-version
				// blobs yet (captured TODOS follow-up), so empty it explicitly here.
				if lv, lerr := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(name)}); lerr == nil {
					for _, v := range lv.Versions {
						_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(name), Key: v.Key, VersionId: v.VersionId})
					}
				}
				_, _ = runCLI(tb, "", "bucket", "delete", name, "--endpoint", tgt.adminSockPath())
			})
		})
	}
})
