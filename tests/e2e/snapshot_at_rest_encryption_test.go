package e2e

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Snapshot at-rest encryption", func() {
	ginkgo.Context("SingleNode", func() {
		var tgt s3Target

		ginkgo.BeforeEach(func() {
			tgt = newDedicatedSingleNodeS3Target(ginkgo.GinkgoTB(), nil)
		})

		ginkgo.It("seals object-metadata snapshots on disk", func() {
			t := ginkgo.GinkgoTB()
			ctx := context.Background()
			client := tgt.pickNode(0)
			serverURL := tgt.endpoint(0)
			bucket := tgt.uniqueBucket(t, "enc")

			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String("confidential-object-key"),
				Body:   strings.NewReader("confidential-value"),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			createPITRSnapshot(t, serverURL, "at-rest-enc")

			pattern := filepath.Join(tgt.dataDir, "snapshots", "snapshot-*.json.zst")
			matches, err := filepath.Glob(pattern)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(matches).NotTo(gomega.BeEmpty(), "at least one snapshot file must exist after createPITRSnapshot")

			for _, path := range matches {
				raw, readErr := os.ReadFile(path)
				gomega.Expect(readErr).NotTo(gomega.HaveOccurred(), "read snapshot file %s", path)
				gomega.Expect(raw[:4]).To(gomega.Equal([]byte("GSNE")),
					"snapshot file %s must start with GSNE envelope magic", path)
				gomega.Expect(bytes.Contains(raw, []byte("confidential-object-key"))).To(gomega.BeFalse(),
					"snapshot file %s must not contain plaintext object key", path)
			}
		})

		ginkgo.It("restores from a sealed snapshot", func() {
			t := ginkgo.GinkgoTB()
			ctx := context.Background()
			client := tgt.pickNode(0)
			serverURL := tgt.endpoint(0)
			bucket := tgt.uniqueBucket(t, "restore")

			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String("confidential-object-key"),
				Body:   strings.NewReader("confidential-value"),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			createPITRSnapshot(t, serverURL, "at-rest-restore")

			getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String("confidential-object-key"),
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(getResp.Body.Close)
			body, _ := io.ReadAll(getResp.Body)
			gomega.Expect(string(body)).To(gomega.Equal("confidential-value"))
		})
	})
})
