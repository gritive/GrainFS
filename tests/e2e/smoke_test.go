package e2e

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// TestSmokeDeploymentE2E is a fast deployment smoke test (under 2 minutes,
// critical path only). Single-node-only: probes the single-binary install
// surface; cluster smoke is covered by the cluster_bootstrap/cluster_join
// suites.
var _ = ginkgo.Describe("Smoke deployment", func() {
	ginkgo.Context("SingleNode", ginkgo.Ordered, func() {
		runSmokeDeploymentCases()
	})
})

func runSmokeDeploymentCases() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		dir       string
		bootstrap iamSAResult
		client    *s3.Client
	)

	ginkgo.BeforeAll(func() {
		t := ginkgo.GinkgoTB()
		var err error
		dir, err = os.MkdirTemp("", "grainfs-smoke-*")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = os.RemoveAll(dir) })

		binary := getBinary()
		port := freePort()

		cmd := exec.Command(binary, "serve",
			"--data", dir,
			"--port", fmt.Sprintf("%d", port),
			"--nfs4-port", fmt.Sprintf("%d", freePort()),
			"--nbd-port", fmt.Sprintf("%d", freePort()),
			"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		gomega.Expect(cmd.Start()).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { terminateProcess(cmd) })

		endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
		waitForPort(t, port, 30*time.Second)

		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		ginkgo.DeferCleanup(cancel)

		bootstrap, _ = bootstrapAdminViaUDSAnyResult(t, []string{dir}, 30*time.Second)
		client = s3ClientFor(endpoint, bootstrap.AccessKey, bootstrap.SecretKey)
	})

	// Test 1: Health check
	ginkgo.It("passes the health check (HealthCheck)", func() {
		// Verify server is responding
		resp, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "health check should succeed")
		gomega.Expect(resp).NotTo(gomega.BeNil(), "health check response should not be nil")
	})

	// Test 2: Create bucket through the admin control plane.
	ginkgo.It("creates the smoke bucket through the admin plane (CreateBucket)", func() {
		t := ginkgo.GinkgoTB()
		createBucketWithAdminPolicyAttachViaUDSAny(t, []string{dir}, bootstrap.SAID, "smoke-test", client)
	})

	// Test 3: Put object
	ginkgo.It("puts an object (PutObject)", func() {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("smoke-test"),
			Key:    aws.String("test-object"),
			Body:   strings.NewReader("smoke test data"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "put object should succeed")
	})

	// Test 4: Get object
	ginkgo.It("gets the object (GetObject)", func() {
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("smoke-test"),
			Key:    aws.String("test-object"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "get object should succeed")
		ginkgo.DeferCleanup(resp.Body.Close)

		content, err := io.ReadAll(resp.Body)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "read object body should succeed")
		gomega.Expect(string(content)).To(gomega.Equal("smoke test data"), "object content should match")
	})

	// Test 5: Delete object
	ginkgo.It("deletes the object (DeleteObject)", func() {
		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String("smoke-test"),
			Key:    aws.String("test-object"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "delete object should succeed")
	})

	// Test 6: List objects (should be empty)
	ginkgo.It("lists no objects after delete (ListObjects)", func() {
		resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String("smoke-test"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "list objects should succeed")
		gomega.Expect(resp.Contents).To(gomega.BeEmpty(), "bucket should be empty after delete")
	})

}
