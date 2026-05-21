package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func getBinary() string {
	binary := os.Getenv("GRAINFS_BINARY")
	if binary == "" {
		binary = "../../bin/grainfs"
	}
	return binary
}

var _ = ginkgo.Describe("Cluster and no-peers behavior", func() {
	ginkgo.Context("NoPeers Multipart SingleNode", func() {
		ginkgo.It("completes a multipart upload", func() {
			runNoPeersMultipartCases(ginkgo.GinkgoTB())
		})
	})

	ginkgo.Context("NoPeers RestartPersistence SingleNode", func() {
		ginkgo.It("preserves objects across restart", func() {
			runNoPeersRestartPersistenceCases(ginkgo.GinkgoTB())
		})
	})

	ginkgo.Context("MultipartListFanout Cluster4Node", func() {
		var tgt s3Target
		ginkgo.BeforeEach(func() {
			tgt = newSharedClusterS3Target(ginkgo.GinkgoTB())
		})
		ginkgo.It("lists incomplete multipart uploads from every node", func() {
			runMultipartListFanoutCases(ginkgo.GinkgoTB(), tgt)
		})
	})
})

// TestNoPeersRestartPersistenceE2E exercises stop+restart on the same dataDir
// — single-binary semantics. Cluster has no analogue (raft handles
// process-restart differently), so this is single-node-only by nature.
func runNoPeersRestartPersistenceCases(t testing.TB) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-cluster-e2e-*")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(os.RemoveAll, dir)

	binary := getBinary()

	// Shared encryption key — restart on the same dataDir requires the
	// IAM secrets to round-trip across processes.
	encKeyFile := makeSharedEncryptionKeyFile(t)

	// Step 1: Start in no-peers mode, create some data
	port1 := freePort()
	cmd1 := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port1),
		"--encryption-key-file", encKeyFile,
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd1.Stdout = os.Stdout
	cmd1.Stderr = os.Stderr
	gomega.Expect(cmd1.Start()).To(gomega.Succeed())
	cmd1Running := true
	ginkgo.DeferCleanup(func() {
		if cmd1Running {
			terminateProcess(cmd1)
		}
	})

	endpoint1 := fmt.Sprintf("http://127.0.0.1:%d", port1)
	waitForPort(t, port1, 30*time.Second)
	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dir}, 30*time.Second)
	ak, sk := bootstrap.AccessKey, bootstrap.SecretKey
	client1 := s3ClientFor(endpoint1, ak, sk)

	ctx := context.Background()

	gomega.Expect(adminCreateBucketWithPolicyAttachAny([]string{dir}, bootstrap.SAID, "persist-test", 30*time.Second)).To(gomega.Succeed())

	testData := map[string]string{
		"file1.txt":           "hello from local",
		"docs/readme.md":      "# GrainFS",
		"data/nested/obj.bin": "binary content",
	}

	for key, content := range testData {
		gomega.Eventually(func() bool {
			return tryPutObject(ctx, client1, "persist-test", key, []byte(content)) == nil
		}).WithTimeout(30*time.Second).WithPolling(500*time.Millisecond).Should(gomega.BeTrue(), "put %s", key)
	}

	listOut, err := client1.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("persist-test"),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(listOut.Contents).To(gomega.HaveLen(len(testData)))

	// Stop server and wait for full teardown
	terminateProcess(cmd1)
	cmd1Running = false
	time.Sleep(200 * time.Millisecond) // let OS release file locks

	// Step 2: Restart on a different port and verify data is intact
	port2 := freePort()
	cmd2 := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port2),
		"--encryption-key-file", encKeyFile,
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd2.Stdout = os.Stdout
	cmd2.Stderr = os.Stderr
	gomega.Expect(cmd2.Start()).To(gomega.Succeed())
	ginkgo.DeferCleanup(terminateProcess, cmd2)

	endpoint2 := fmt.Sprintf("http://127.0.0.1:%d", port2)
	waitForPort(t, port2, 30*time.Second)
	client2 := s3ClientFor(endpoint2, ak, sk)

	var listOut2 *s3.ListObjectsV2Output
	gomega.Eventually(func() bool {
		attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		listOut2, err = client2.ListObjectsV2(attemptCtx, &s3.ListObjectsV2Input{
			Bucket: aws.String("persist-test"),
		}, func(o *s3.Options) {
			o.RetryMaxAttempts = 1
		})
		return err == nil && len(listOut2.Contents) == len(testData)
	}).WithTimeout(60*time.Second).WithPolling(1*time.Second).Should(gomega.BeTrue(), "list objects after restart: %v", err)
	gomega.Expect(listOut2.Contents).To(gomega.HaveLen(len(testData)), "all objects should survive restart")

	for key, expectedContent := range testData {
		getOut, err := client2.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("persist-test"),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "get %s after restart", key)
		body, _ := io.ReadAll(getOut.Body)
		getOut.Body.Close()
		gomega.Expect(string(body)).To(gomega.Equal(expectedContent), "content mismatch for %s", key)
	}

	// Verify new writes work after restart
	gomega.Eventually(func() bool {
		return tryPutObject(ctx, client2, "persist-test", "post-restart.txt", []byte("new data after restart")) == nil
	}).WithTimeout(30*time.Second).WithPolling(500*time.Millisecond).Should(gomega.BeTrue(), "put post-restart.txt")

	getOut, err := client2.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("persist-test"),
		Key:    aws.String("post-restart.txt"),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	body, _ := io.ReadAll(getOut.Body)
	getOut.Body.Close()
	gomega.Expect(string(body)).To(gomega.Equal("new data after restart"))
}

// TestNoPeersMultipartE2E exercises multipart against a single-binary
// no-peers server. The cluster shape's multipart coverage lives in
// TestMultipartE2E/Cluster4Node; this case stays single-only by design.
func runNoPeersMultipartCases(t testing.TB) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-cluster-mp-*")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(os.RemoveAll, dir)

	binary := getBinary()
	port := freePort()

	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	gomega.Expect(cmd.Start()).To(gomega.Succeed())
	ginkgo.DeferCleanup(terminateProcess, cmd)

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 30*time.Second)
	bootstrap, _ := bootstrapAdminViaUDSAnyResult(t, []string{dir}, 30*time.Second)
	client := s3ClientFor(endpoint, bootstrap.AccessKey, bootstrap.SecretKey)

	ctx := context.Background()

	gomega.Expect(adminCreateBucketWithPolicyAttachAny([]string{dir}, bootstrap.SAID, "mp-cluster", 30*time.Second)).To(gomega.Succeed())
	waitForS3Write(t, client, "mp-cluster", "__grainfs_e2e_ready", 30*time.Second)
	_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("mp-cluster"),
		Key:    aws.String("__grainfs_e2e_ready"),
	})

	// Multipart upload
	key := "multipart-cluster.bin"
	part1Data := bytes.Repeat([]byte("X"), 5*1024*1024)
	part2Data := bytes.Repeat([]byte("Y"), 512)

	initOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("mp-cluster"),
		Key:    aws.String(key),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	p1, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("mp-cluster"),
		Key:        aws.String(key),
		UploadId:   initOut.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part1Data),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	p2, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("mp-cluster"),
		Key:        aws.String(key),
		UploadId:   initOut.UploadId,
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader(part2Data),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("mp-cluster"),
		Key:      aws.String(key),
		UploadId: initOut.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: p1.ETag},
				{PartNumber: aws.Int32(2), ETag: p2.ETag},
			},
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("mp-cluster"),
		Key:    aws.String(key),
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer getOut.Body.Close()

	body, _ := io.ReadAll(getOut.Body)
	expected := append(part1Data, part2Data...)
	gomega.Expect(body).To(gomega.Equal(expected))
}

// Multipart list fanout verifies that an incomplete multipart
// upload created on the leader is visible from every cluster node — i.e.
// metadata fan-out works. Cluster-only by nature: the case probes per-node
// visibility, which has no single-node analogue. Runs on the shared cluster.
//
// The plain multipart-list assertion is already exercised by
// TestMultipartE2E/Cluster4Node/List; this test adds the per-node fanout
// assertion on top of the same fixture.
func runMultipartListFanoutCases(t testing.TB, tgt s3Target) {
	t.Helper()
	gomega.Expect(tgt.isCluster).To(gomega.BeTrue(), "multipart list fanout requires cluster fixture")

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	ginkgo.DeferCleanup(cancel)

	bucket := tgt.uniqueBucket(t, "mpfanout")
	// Any node is fine — cluster forwards writes to the owning peer. We use
	// tgt.pickNode(0) for the create/fixture and assert visibility from
	// every node below.
	driver := tgt.pickNode(0)
	waitForMultipartListingCreate(t, ctx, driver, bucket, multipartListingKey, 120*time.Second)
	fixture := createIncompleteMultipartListingFixture(t, ctx, driver, bucket, "cluster-fanout-part")

	for i := 0; i < tgt.nodes; i++ {
		assertMultipartListingFeature(t, ctx, tgt.pickNode(i), fixture, true)
	}
}
