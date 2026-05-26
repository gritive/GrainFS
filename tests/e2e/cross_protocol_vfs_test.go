package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// vfsStatResponse is the response from VFS stat operations.
type vfsStatResponse struct {
	Exists  bool   `json:"exists"`
	Name    string `json:"name,omitempty"`
	Size    int64  `json:"size,omitempty"`
	IsDir   bool   `json:"is_dir,omitempty"`
	ModTime string `json:"mod_time,omitempty"`
	Error   string `json:"error,omitempty"`
}

// vfsStatCall performs a VFS stat operation via the admin API on tgt's writable node.
func vfsStatCall(t testing.TB, endpoint string, req map[string]string, resp *vfsStatResponse) {
	t.Helper()

	body, err := json.Marshal(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	httpResp, err := http.Post(endpoint+"/admin/debug/vfs/stat", "application/json", bytes.NewReader(body)) //nolint:noctx
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(httpResp.Body.Close)

	respBody, err := io.ReadAll(httpResp.Body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	gomega.Expect(httpResp.StatusCode).To(gomega.Equal(http.StatusOK), "VFS stat failed: %s", string(respBody))

	err = json.Unmarshal(respBody, resp)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// Cross-protocol specs exercise S3 ↔ VFS coherency: an S3 PUT must be
// observable via the admin /admin/debug/vfs/stat probe, and an S3 DELETE
// must make the VFS view disappear. Each sub-test scopes itself with a
// uniqueBucket.
var _ = ginkgo.Describe("Cross protocol VFS coherency", func() {
	describeCrossProtocolContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})

	describeCrossProtocolContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describeCrossProtocolContext(name string, factory func() s3Target) {
	ginkgo.Context(name, ginkgo.Ordered, func() {
		var (
			tgt      s3Target
			cli      *s3.Client
			endpoint string
		)

		ginkgo.BeforeAll(func() {
			tgt = factory()
			cli = tgt.pickNode(0)
			endpoint = tgt.endpoint(0)
		})

		runCrossProtocolCases(func() s3Target { return tgt }, func() *s3.Client { return cli }, func() string { return endpoint })
	})
}

func runCrossProtocolCases(getTgt func() s3Target, getClient func() *s3.Client, getEndpoint func() string) {
	ginkgo.It("makes S3 PUTs visible through VFS stat", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		cli := getClient()
		endpoint := getEndpoint()
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "putvfs")

		key := "test-file.txt"
		content := "hello, world"
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte(content)),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		head, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(head.ContentLength).NotTo(gomega.BeNil())
		gomega.Expect(*head.ContentLength).To(gomega.Equal(int64(len(content))))

		var statResp vfsStatResponse
		vfsStatCall(t, endpoint, map[string]string{
			"volume_name": bucket,
			"file_path":   key,
		}, &statResp)

		gomega.Expect(statResp.Exists).To(gomega.BeTrue(), "VFS should see file after S3 PUT")
		gomega.Expect(statResp.Name).To(gomega.Equal(key))
		gomega.Expect(statResp.Size).To(gomega.Equal(int64(len(content))))
		gomega.Expect(statResp.IsDir).To(gomega.BeFalse())
	})

	ginkgo.It("removes deleted S3 objects from VFS stat", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		cli := getClient()
		endpoint := getEndpoint()
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "delvfs")

		key := "test-file.txt"
		content := "hello, world"
		_, err := cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte(content)),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		head, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(head).NotTo(gomega.BeNil())

		statReq := map[string]string{
			"volume_name": bucket,
			"file_path":   key,
		}
		var statResp vfsStatResponse
		vfsStatCall(t, endpoint, statReq, &statResp)
		gomega.Expect(statResp.Exists).To(gomega.BeTrue(), "VFS should see file before delete")

		_, err = cli.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Eventual consistency window (Raft commit + cache invalidation).
		time.Sleep(200 * time.Millisecond)

		_, err = cli.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		gomega.Expect(err).To(gomega.HaveOccurred(), "S3 HEAD should fail after delete")

		vfsStatCall(t, endpoint, statReq, &statResp)
		gomega.Expect(statResp.Exists).To(gomega.BeFalse(), "VFS should not see deleted file")
	})
}
