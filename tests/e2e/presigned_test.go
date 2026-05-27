// Presigned URL S3 e2e (target table-driven).
//
// The four presigned URL cases (GET, PUT, Expired, WrongKey) run against
// both a single-node fixture and a 4-node cluster fixture. Created buckets use
// tgt.uniqueBucket so shared fixtures do not leak state across specs.
//
// TestMetrics_Endpoint and TestDashboard_Serves are not S3-op tests and stay
// out of the target-table.
package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/s3auth"
)

var _ = ginkgo.Describe("Presigned URL and metrics", func() {
	describePresignedContext("SingleNode", func() s3Target {
		return newSingleNodeS3Target()
	})

	describePresignedContext("Cluster4Node", func() s3Target {
		return newSharedClusterS3Target(ginkgo.GinkgoTB())
	})
})

func describePresignedContext(name string, factory func() s3Target) {
	ginkgo.Context(name, func() {
		var tgt s3Target

		ginkgo.BeforeEach(func() {
			tgt = factory()
		})

		runPresignedCases(func() s3Target { return tgt })
		runMetricsEndpointCases(func() s3Target { return tgt })
	})
}

func runPresignedCases(getTgt func() s3Target) {
	ginkgo.It("serves presigned GET URLs (GET)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
		endpoint := tgt.endpoint(0)
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "presign-get")

		content := "presigned content"
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("secret.txt"),
			Body:   strings.NewReader(content),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		presigned, err := s3auth.PresignURL(http.MethodGet,
			endpoint+"/"+bucket+"/secret.txt",
			tgt.accessKey, tgt.secretKey, "us-east-1", 3600)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		resp, err := http.Get(presigned)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(resp.Body.Close)

		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
		body, _ := io.ReadAll(resp.Body)
		gomega.Expect(string(body)).To(gomega.Equal(content))
	})

	ginkgo.It("accepts presigned PUT URLs (PUT)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
		endpoint := tgt.endpoint(0)
		ctx := context.Background()
		bucket := tgt.uniqueBucket(t, "presign-put")

		presigned, err := s3auth.PresignURL(http.MethodPut,
			endpoint+"/"+bucket+"/uploaded.txt",
			tgt.accessKey, tgt.secretKey, "us-east-1", 3600)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		content := "uploaded via presigned"
		var lastErr error
		var lastStatus int
		gomega.Eventually(func() bool {
			req, _ := http.NewRequest(http.MethodPut, presigned, strings.NewReader(content))
			resp, err := http.DefaultClient.Do(req)
			lastErr = err
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			lastStatus = resp.StatusCode
			return resp.StatusCode == http.StatusOK
		}).WithTimeout(30*time.Second).WithPolling(500*time.Millisecond).Should(gomega.BeTrue(), "presigned PUT status=%d err=%v", lastStatus, lastErr)

		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("uploaded.txt"),
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(getOut.Body.Close)

		body, _ := io.ReadAll(getOut.Body)
		gomega.Expect(string(body)).To(gomega.Equal(content))
	})

	ginkgo.It("rejects expired presigned URLs (Expired)", func() {
		tgt := getTgt()
		endpoint := tgt.endpoint(0)
		presigned, err := s3auth.PresignURLAt(http.MethodGet,
			endpoint+"/"+tgt.name+"-presign-exp/file.txt",
			tgt.accessKey, tgt.secretKey, "us-east-1", 1, time.Now().Add(-10*time.Second))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		resp, err := http.Get(presigned)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(resp.Body.Close)
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusForbidden))
	})

	ginkgo.It("rejects presigned URLs signed with the wrong key (WrongKey)", func() {
		tgt := getTgt()
		endpoint := tgt.endpoint(0)
		presigned, err := s3auth.PresignURL(http.MethodGet,
			endpoint+"/"+tgt.name+"-presign-wrong/file.txt",
			tgt.accessKey, "wrongsecret", "us-east-1", 3600)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		resp, err := http.Get(presigned)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(resp.Body.Close)
		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusForbidden))
	})
}

// ----- non-S3-op tests (kept as-is; not part of the target-table refactor) -----

// authServer is the handle returned by startAuthServer. The credentials
// are bootstrapped via the admin UDS after server start, so callers must
// thread them through to s3auth.PresignURL etc.
type authServer struct {
	Client    *s3.Client
	Endpoint  string
	DataDir   string
	AccessKey string
	SecretKey string
	Cleanup   func()
}

// startAuthServer starts grainfs and bootstraps an admin SA via UDS.
func startAuthServer(t testing.TB) authServer {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-auth-e2e-*")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

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

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 30*time.Second)

	ak, sk := bootstrapAdminViaUDS(t, dir)

	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(ak, sk, ""),
		UsePathStyle: true,
	})
	gomega.Expect(waitForIAMReady(client, 30*time.Second)).To(gomega.Succeed())

	cleanup := func() {
		cmd.Process.Kill()
		cmd.Wait()
		removeE2EDir(dir)
	}

	return authServer{
		Client:    client,
		Endpoint:  endpoint,
		DataDir:   dir,
		AccessKey: ak,
		SecretKey: sk,
		Cleanup:   cleanup,
	}
}

func runMetricsEndpointCases(getTgt func() s3Target) {
	ginkgo.It("exposes HTTP metrics (ExposesHTTPMetrics)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		ctx := context.Background()
		cli := tgt.pickNode(0)
		endpoint := tgt.endpoint(0)
		bucket := tgt.uniqueBucket(t, "metrics")
		_, _ = cli.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("file.txt"),
			Body:   strings.NewReader("data"),
		})

		req, _ := http.NewRequest(http.MethodGet, endpoint+"/metrics", nil)
		req.Header.Set("Accept-Encoding", "identity")
		resp, err := http.DefaultClient.Do(req)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(resp.Body.Close)

		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
		body, _ := io.ReadAll(resp.Body)
		bodyStr := string(body)

		gomega.Expect(bodyStr).To(gomega.ContainSubstring("grainfs_http_requests_total"))
		gomega.Expect(bodyStr).To(gomega.ContainSubstring("grainfs_http_request_duration_seconds"))
	})
}
