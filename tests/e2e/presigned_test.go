// Presigned URL S3 e2e (target table-driven).
//
// The four presigned URL cases (GET, PUT, Expired, WrongKey) run against
// both a single-node fixture and a 4-node cluster fixture. Bucket names are
// prefixed with tgt.name to avoid collisions.
//
// TestMetrics_Endpoint and TestDashboard_Serves are not S3-op tests and stay
// out of the target-table — they continue to use the shared server.
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		bucket := tgt.name + "-presign-get"
		tgt.createBkt(t, bucket)

		content := "presigned content"
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("secret.txt"),
			Body:   strings.NewReader(content),
		})
		require.NoError(t, err)

		presigned, err := s3auth.PresignURL(http.MethodGet,
			endpoint+"/"+bucket+"/secret.txt",
			tgt.accessKey, tgt.secretKey, "us-east-1", 3600)
		require.NoError(t, err)

		resp, err := http.Get(presigned)
		require.NoError(t, err)
		ginkgo.DeferCleanup(resp.Body.Close)

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		body, _ := io.ReadAll(resp.Body)
		assert.Equal(t, content, string(body))
	})

	ginkgo.It("accepts presigned PUT URLs (PUT)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		client := tgt.pickNode(0)
		endpoint := tgt.endpoint(0)
		ctx := context.Background()
		bucket := tgt.name + "-presign-put"
		tgt.createBkt(t, bucket)

		presigned, err := s3auth.PresignURL(http.MethodPut,
			endpoint+"/"+bucket+"/uploaded.txt",
			tgt.accessKey, tgt.secretKey, "us-east-1", 3600)
		require.NoError(t, err)

		content := "uploaded via presigned"
		var lastErr error
		var lastStatus int
		require.Eventually(t, func() bool {
			req, _ := http.NewRequest(http.MethodPut, presigned, strings.NewReader(content))
			resp, err := http.DefaultClient.Do(req)
			lastErr = err
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			lastStatus = resp.StatusCode
			return resp.StatusCode == http.StatusOK
		}, 30*time.Second, 500*time.Millisecond, "presigned PUT status=%d err=%v", lastStatus, lastErr)

		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("uploaded.txt"),
		})
		require.NoError(t, err)
		ginkgo.DeferCleanup(getOut.Body.Close)

		body, _ := io.ReadAll(getOut.Body)
		assert.Equal(t, content, string(body))
	})

	ginkgo.It("rejects expired presigned URLs (Expired)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		endpoint := tgt.endpoint(0)
		presigned, err := s3auth.PresignURLAt(http.MethodGet,
			endpoint+"/"+tgt.name+"-presign-exp/file.txt",
			tgt.accessKey, tgt.secretKey, "us-east-1", 1, time.Now().Add(-10*time.Second))
		require.NoError(t, err)

		resp, err := http.Get(presigned)
		require.NoError(t, err)
		ginkgo.DeferCleanup(resp.Body.Close)
		assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	})

	ginkgo.It("rejects presigned URLs signed with the wrong key (WrongKey)", func() {
		t := ginkgo.GinkgoTB()
		tgt := getTgt()
		endpoint := tgt.endpoint(0)
		presigned, err := s3auth.PresignURL(http.MethodGet,
			endpoint+"/"+tgt.name+"-presign-wrong/file.txt",
			tgt.accessKey, "wrongsecret", "us-east-1", 3600)
		require.NoError(t, err)

		resp, err := http.Get(presigned)
		require.NoError(t, err)
		ginkgo.DeferCleanup(resp.Body.Close)
		assert.Equal(t, http.StatusForbidden, resp.StatusCode)
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
	require.NoError(t, err)

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
	require.NoError(t, cmd.Start())

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 30*time.Second)

	ak, sk := bootstrapAdminViaUDS(t, dir)

	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(ak, sk, ""),
		UsePathStyle: true,
	})
	require.NoError(t, waitForIAMReady(client, 30*time.Second))

	cleanup := func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.RemoveAll(dir)
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
		require.NoError(t, err)
		ginkgo.DeferCleanup(resp.Body.Close)

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		body, _ := io.ReadAll(resp.Body)
		bodyStr := string(body)

		assert.Contains(t, bodyStr, "grainfs_http_requests_total")
		assert.Contains(t, bodyStr, "grainfs_http_request_duration_seconds")
	})
}
