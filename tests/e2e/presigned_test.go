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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
)

// startAuthServer starts grainfs with authentication enabled.
func startAuthServer(t *testing.T) (*s3.Client, string, string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "grainfs-auth-e2e-*")
	require.NoError(t, err)

	binary := getBinary()
	port := freePort()

	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--access-key", "testkey",
		"--secret-key", "testsecret",
		"--nfs4-port", "0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(port, 5*time.Second)

	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("testkey", "testsecret", ""),
		UsePathStyle: true,
	})

	cleanup := func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.RemoveAll(dir)
	}

	return client, endpoint, dir, cleanup
}

func TestPresigned_GET(t *testing.T) {
	client, endpoint, _, cleanup := startAuthServer(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("presign-get"),
	})
	require.NoError(t, err)

	content := "presigned content"
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("presign-get"),
		Key:    aws.String("secret.txt"),
		Body:   strings.NewReader(content),
	})
	require.NoError(t, err)

	presigned, err := s3auth.PresignURL(http.MethodGet,
		endpoint+"/presign-get/secret.txt",
		"testkey", "testsecret", "us-east-1", 3600)
	require.NoError(t, err)

	resp, err := http.Get(presigned)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, content, string(body))
}

func TestPresigned_PUT(t *testing.T) {
	client, endpoint, _, cleanup := startAuthServer(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("presign-put"),
	})
	require.NoError(t, err)

	presigned, err := s3auth.PresignURL(http.MethodPut,
		endpoint+"/presign-put/uploaded.txt",
		"testkey", "testsecret", "us-east-1", 3600)
	require.NoError(t, err)

	content := "uploaded via presigned"
	req, _ := http.NewRequest(http.MethodPut, presigned, strings.NewReader(content))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("presign-put"),
		Key:    aws.String("uploaded.txt"),
	})
	require.NoError(t, err)
	defer getOut.Body.Close()

	body, _ := io.ReadAll(getOut.Body)
	assert.Equal(t, content, string(body))
}

func TestPresigned_Expired(t *testing.T) {
	_, endpoint, _, cleanup := startAuthServer(t)
	defer cleanup()

	presigned, err := s3auth.PresignURLAt(http.MethodGet,
		endpoint+"/presign-exp/file.txt",
		"testkey", "testsecret", "us-east-1", 1, time.Now().Add(-10*time.Second))
	require.NoError(t, err)

	resp, err := http.Get(presigned)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestPresigned_WrongKey(t *testing.T) {
	_, endpoint, _, cleanup := startAuthServer(t)
	defer cleanup()

	presigned, err := s3auth.PresignURL(http.MethodGet,
		endpoint+"/presign-wrong/file.txt",
		"testkey", "wrongsecret", "us-east-1", 3600)
	require.NoError(t, err)

	resp, err := http.Get(presigned)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestMetrics_Endpoint(t *testing.T) {
	client, endpoint, _, cleanup := startAuthServer(t)
	defer cleanup()

	ctx := context.Background()

	// Make some API calls to populate metrics
	client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("metrics-test")})
	client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("metrics-test"),
		Key:    aws.String("file.txt"),
		Body:   strings.NewReader("data"),
	})

	req, _ := http.NewRequest(http.MethodGet, endpoint+"/metrics", nil)
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	assert.Contains(t, bodyStr, "grainfs_http_requests_total")
	assert.Contains(t, bodyStr, "grainfs_http_request_duration_seconds")
}

func TestDashboard_Serves(t *testing.T) {
	resp, err := http.Get(testServerURL + "/ui/")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	assert.Contains(t, bodyStr, "GrainFS")
	assert.Contains(t, bodyStr, "<!DOCTYPE html>")
}
