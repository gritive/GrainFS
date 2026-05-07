package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// iamSAResult is the deserialized response from POST /v1/iam/sa.
type iamSAResult struct {
	SAID      string    `json:"sa_id"`
	Name      string    `json:"name"`
	AccessKey string    `json:"access_key"`
	SecretKey string    `json:"secret_key"`
	CreatedAt time.Time `json:"created_at"`
}

// iamKeyResult is the deserialized response from POST /v1/iam/sa/{id}/key.
type iamKeyResult struct {
	AccessKey string     `json:"access_key"`
	SecretKey string     `json:"secret_key"`
	SAID      string     `json:"sa_id"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// iamGrant matches the GrantListItem wire shape.
type iamGrant struct {
	SAID   string `json:"sa_id"`
	Bucket string `json:"bucket"`
	Role   string `json:"role"`
}

// iamUDSClient builds an *http.Client that dials the admin Unix socket.
func iamUDSClient(sock string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
	}
}

// iamDo issues an admin UDS request and decodes JSON into out (if non-nil).
// Fatals the test on transport / non-2xx errors.
func iamDo(t *testing.T, sock, method, path string, body any, out any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		require.NoError(t, err, "marshal body")
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, "http://unix"+path, rdr)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := iamUDSClient(sock).Do(req)
	require.NoErrorf(t, err, "admin %s %s", method, path)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		t.Fatalf("admin %s %s -> %d: %s", method, path, resp.StatusCode, string(respBody))
	}
	if out != nil && len(respBody) > 0 {
		require.NoErrorf(t, json.Unmarshal(respBody, out), "decode %s %s", method, path)
	}
}

// iamCreateSA POSTs /v1/iam/sa and returns the SA's id + first key pair.
func iamCreateSA(t *testing.T, sock, name string) iamSAResult {
	t.Helper()
	var out iamSAResult
	iamDo(t, sock, "POST", "/v1/iam/sa",
		map[string]string{"name": name}, &out)
	return out
}

// iamSADelete DELETEs the given SA. 204 on success.
func iamSADelete(t *testing.T, sock, saID string) {
	t.Helper()
	iamDo(t, sock, "DELETE", "/v1/iam/sa/"+saID, nil, nil)
}

// iamGrantPut PUTs an explicit grant (Role: Read|Write|Admin, exact bucket).
func iamGrantPut(t *testing.T, sock, saID, bucket, role string) {
	t.Helper()
	iamDo(t, sock, "PUT", "/v1/iam/grant",
		map[string]string{"sa_id": saID, "bucket": bucket, "role": role}, nil)
}

// iamKeyRevoke marks the given access_key revoked.
func iamKeyRevoke(t *testing.T, sock, saID, accessKey string) {
	t.Helper()
	iamDo(t, sock, "DELETE", "/v1/iam/sa/"+saID+"/key/"+accessKey, nil, nil)
}

// iamKeyCreateExpiringIn rotates a new key with a future ExpiresAt.
func iamKeyCreateExpiringIn(t *testing.T, sock, saID string, ttl time.Duration) iamKeyResult {
	t.Helper()
	exp := time.Now().UTC().Add(ttl)
	var out iamKeyResult
	iamDo(t, sock, "POST", "/v1/iam/sa/"+saID+"/key",
		map[string]any{"expires_at": exp.Format(time.RFC3339Nano)}, &out)
	return out
}

// iamListGrants returns all grants matching optional sa / bucket filters.
func iamListGrants(t *testing.T, sock, saFilter, bucketFilter string) []iamGrant {
	t.Helper()
	q := url.Values{}
	if saFilter != "" {
		q.Set("sa", saFilter)
	}
	if bucketFilter != "" {
		q.Set("bucket", bucketFilter)
	}
	path := "/v1/iam/grant"
	if enc := q.Encode(); enc != "" {
		path += "?" + enc
	}
	var out []iamGrant
	iamDo(t, sock, "GET", path, nil, &out)
	return out
}

// iamTestServer is the wired-up handle returned by startIAMTestServer.
type iamTestServer struct {
	S3URL       string
	AdminSock   string
	DataDir     string
	BootstrapAK string
	BootstrapSK string
	Client      *s3.Client
}

// Stop is a no-op — cleanup happens via t.Cleanup hooks registered in
// startIAMTestServer. Provided so callers can write `defer srv.Stop()`
// without surprise.
func (s iamTestServer) Stop() {}

// startIAMTestServer launches a single-node grainfs binary, bootstraps a
// unique (per-test) Admin SA via --access-key/--secret-key, and returns a
// handle the IAM e2e tests can use to drive both the S3 plane and the
// admin UDS. Per-test bootstrap creds avoid collisions when tests run in
// parallel against separate data dirs.
func startIAMTestServer(t *testing.T) iamTestServer {
	t.Helper()

	// 6-byte hex suffix → 12 hex chars; plenty of entropy for parallel runs.
	var nonce [6]byte
	_, _ = rand.Read(nonce[:])
	tag := hex.EncodeToString(nonce[:])
	bootAK := "iamtest" + tag
	bootSK := "iamtest" + tag + "-secret"

	dir, err := os.MkdirTemp("", "grainfs-iam-e2e-*")
	require.NoError(t, err, "mkdtemp")
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	port := freePort()
	cmd := exec.Command(getBinary(), "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--dedup=false",
		"--access-key", bootAK,
		"--secret-key", bootSK,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start(), "start IAM e2e server")
	t.Cleanup(func() { terminateProcess(cmd) })

	s3URL := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 30*time.Second)
	cli := s3ClientFor(s3URL, bootAK, bootSK)
	require.NoError(t, waitForIAMReady(cli, 30*time.Second))

	return iamTestServer{
		S3URL:       s3URL,
		AdminSock:   filepath.Join(dir, "admin.sock"),
		DataDir:     dir,
		BootstrapAK: bootAK,
		BootstrapSK: bootSK,
		Client:      cli,
	}
}

// s3ClientFor builds an aws-sdk-go-v2 S3 client signing with custom static
// creds. Mirrors newS3Client (test/test) but takes the credentials so each
// IAM test can drive the API as a different SA.
func s3ClientFor(endpoint, ak, sk string) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(ak, sk, ""),
		UsePathStyle: true,
	})
}

// TestIAMHelpers_StartServer_BootstrapAccepted smoke-tests that
// startIAMTestServer brings up a server with bootstrap creds wired
// correctly: HeadBucket on a missing bucket returns NotFound (not 401),
// proving the SigV4 verifier accepts the bootstrap key pair.
func TestIAMHelpers_StartServer_BootstrapAccepted(t *testing.T) {
	srv := startIAMTestServer(t)
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := srv.Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("__bootstrap_probe__"),
	})
	if err == nil {
		// 200 also means auth succeeded (probe bucket somehow existed).
		return
	}
	// NotFound / NoSuchBucket → auth passed, just no such bucket.
	// 401/403 → auth failed, fail the test.
	msg := err.Error()
	if !(contains(msg, "NotFound") || contains(msg, "NoSuchBucket") || contains(msg, "404")) {
		t.Fatalf("HeadBucket with bootstrap creds returned non-auth error: %v", err)
	}
}

func contains(s, sub string) bool {
	return bytes.Contains([]byte(s), []byte(sub))
}
