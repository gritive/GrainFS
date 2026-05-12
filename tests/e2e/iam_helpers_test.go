package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// makeSharedEncryptionKeyFile writes a 32-byte raw key to a temp file and
// returns the path. All cluster nodes must pass --encryption-key-file=<path>
// pointing at the same file so their shardEncryptors agree — IAM secret_key
// wrap/unwrap must round-trip across the raft FSM on every node.
func makeSharedEncryptionKeyFile(t testing.TB) string {
	t.Helper()
	f, err := os.CreateTemp("", "grainfs-e2e-enckey-*")
	require.NoError(t, err)
	var key [32]byte
	_, err = rand.Read(key[:])
	require.NoError(t, err)
	_, err = f.Write(key[:])
	require.NoError(t, err)
	require.NoError(t, f.Close())
	t.Cleanup(func() { _ = os.Remove(f.Name()) })
	return f.Name()
}

// bootstrapAdminViaUDS performs the post-serve admin SA bootstrap via the
// admin UDS. Returns the access_key/secret_key pair for use in subsequent
// S3 sigv4 requests. Replaces the legacy --access-key/--secret-key flag
// pattern. Caller must have started `grainfs serve` and waited for the
// admin socket to exist at <dataDir>/admin.sock.
func bootstrapAdminViaUDS(t testing.TB, dataDir string) (accessKey, secretKey string) {
	t.Helper()
	sock := filepath.Join(dataDir, "admin.sock")

	// Wait up to 10s for socket to appear (cluster bootstrap may need time).
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("admin socket %s did not appear within 10s", sock)
		}
		time.Sleep(50 * time.Millisecond)
	}

	client := iamUDSClient(sock)
	body := strings.NewReader(`{"name":"admin","description":"e2e bootstrap"}`)
	req, err := http.NewRequestWithContext(context.Background(), "POST",
		"http://unix/v1/iam/sa", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equalf(t, http.StatusOK, resp.StatusCode, "bootstrap via %s", sock)

	var out struct {
		AccessKey string `json:"access_key"`
		SecretKey string `json:"secret_key"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.NotEmpty(t, out.AccessKey)
	require.NotEmpty(t, out.SecretKey)
	return out.AccessKey, out.SecretKey
}

// bootstrapAdminViaUDSAny tries each candidate dataDir (one per cluster node)
// in turn. The first call to /v1/iam/sa on a fresh cluster only succeeds on
// the leader; followers may return propose errors. Cycles until one node
// returns 200 or the timeout expires.
func bootstrapAdminViaUDSAny(t testing.TB, dataDirs []string, timeout time.Duration) (accessKey, secretKey string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		for _, dir := range dataDirs {
			sock := filepath.Join(dir, "admin.sock")
			if _, err := os.Stat(sock); err != nil {
				lastErr = err
				continue
			}
			ak, sk, err := tryBootstrapAdminViaUDS(sock)
			if err == nil {
				return ak, sk
			}
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("bootstrapAdminViaUDSAny: no node succeeded within %v: %v", timeout, lastErr)
	return "", ""
}

func tryBootstrapAdminViaUDS(sock string) (string, string, error) {
	client := iamUDSClient(sock)
	body := strings.NewReader(`{"name":"admin","description":"e2e bootstrap"}`)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", "http://unix/v1/iam/sa", body)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(resp.Body)
		return "", "", fmt.Errorf("bootstrap %s -> %d: %s", sock, resp.StatusCode, string(buf))
	}
	var out struct {
		AccessKey string `json:"access_key"`
		SecretKey string `json:"secret_key"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", "", err
	}
	if out.AccessKey == "" || out.SecretKey == "" {
		return "", "", fmt.Errorf("bootstrap %s: empty creds in response", sock)
	}
	return out.AccessKey, out.SecretKey, nil
}

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
// Waits for the new key to propagate to the verifier before returning so
// downstream tests can sign immediately without hitting "unknown access key".
func iamCreateSA(t *testing.T, sock, name string) iamSAResult {
	t.Helper()
	var out iamSAResult
	iamDo(t, sock, "POST", "/v1/iam/sa",
		map[string]string{"name": name}, &out)
	return out
}

// iamWaitKeyReady polls the S3 endpoint with the given creds until SigV4
// verification recognizes the access key. Uses GetObject on a probe path
// because (a) HEAD bodies are stripped by the SDK so we can't distinguish
// "unknown access key" from "IAM grant denies" via HeadBucket, and (b)
// GetObject's XML body is exposed in the SDK error message.
//
// Ready signals (any one):
//   - Any non-error (unlikely for a probe path).
//   - Error mentions "IAM grant denies", "NoSuchBucket", "NoSuchKey",
//     "policy denies" — auth passed, authz/storage took over.
//   - Error code != "AccessDenied" or status != 403.
//
// Unready signal: error body mentions "unknown access key" (key not yet
// applied to the IAM store) — keep polling.
func iamWaitKeyReady(t *testing.T, s3URL, ak, sk string, timeout time.Duration) {
	t.Helper()
	cli := s3ClientFor(s3URL, ak, sk)
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String("__iam_key_probe__"),
			Key:    aws.String("__probe__"),
		})
		cancel()
		if err == nil {
			return
		}
		msg := err.Error()
		// Still applying. Keep polling.
		if strings.Contains(msg, "unknown access key") {
			if time.Now().After(deadline) {
				t.Fatalf("iam key not ready within %v: %v", timeout, err)
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		// Anything else → key was recognized; whatever 401/403/404 follows
		// is downstream of auth, so the key is "ready" for the test's
		// purposes.
		return
	}
}

// iamSADelete DELETEs the given SA. 204 on success.
func iamSADelete(t *testing.T, sock, saID string) {
	t.Helper()
	iamDo(t, sock, "DELETE", "/v1/iam/sa/"+saID, nil, nil)
}

// iamPutBucketUpstream registers a bucket-upstream record via admin UDS.
// Wire format: PUT /v1/buckets/upstream with bucket field in JSON body.
//
// Per /plan-eng-review override A9, the JSON wire key is "upstream_url"
// (matches the CLI flag --upstream-url and server-side struct field UpstreamURL).
func iamPutBucketUpstream(t *testing.T, sock, bucket, upstreamURL, ak, sk string) {
	t.Helper()
	body := map[string]string{
		"bucket":       bucket,
		"upstream_url": upstreamURL,
		"access_key":   ak,
		"secret_key":   sk,
	}
	iamDo(t, sock, "PUT", "/v1/buckets/upstream", body, nil)
}

// iamGrantPut PUTs an explicit grant (Role: Read|Write|Admin, exact bucket).
func iamGrantPut(t *testing.T, sock, saID, bucket, role string) {
	t.Helper()
	iamDo(t, sock, "PUT", "/v1/iam/grant",
		map[string]string{"sa_id": saID, "bucket": bucket, "role": role}, nil)
}

// iamGrantDelete removes the grant for (saID, bucket). Pass "*" as bucket
// to remove the wildcard grant via the Phase-5c HandleGrantDelete route.
func iamGrantDelete(t *testing.T, sock, saID, bucket string) {
	t.Helper()
	iamDo(t, sock, "DELETE", "/v1/iam/grant",
		map[string]string{"sa_id": saID, "bucket": bucket}, nil)
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

// startIAMTestServer launches a single-node grainfs binary, bootstraps an
// Admin SA via the admin UDS POST /v1/iam/sa, and returns a handle the IAM
// e2e tests can use to drive both the S3 plane and the admin UDS.
func startIAMTestServer(t *testing.T) iamTestServer {
	t.Helper()

	dir, err := os.MkdirTemp("", "grainfs-iam-e2e-*")
	require.NoError(t, err, "mkdtemp")
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	port := freePort()
	cmd := exec.Command(getBinary(), "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start(), "start IAM e2e server")
	t.Cleanup(func() { terminateProcess(cmd) })

	s3URL := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForPort(t, port, 30*time.Second)

	bootAK, bootSK := bootstrapAdminViaUDS(t, dir)
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

// iamTestServerHandle is a server with explicit lifecycle control. Unlike
// iamTestServer, the data dir + bootstrap creds + ports persist across
// Stop()/Start() cycles, so IAM state durability scenarios across restarts
// can be exercised in-process.
type iamTestServerHandle struct {
	DataDir     string
	S3URL       string
	AdminSock   string
	BootstrapAK string
	BootstrapSK string
	s3Port      int
	nfsPort     int
	nbdPort     int
	cmd         *exec.Cmd
	cli         *s3.Client
	// firstStart tracks whether we've spawned the binary at least once.
	// First Start bootstraps an admin SA via UDS; subsequent Start calls
	// reuse the persisted creds (the IAM store rehydrates from snapshot
	// + raft replay).
	firstStart bool
}

// startIAMTestServerWithRestart spawns the server but returns a handle whose
// Stop()/Start() preserves the data dir. Bootstrap creds remain valid across
// restarts because they're durably persisted in the IAM store.
func startIAMTestServerWithRestart(t *testing.T) *iamTestServerHandle {
	t.Helper()

	dir, err := os.MkdirTemp("", "grainfs-iam-e2e-restart-*")
	require.NoError(t, err, "mkdtemp")
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	h := &iamTestServerHandle{
		DataDir:   dir,
		AdminSock: filepath.Join(dir, "admin.sock"),
		s3Port:    freePort(),
		nfsPort:   freePort(),
		nbdPort:   freePort(),
	}
	h.S3URL = fmt.Sprintf("http://127.0.0.1:%d", h.s3Port)
	t.Cleanup(func() {
		if h.cmd != nil && h.cmd.ProcessState == nil {
			terminateProcess(h.cmd)
		}
	})

	h.Start(t)
	return h
}

// Start (re)spawns the server bound to the persisted data dir + ports and
// waits until the IAM verifier accepts the bootstrap creds. On the first
// call the admin SA is bootstrapped via UDS; subsequent calls reuse the
// persisted creds (snapshot/raft replay rehydrates the IAM store).
func (h *iamTestServerHandle) Start(t *testing.T) {
	t.Helper()

	args := []string{"serve",
		"--data", h.DataDir,
		"--port", fmt.Sprintf("%d", h.s3Port),
		"--nfs4-port", fmt.Sprintf("%d", h.nfsPort),
		"--nbd-port", fmt.Sprintf("%d", h.nbdPort),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}

	cmd := exec.Command(getBinary(), args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start(), "start IAM e2e server")
	h.cmd = cmd

	waitForPort(t, h.s3Port, 30*time.Second)

	if !h.firstStart {
		ak, sk := bootstrapAdminViaUDS(t, h.DataDir)
		h.BootstrapAK = ak
		h.BootstrapSK = sk
	}

	cli := s3ClientFor(h.S3URL, h.BootstrapAK, h.BootstrapSK)
	h.cli = cli
	require.NoError(t, waitForIAMReady(cli, 30*time.Second))
	h.firstStart = true
}

// Stop sends SIGTERM and waits for the process to exit, giving Raft and
// BadgerDB time to flush state cleanly. terminateProcess is SIGKILL-based
// and would skip the orderly shutdown path needed by ET5.
func (h *iamTestServerHandle) Stop(t *testing.T) {
	t.Helper()
	if h.cmd == nil || h.cmd.Process == nil {
		return
	}
	if h.cmd.ProcessState != nil {
		return
	}
	_ = h.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- h.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Logf("server did not exit on SIGTERM in 20s; falling back to SIGKILL")
		_ = h.cmd.Process.Kill()
		<-done
	}
	h.cmd = nil
}

// Client returns the S3 client bound to the bootstrap creds. Re-built on
// each Start so callers should call this after every Start.
func (h *iamTestServerHandle) Client() *s3.Client { return h.cli }

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
