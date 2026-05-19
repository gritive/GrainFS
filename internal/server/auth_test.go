package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

func setupAuthServer(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err, "NewLocalBackend")
	t.Cleanup(func() { backend.Close() })

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	creds := []s3auth.Credentials{{AccessKey: "testkey", SecretKey: "testsecret"}}
	srv := New(addr, backend, WithAuth(creds))
	go srv.Run()
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "http://" + addr
}

func TestAuthRejectsUnsigned(t *testing.T) {
	base := setupAuthServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "request")
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestAuthAcceptsValidSignature(t *testing.T) {
	// D#8: CreateBucket is admin-UDS-only. Pre-create via backend; verify signed
	// object PUT still works.
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	require.NoError(t, backend.CreateBucket(t.Context(), "mybucket"))

	base := "http://" + func() string {
		port := freePort(t)
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		creds := []s3auth.Credentials{{AccessKey: "testkey", SecretKey: "testsecret"}}
		srv := New(addr, backend, WithAuth(creds))
		go srv.Run()
		for i := 0; i < 50; i++ {
			conn, err2 := net.Dial("tcp", addr)
			if err2 == nil {
				conn.Close()
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		return addr
	}()

	// D#8: CreateBucket via S3 returns 403.
	req, _ := http.NewRequest(http.MethodPut, base+"/newbucket", nil)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "testkey", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "create bucket S3")
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	// Object PUT still works.
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt", bytes.NewReader([]byte("data")))
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "testkey", "testsecret", "us-east-1")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err, "put")
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAuthBucketPolicyCRUDRequiresSignature(t *testing.T) {
	// D#8: bucket policy mutation is admin-UDS-only on the S3 data plane.
	// Both signed and unsigned PUT/DELETE ?policy return 403 unconditionally.
	base := setupAuthServer(t)

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::policy-bucket/*"]}]}`

	// Unsigned PUT policy → 403 (D#8, before any auth check).
	req, _ := http.NewRequest(http.MethodPut, base+"/policy-bucket?policy", bytes.NewReader([]byte(policy)))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "unsigned put policy")
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	// Signed PUT policy → still 403 (D#8 handler fires before policy logic).
	req, _ = http.NewRequest(http.MethodPut, base+"/policy-bucket?policy", bytes.NewReader([]byte(policy)))
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "testkey", "testsecret", "us-east-1")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err, "signed put policy")
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	// Signed DELETE policy → 403 (D#8).
	req, _ = http.NewRequest(http.MethodDelete, base+"/policy-bucket?policy", nil)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "testkey", "testsecret", "us-east-1")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err, "signed delete policy")
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestAuthAcceptsSignedPostPolicyFormUpload(t *testing.T) {
	// D#8: pre-create the bucket via backend; test only the form upload path.
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	require.NoError(t, backend.CreateBucket(t.Context(), "form-auth"))

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	creds := []s3auth.Credentials{{AccessKey: "testkey", SecretKey: "testsecret"}}
	srv := New(addr, backend, WithAuth(creds))
	go srv.Run()
	for i := 0; i < 50; i++ {
		conn, err2 := net.Dial("tcp", addr)
		if err2 == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	base := "http://" + addr

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	require.NoError(t, w.WriteField("key", "uploaded.txt"))
	require.NoError(t, w.WriteField("Content-Type", "text/plain"))
	require.NoError(t, w.WriteField("success_action_status", "201"))
	addSignedPostPolicyFields(t, w, "form-auth", "uploaded.txt", "testkey", "testsecret")
	fw, err := w.CreateFormFile("file", "uploaded.txt")
	require.NoError(t, err)
	_, err = fw.Write([]byte("form upload body"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	req, _ := http.NewRequest(http.MethodPost, base+"/form-auth", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

func addSignedPostPolicyFields(t *testing.T, w *multipart.Writer, bucket, key, accessKey, secretKey string) {
	t.Helper()
	date := time.Now().UTC().Format("20060102")
	credential := accessKey + "/" + date + "/us-east-1/s3/aws4_request"
	policy := map[string]any{
		"expiration": time.Now().UTC().Add(time.Hour).Format("2006-01-02T15:04:05Z"),
		"conditions": []any{
			map[string]string{"bucket": bucket},
			map[string]string{"key": key},
			map[string]string{"Content-Type": "text/plain"},
			map[string]string{"success_action_status": "201"},
			map[string]string{"X-Amz-Credential": credential},
		},
	}
	raw, err := json.Marshal(policy)
	require.NoError(t, err)
	policyB64 := base64.StdEncoding.EncodeToString(raw)
	require.NoError(t, w.WriteField("policy", policyB64))
	require.NoError(t, w.WriteField("X-Amz-Credential", credential))
	require.NoError(t, w.WriteField("X-Amz-Signature", s3auth.SignPostPolicy(policyB64, secretKey, date, "us-east-1", "s3")))
}

func TestAuthRejectsWrongKey(t *testing.T) {
	base := setupAuthServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "testkey", "wrongsecret", "us-east-1")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestNoAuthServerAllowsAll(t *testing.T) {
	// D#8: CreateBucket returns 403 even on a no-auth server. Object reads
	// (GET/HEAD with public-read ACL) are still allowed. Verify the no-auth
	// server still allows object operations on a pre-created bucket.
	base, backend := setupTestServerWithBackend(t)
	mustCreateBucket(t, backend, "mybucket")

	// D#8: CreateBucket → 403.
	req, _ := http.NewRequest(http.MethodPut, base+"/anotherbucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	// Object PUT still works on the pre-created bucket.
	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt", bytes.NewReader([]byte("hello")))
	req.Header.Set("x-amz-acl", "public-read")
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAuthContextKey(t *testing.T) {
	// Test that AccessKeyFromContext returns empty for unauthenticated context
	ctx := t.Context()
	assert.Empty(t, AccessKeyFromContext(ctx))
}

func TestAuthContextKeyRoundTrip(t *testing.T) {
	ctx := t.Context()
	ctx = WithAccessKey(ctx, "user123")
	assert.Equal(t, "user123", AccessKeyFromContext(ctx))
}
