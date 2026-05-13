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
	base := setupAuthServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "testkey", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "request")
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodPut, base+"/mybucket/file.txt", bytes.NewReader([]byte("data")))
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "testkey", "testsecret", "us-east-1")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err, "put")
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAuthAcceptsSignedPostPolicyFormUpload(t *testing.T) {
	base := setupAuthServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/form-auth", nil)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "testkey", "testsecret", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "create bucket")
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

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

	req, _ = http.NewRequest(http.MethodPost, base+"/form-auth", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err = http.DefaultClient.Do(req)
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
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/mybucket", nil)
	resp, _ := http.DefaultClient.Do(req)
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
