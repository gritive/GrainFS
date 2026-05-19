package server

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSSEPutHeadGetCopyRoundTrip(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)
	mustCreateBucket(t, backend, "bucket")

	req, _ := http.NewRequest(http.MethodPut, base+"/bucket/src.txt", bytes.NewReader([]byte("data")))
	req.Header.Set("x-amz-acl", "public-read")
	req.Header.Set("x-amz-server-side-encryption", "AES256")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "AES256", resp.Header.Get("x-amz-server-side-encryption"))

	req, _ = http.NewRequest(http.MethodHead, base+"/bucket/src.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "AES256", resp.Header.Get("x-amz-server-side-encryption"))

	resp, err = http.Get(base + "/bucket/src.txt")
	require.NoError(t, err)
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "AES256", resp.Header.Get("x-amz-server-side-encryption"))

	req, _ = http.NewRequest(http.MethodPut, base+"/bucket/copy.txt", nil)
	req.Header.Set("x-amz-acl", "public-read")
	req.Header.Set("x-amz-copy-source", "/bucket/src.txt")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "AES256", resp.Header.Get("x-amz-server-side-encryption"))

	req, _ = http.NewRequest(http.MethodHead, base+"/bucket/copy.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "AES256", resp.Header.Get("x-amz-server-side-encryption"))
}

func TestSSEUnsupportedHeadersFailClosed(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)
	mustCreateBucket(t, backend, "bucket")

	for _, tc := range []struct {
		name       string
		headers    map[string]string
		wantStatus int
		wantCode   string
	}{
		{
			name: "kms",
			headers: map[string]string{
				"x-amz-server-side-encryption":                "aws:kms",
				"x-amz-server-side-encryption-aws-kms-key-id": "key-1",
			},
			wantStatus: http.StatusNotImplemented,
			wantCode:   "NotImplemented",
		},
		{
			name: "sse-c",
			headers: map[string]string{
				"x-amz-server-side-encryption-customer-algorithm": "AES256",
				"x-amz-server-side-encryption-customer-key":       "secret",
			},
			wantStatus: http.StatusNotImplemented,
			wantCode:   "NotImplemented",
		},
		{
			name: "invalid",
			headers: map[string]string{
				"x-amz-server-side-encryption": "DES",
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidArgument",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPut, base+"/bucket/"+tc.name+".txt", bytes.NewReader([]byte("data")))
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			require.Equal(t, tc.wantStatus, resp.StatusCode)
			require.Contains(t, string(body), tc.wantCode)

			req, _ = http.NewRequest(http.MethodHead, base+"/bucket/"+tc.name+".txt", nil)
			resp, err = http.DefaultClient.Do(req)
			require.NoError(t, err)
			resp.Body.Close()
			require.Equal(t, http.StatusNotFound, resp.StatusCode)
		})
	}
}
