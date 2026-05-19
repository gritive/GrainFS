package server

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetBucketObjectLockConfiguration_Enabled(t *testing.T) {
	url, sign, backend := setupECAuthServer(t)
	taggingPutBucket(t, backend, "b")

	req, err := http.NewRequest(http.MethodGet, url+"/b?object-lock", nil)
	require.NoError(t, err)
	resp := taggingDo(t, sign, req)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "<ObjectLockEnabled>Enabled</ObjectLockEnabled>")
}

func TestPutObjectRetention_DoesNotOverwriteObject(t *testing.T) {
	url, sign, backend := setupECAuthServer(t)
	taggingPutBucket(t, backend, "b")
	taggingPutObject(t, url, sign, "b", "k", "original")

	body := []byte(`<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-02T03:04:05Z</RetainUntilDate></Retention>`)
	req, err := http.NewRequest(http.MethodPut, url+"/b/k?retention", bytes.NewReader(body))
	require.NoError(t, err)
	resp := taggingDo(t, sign, req)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, err = http.NewRequest(http.MethodGet, url+"/b/k", nil)
	require.NoError(t, err)
	resp = taggingDo(t, sign, req)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	got, _ := io.ReadAll(resp.Body)
	require.Equal(t, "original", strings.TrimSpace(string(got)))
}
