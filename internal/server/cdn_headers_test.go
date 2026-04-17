package server

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetObject_CacheControlHeader verifies that getObject returns Cache-Control
// header so CDN edges know how long to cache the object.
func TestGetObject_CacheControlHeader(t *testing.T) {
	base := setupTestServer(t)

	// Create bucket and upload object
	req, _ := http.NewRequest(http.MethodPut, base+"/cdn-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := bytes.NewReader([]byte("cdn content"))
	req, _ = http.NewRequest(http.MethodPut, base+"/cdn-bucket/asset.js", body)
	req.Header.Set("Content-Type", "application/javascript")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// GET — Cache-Control must be present
	resp, err = http.Get(base + "/cdn-bucket/asset.js")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	cc := resp.Header.Get("Cache-Control")
	assert.NotEmpty(t, cc, "Cache-Control header must be present for CDN origin use")
	assert.True(t, strings.Contains(cc, "max-age="),
		"Cache-Control must include max-age directive, got: %q", cc)
}

// TestHeadObject_CacheControlHeader verifies that headObject also returns Cache-Control.
func TestHeadObject_CacheControlHeader(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/cdn-bucket2", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := bytes.NewReader([]byte("head content"))
	req, _ = http.NewRequest(http.MethodPut, base+"/cdn-bucket2/file.css", body)
	req.Header.Set("Content-Type", "text/css")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodHead, base+"/cdn-bucket2/file.css", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	cc := resp.Header.Get("Cache-Control")
	assert.NotEmpty(t, cc, "Cache-Control header must be present on HEAD for CDN origin use")
}

// TestGetObject_ConditionalGet_ETag verifies 304 Not Modified when ETag matches.
func TestGetObject_ConditionalGet_ETag(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/etag-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := bytes.NewReader([]byte("etag test content"))
	req, _ = http.NewRequest(http.MethodPut, base+"/etag-bucket/obj.txt", body)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Initial GET — grab ETag
	resp, err = http.Get(base + "/etag-bucket/obj.txt")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	etag := resp.Header.Get("ETag")
	require.NotEmpty(t, etag, "ETag must be present")

	// Conditional GET with matching ETag → 304
	req, _ = http.NewRequest(http.MethodGet, base+"/etag-bucket/obj.txt", nil)
	req.Header.Set("If-None-Match", etag)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, http.StatusNotModified, resp.StatusCode,
		"If-None-Match with matching ETag must return 304")

	// ETag must still be present on 304
	assert.Equal(t, etag, resp.Header.Get("ETag"),
		fmt.Sprintf("ETag must be present on 304, got headers: %v", resp.Header))
}
