package server

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// putBucketVersioning enables versioning on the bucket.
func putBucketVersioning(t *testing.T, url string, sign func(*http.Request), bucket string) {
	t.Helper()
	body := []byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`)
	req, err := http.NewRequest(http.MethodPut, url+"/"+bucket+"?versioning", bytes.NewReader(body))
	require.NoError(t, err)
	resp := taggingDo(t, sign, req)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// putObjectAndReturnVersionID PUTs and reads x-amz-version-id from the response.
func putObjectAndReturnVersionID(t *testing.T, url string, sign func(*http.Request), bucket, key, body string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url+"/"+bucket+"/"+key, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "text/plain")
	resp := taggingDo(t, sign, req)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return resp.Header.Get("x-amz-version-id")
}

// putTagging PUTs a single-tag XML body, optionally for a specific versionId.
func putTagging(t *testing.T, url string, sign func(*http.Request), bucket, key, versionID, k, v string) {
	t.Helper()
	body := []byte(`<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>` + k + `</Key><Value>` + v + `</Value></Tag></TagSet></Tagging>`)
	target := url + "/" + bucket + "/" + key + "?tagging"
	if versionID != "" {
		target += "&versionId=" + versionID
	}
	req, err := http.NewRequest(http.MethodPut, target, bytes.NewReader(body))
	require.NoError(t, err)
	resp := taggingDo(t, sign, req)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// getTaggingBody returns the response body of GET ?tagging for the target.
func getTaggingBody(t *testing.T, url string, sign func(*http.Request), bucket, key, versionID string) string {
	t.Helper()
	target := url + "/" + bucket + "/" + key + "?tagging"
	if versionID != "" {
		target += "&versionId=" + versionID
	}
	req, err := http.NewRequest(http.MethodGet, target, nil)
	require.NoError(t, err)
	resp := taggingDo(t, sign, req)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	got, _ := io.ReadAll(resp.Body)
	return string(got)
}

func TestTagging_VersionedBucket_PerVersion(t *testing.T) {
	t.Skip("LocalBackend does not implement BucketVersioner; per-version tags covered by Task 21 E2E Cluster4Node fixture")
	url, sign, _ := setupECAuthServer(t)
	taggingPutBucket(t, url, sign, "b")
	putBucketVersioning(t, url, sign, "b")

	v1 := putObjectAndReturnVersionID(t, url, sign, "b", "k", "v1")
	putTagging(t, url, sign, "b", "k", "", "v", "1")
	_ = putObjectAndReturnVersionID(t, url, sign, "b", "k", "v2")
	putTagging(t, url, sign, "b", "k", "", "v", "2")

	require.Contains(t, getTaggingBody(t, url, sign, "b", "k", v1), "<Value>1</Value>")
	require.Contains(t, getTaggingBody(t, url, sign, "b", "k", ""), "<Value>2</Value>")
}

func TestTagging_ETagUnchanged(t *testing.T) {
	url, sign, _ := setupECAuthServer(t)
	taggingPutBucket(t, url, sign, "b")
	taggingPutObject(t, url, sign, "b", "k", "body")

	// Read ETag before tagging.
	req, err := http.NewRequest(http.MethodHead, url+"/b/k", nil)
	require.NoError(t, err)
	resp := taggingDo(t, sign, req)
	resp.Body.Close()
	preETag := resp.Header.Get("ETag")
	require.NotEmpty(t, preETag)

	putTagging(t, url, sign, "b", "k", "", "x", "y")

	req, err = http.NewRequest(http.MethodHead, url+"/b/k", nil)
	require.NoError(t, err)
	resp = taggingDo(t, sign, req)
	resp.Body.Close()
	require.Equal(t, preETag, resp.Header.Get("ETag"))
}
