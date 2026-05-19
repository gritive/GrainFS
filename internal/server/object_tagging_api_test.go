package server

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Local helpers — shared across tagging HTTP tests (Task 16, 17, 19)
func taggingPutBucket(t *testing.T, url string, sign func(*http.Request), bucket string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url+"/"+bucket, nil)
	require.NoError(t, err)
	sign(req)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func taggingPutObject(t *testing.T, url string, sign func(*http.Request), bucket, key, body string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url+"/"+bucket+"/"+key, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "text/plain")
	sign(req)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func taggingDo(t *testing.T, sign func(*http.Request), req *http.Request) *http.Response {
	t.Helper()
	sign(req)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func TestPutObjectTagging_Roundtrip(t *testing.T) {
	url, sign := setupECAuthServer(t)
	taggingPutBucket(t, url, sign, "b")
	taggingPutObject(t, url, sign, "b", "k", "body")

	body := []byte(`<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>`)
	req, err := http.NewRequest(http.MethodPut, url+"/b/k?tagging", bytes.NewReader(body))
	require.NoError(t, err)
	resp := taggingDo(t, sign, req)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, err = http.NewRequest(http.MethodGet, url+"/b/k?tagging", nil)
	require.NoError(t, err)
	resp = taggingDo(t, sign, req)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	got, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(got), "<Key>env</Key>")
	require.Contains(t, string(got), "<Value>prod</Value>")
}

func TestPutObjectTagging_RejectInvalid(t *testing.T) {
	url, sign := setupECAuthServer(t)
	taggingPutBucket(t, url, sign, "b")
	taggingPutObject(t, url, sign, "b", "k", "body")

	body := []byte(`<Tagging><TagSet><Tag><Key>aws:env</Key><Value>x</Value></Tag></TagSet></Tagging>`)
	req, err := http.NewRequest(http.MethodPut, url+"/b/k?tagging", bytes.NewReader(body))
	require.NoError(t, err)
	resp := taggingDo(t, sign, req)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	got, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(got), "InvalidTag")
}

func TestDeleteObjectTagging_Idempotent(t *testing.T) {
	url, sign := setupECAuthServer(t)
	taggingPutBucket(t, url, sign, "b")
	taggingPutObject(t, url, sign, "b", "k", "body")

	for i := 0; i < 2; i++ {
		req, err := http.NewRequest(http.MethodDelete, url+"/b/k?tagging", nil)
		require.NoError(t, err)
		resp := taggingDo(t, sign, req)
		resp.Body.Close()
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
	}
}
