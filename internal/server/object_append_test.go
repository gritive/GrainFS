package server

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Red 18: invalid x-amz-write-offset-bytes header → 400 InvalidArgument.
func TestAppendObjectRejectsInvalidOffsetHeader(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/b", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, base+"/b/k", bytes.NewReader([]byte("hello")))
	req.Header.Set(appendOffsetHeader, "abc")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "InvalidArgument")
}

// Red 19: writing with a wrong offset against an existing appendable object →
// 400 InvalidWriteOffset XML.
func TestAppendObjectInvalidWriteOffsetResponse(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/b", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// Initial append at offset 0 — creates a 5-byte appendable object.
	req, _ = http.NewRequest(http.MethodPut, base+"/b/k", bytes.NewReader([]byte("hello")))
	req.Header.Set(appendOffsetHeader, "0")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "initial append must succeed")

	// Second append with a wrong offset must be rejected.
	req, _ = http.NewRequest(http.MethodPut, base+"/b/k", bytes.NewReader([]byte("world")))
	req.Header.Set(appendOffsetHeader, "99")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "<Code>InvalidWriteOffset</Code>")
}

// Red 20: AppendObject against a versioning-enabled bucket is rejected with 501.
func TestAppendObjectVersioningBucketRejected(t *testing.T) {
	base := setupECTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/ver-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	enableXML := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket?versioning", strings.NewReader(enableXML))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodPut, base+"/ver-bucket/k", bytes.NewReader([]byte("h")))
	req.Header.Set(appendOffsetHeader, "0")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}

// Red 21: a plain PUT (no x-amz-write-offset-bytes header) overwrites a
// previously appendable object — the append-mode flag must not lock subsequent
// regular writes out.
func TestAppendableObjectOverwriteByPlainPut(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/b", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// Seed via AppendObject path.
	req, _ = http.NewRequest(http.MethodPut, base+"/b/k", bytes.NewReader([]byte("aaaa")))
	req.Header.Set(appendOffsetHeader, "0")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Plain PUT must overwrite cleanly — no header, no offset semantics.
	req, _ = http.NewRequest(http.MethodPut, base+"/b/k", bytes.NewReader([]byte("zzz")))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "plain PUT must succeed against an appendable object")

	// HEAD reports the new (smaller) size.
	req, _ = http.NewRequest(http.MethodHead, base+"/b/k", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "3", resp.Header.Get("Content-Length"))
}
