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
	base, backend := setupTestServerWithBackend(t)
	mustCreateBucket(t, backend, "b")

	req, _ := http.NewRequest(http.MethodPut, base+"/b/k", bytes.NewReader([]byte("hello")))
	req.Header.Set(appendOffsetHeader, "abc")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "InvalidArgument")
}

// Red 19: writing with a wrong offset against an existing appendable object →
// 400 InvalidWriteOffset XML.
func TestAppendObjectInvalidWriteOffsetResponse(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)
	mustCreateBucket(t, backend, "b")

	// Initial append at offset 0 — creates a 5-byte appendable object.
	req, _ := http.NewRequest(http.MethodPut, base+"/b/k", bytes.NewReader([]byte("hello")))
	req.Header.Set(appendOffsetHeader, "0")
	resp, err := http.DefaultClient.Do(req)
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
	base, b := setupECTestServer(t)
	require.NoError(t, b.CreateBucket(t.Context(), "ver-bucket"))

	enableXML := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	req, _ := http.NewRequest(http.MethodPut, base+"/ver-bucket?versioning", strings.NewReader(enableXML))
	resp, err := http.DefaultClient.Do(req)
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

// NOTE: TestAppendableObjectOverwriteByPlainPut removed — equivalent SDK
// coverage already exists in tests/e2e/append_object_test.go as
// TestAppendObjectE2E/{SingleNode,Cluster4Node}/PlainPutOverwritesAppendable.

// NOTE: TestAppendObjectTooLargeResponse — ErrAppendObjectTooLarge → 400 EntityTooLarge
// covered end-to-end in tests/e2e/append_size_cap_test.go (TestAppendSizeCapE2E).
