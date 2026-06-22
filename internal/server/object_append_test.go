package server

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
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

func TestAppendObjectReturnsFinalObjectSizeHeader(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)
	mustCreateBucket(t, backend, "b")

	req, _ := http.NewRequest(http.MethodPut, base+"/b/k", bytes.NewReader([]byte("hello")))
	req.Header.Set(appendOffsetHeader, "0")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "5", resp.Header.Get(appendSizeHeader))

	req, _ = http.NewRequest(http.MethodPut, base+"/b/k", bytes.NewReader([]byte("world")))
	req.Header.Set(appendOffsetHeader, "5")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "10", resp.Header.Get(appendSizeHeader))
}

func TestAppendObjectDecodesStreamingTrailerBody(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)
	mustCreateBucket(t, backend, "b")

	body := "5;chunk-signature=abc\r\nhello\r\n0;chunk-signature=def\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n"
	req, _ := http.NewRequest(http.MethodPut, base+"/b/k", strings.NewReader(body))
	req.Header.Set(appendOffsetHeader, "0")
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "5", resp.Header.Get(appendSizeHeader))

	rc, _, err := backend.GetObject(t.Context(), "b", "k")
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, "hello", string(got))
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

// TestAppendObjectGateUsesLinearizedRead guards that the AppendObject 501
// feature-gate is a MUTATING edge: it must resolve bucket versioning via the
// LINEARIZED read (#839), not the plain local read. A just-joined group-0
// follower whose local versioning replica lags (~90s) would otherwise read
// "Unversioned" for an Enabled bucket and let the append bypass the 501 gate.
// recordingVersioner returns "Enabled" from both reads, so the gate fires (501)
// regardless; the call-count is the discriminator (same technique as
// TestEdgeResolverSelection).
func TestAppendObjectGateUsesLinearizedRead(t *testing.T) {
	real, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	rv := &recordingVersioner{Backend: real}
	srv := New("127.0.0.1:0", rv)

	c := app.NewContext(0)
	c.Request.Header.Set(appendOffsetHeader, "0")
	handled := srv.appendObject(t.Context(), c, "b", "k")

	require.True(t, handled)
	require.Equal(t, http.StatusNotImplemented, c.Response.StatusCode())
	require.Equal(t, 1, rv.linCalls, "append 501-gate must use the linearized read")
	require.Equal(t, 0, rv.plainCalls, "append 501-gate must NOT use the plain (stale-prone) read")
}

// NOTE: TestAppendableObjectOverwriteByPlainPut removed — equivalent SDK
// coverage already exists in tests/e2e/append_object_test.go as
// TestAppendObjectE2E/{SingleNode,Cluster4Node}/PlainPutOverwritesAppendable.

// NOTE: TestAppendObjectTooLargeResponse — ErrAppendObjectTooLarge → 400 EntityTooLarge
// covered end-to-end in tests/e2e/append_size_cap_test.go (TestAppendSizeCapE2E).
