// internal/server/handlers_audit_emit_test.go
package server

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/s3auth"
)

func TestAuditEmitHook_Put(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	base, backend := setupTestServerWithBackend(t, WithAuditOutbox(outbox))
	mustCreateBucket(t, backend, "emit-test-bucket")

	req, _ := http.NewRequest(http.MethodPut, base+"/emit-test-bucket/hello.txt",
		bytes.NewReader([]byte("world")))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	events, err := outbox.Pending(context.Background(), 100)
	require.NoError(t, err)
	require.NotEmpty(t, events, "PUT must emit an audit event")
	var found bool
	for _, e := range events {
		if e.Method == "PUT" && e.Bucket == "emit-test-bucket" && e.Key == "hello.txt" {
			found = true
			assert.Equal(t, int32(200), e.Status)
			assert.Equal(t, "PutObject", e.Operation)
			assert.NotZero(t, e.Ts)
		}
	}
	require.True(t, found, "PUT event for emit-test-bucket/hello.txt must be present")
}

func TestAuditEmitHook_GetAndDelete(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	base, backend := setupTestServerWithBackend(t, WithAuditOutbox(outbox))
	mustCreateBucket(t, backend, "gd-bucket")

	// create object
	req, _ := http.NewRequest(http.MethodPut, base+"/gd-bucket/obj", bytes.NewReader([]byte("data")))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// GET
	resp, err = http.Get(base + "/gd-bucket/obj")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	eventsGet, err := outbox.Pending(context.Background(), 100)
	require.NoError(t, err)
	var gotGet bool
	for _, e := range eventsGet {
		if e.Method == "GET" && e.Bucket == "gd-bucket" && e.Key == "obj" {
			gotGet = true
			assert.Equal(t, int32(200), e.Status)
			assert.Equal(t, "GetObject", e.Operation)
		}
	}
	require.True(t, gotGet, "GET event expected")

	// DELETE
	req, _ = http.NewRequest(http.MethodDelete, base+"/gd-bucket/obj", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	eventsDel, err := outbox.Pending(context.Background(), 100)
	require.NoError(t, err)
	var gotDel bool
	for _, e := range eventsDel {
		if e.Method == "DELETE" && e.Bucket == "gd-bucket" && e.Key == "obj" {
			gotDel = true
			assert.Equal(t, int32(204), e.Status)
			assert.Equal(t, "DeleteObject", e.Operation)
		}
	}
	require.True(t, gotDel, "DELETE event expected")
}

func TestAuditEmitHook_List(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	base, backend := setupTestServerWithBackend(t, WithAuditOutbox(outbox))
	mustCreateBucket(t, backend, "list-bucket")

	// LIST
	resp, err := http.Get(base + "/list-bucket?list-type=2")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	events, err := outbox.Pending(context.Background(), 100)
	require.NoError(t, err)
	var found bool
	for _, e := range events {
		if e.Method == "GET" && e.Bucket == "list-bucket" && e.Key == "" {
			found = true
			assert.Equal(t, int32(200), e.Status)
			assert.Equal(t, "ListObjects", e.Operation)
		}
	}
	require.True(t, found, "LIST event expected")
}

func TestAuditEmitHook_NoEmitterNoPanic(t *testing.T) {
	// server without audit emitter must not panic when a PUT (→ 403) is handled.
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/nopanic-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	// D#8: 403 is fine here — we only care that there's no panic.
}

func TestAuditEmitterFallback_Put(t *testing.T) {
	emitter := audit.NewEmitter("node-test")
	base, backend := setupTestServerWithBackend(t, WithAuditEmitter(emitter))
	mustCreateBucket(t, backend, "ring-emit-bucket")

	req, err := http.NewRequest(http.MethodPut, base+"/ring-emit-bucket/hello.txt", bytes.NewReader([]byte("hello")))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	events := emitter.Ring().DrainInto(nil)
	for _, ev := range events {
		if ev.Operation == "PutObject" && ev.Bucket == "ring-emit-bucket" && ev.Key == "hello.txt" {
			require.Equal(t, int32(http.StatusOK), ev.Status)
			require.Equal(t, "allow", ev.AuthStatus)
			return
		}
	}
	t.Fatal("WithAuditEmitter fallback event not found")
}

func TestAuditEnvelope_RecordsHeadAndRequestID(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	base, backend := setupTestServerWithBackend(t, WithAuditOutbox(outbox))
	mustCreateBucket(t, backend, "audit-head-bucket")

	req, _ := http.NewRequest(http.MethodPut, base+"/audit-head-bucket/obj", bytes.NewReader([]byte("body")))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodHead, base+"/audit-head-bucket/obj", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	requestID := resp.Header.Get("x-amz-request-id")
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotEmpty(t, requestID)

	events, err := outbox.Pending(context.Background(), 100)
	require.NoError(t, err)
	var found bool
	for _, ev := range events {
		if ev.Operation == "HeadObject" && ev.Bucket == "audit-head-bucket" && ev.Key == "obj" {
			found = true
			require.Equal(t, requestID, ev.RequestID)
			require.Equal(t, int32(http.StatusOK), ev.Status)
			require.Equal(t, "allow", ev.AuthStatus)
		}
	}
	require.True(t, found, "HEAD must be recorded by audit envelope")
}

func TestAuditEnvelope_RecordsNodeID(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	base, backend := setupTestServerWithBackend(t, WithAuditOutbox(outbox), WithAuditNodeID("node-a"))
	mustCreateBucket(t, backend, "audit-node-bucket")

	req, _ := http.NewRequest(http.MethodPut, base+"/audit-node-bucket/obj", bytes.NewReader([]byte("body")))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	events, err := outbox.Pending(context.Background(), 100)
	require.NoError(t, err)
	for _, ev := range events {
		if ev.Bucket == "audit-node-bucket" && ev.Key == "obj" {
			require.Equal(t, "node-a", ev.NodeID)
			return
		}
	}
	t.Fatal("PUT object audit event not found")
}

func TestAuditEnvelope_TruncatesOversizedUserAgentForWire(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	// D#8: bucket create returns 403; audit envelope must still record the event.
	base := setupTestServerWithOptions(t, WithAuditOutbox(outbox))

	req, err := http.NewRequest(http.MethodPut, base+"/audit-ua-bucket", nil)
	require.NoError(t, err)
	req.Header.Set("User-Agent", strings.Repeat("a", 70000))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	// 403 is expected (D#8); the test checks User-Agent truncation in the audit event.

	events, err := outbox.Pending(context.Background(), 100)
	require.NoError(t, err)
	for _, ev := range events {
		if ev.Bucket == "audit-ua-bucket" {
			require.LessOrEqual(t, len(ev.UserAgent), auditString16MaxBytes)
			_, err := audit.EncodeS3Batch([]audit.S3Event{ev})
			require.NoError(t, err)
			return
		}
	}
	t.Fatal("oversized user-agent audit event not found")
}

func TestAuditEnvelope_RecordsFormPostObjectKey(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	base, backend := setupTestServerWithBackend(t, WithAuditOutbox(outbox))
	mustCreateBucket(t, backend, "audit-form-bucket")

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	require.NoError(t, w.WriteField("key", "form/path/uploaded.txt"))
	require.NoError(t, w.WriteField("Content-Type", "text/plain"))
	fw, err := w.CreateFormFile("file", "uploaded.txt")
	require.NoError(t, err)
	_, err = fw.Write([]byte("form upload body"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	req, err := http.NewRequest(http.MethodPost, base+"/audit-form-bucket", &buf)
	require.NoError(t, err)
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	events, err := outbox.Pending(context.Background(), 100)
	require.NoError(t, err)
	for _, ev := range events {
		if ev.Operation == "PostObject" && ev.Bucket == "audit-form-bucket" {
			require.Equal(t, "form/path/uploaded.txt", ev.Key)
			return
		}
	}
	t.Fatal("form POST audit event not found")
}

func TestAuditEnvelope_RecordsStreamedGetBytesOut(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	base, backend := setupTestServerWithBackend(t, WithAuditOutbox(outbox))
	mustCreateBucket(t, backend, "audit-stream-bucket")

	body := bytes.Repeat([]byte("x"), 20*1024)
	req, err := http.NewRequest(http.MethodPut, base+"/audit-stream-bucket/large.bin", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("x-amz-acl", "public-read")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = http.Get(base + "/audit-stream-bucket/large.bin")
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, err = http.NewRequest(http.MethodGet, base+"/audit-stream-bucket/large.bin", nil)
	require.NoError(t, err)
	req.Header.Set("Range", "bytes=2-5")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)

	events, err := outbox.Pending(context.Background(), 100)
	require.NoError(t, err)
	var gotFull, gotRange bool
	for _, ev := range events {
		if ev.Operation != "GetObject" || ev.Bucket != "audit-stream-bucket" || ev.Key != "large.bin" {
			continue
		}
		switch ev.Status {
		case http.StatusOK:
			gotFull = true
			require.Equal(t, int64(len(body)), ev.BytesOut)
		case http.StatusPartialContent:
			gotRange = true
			require.Equal(t, int64(4), ev.BytesOut)
		}
	}
	require.True(t, gotFull, "full streamed GET audit event not found")
	require.True(t, gotRange, "range streamed GET audit event not found")
}

func TestAuditEnvelope_RecordsAuthFailure(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	base := setupTestServerWithOptions(t,
		WithAuth([]s3auth.Credentials{{AccessKey: "AK", SecretKey: "correct"}}),
		WithAuditOutbox(outbox),
	)

	req, err := http.NewRequest(http.MethodPut, base+"/auth-fail-bucket/obj", bytes.NewReader([]byte("body")))
	require.NoError(t, err)
	s3auth.SignRequest(req, "AK", "wrong", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	events, err := outbox.Pending(context.Background(), 100)
	require.NoError(t, err)
	for _, ev := range events {
		if ev.Bucket == "auth-fail-bucket" && ev.Key == "obj" {
			require.Equal(t, int32(http.StatusForbidden), ev.Status)
			require.Equal(t, "deny", ev.AuthStatus)
			require.Equal(t, "authn", ev.ErrReason)
			return
		}
	}
	t.Fatal("auth failure audit event not found")
}

func TestAuditEnvelope_DoesNotMarkNotFoundAsAuthDeny(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	base, backend := setupTestServerWithBackend(t, WithAuditOutbox(outbox))
	mustCreateBucket(t, backend, "missing-audit-bucket")

	resp, err := http.Get(base + "/missing-audit-bucket/no-such-key")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	events, err := outbox.Pending(context.Background(), 100)
	require.NoError(t, err)
	for _, ev := range events {
		if ev.Operation == "GetObject" && ev.Bucket == "missing-audit-bucket" && ev.Key == "no-such-key" {
			require.Equal(t, int32(http.StatusNotFound), ev.Status)
			require.NotEqual(t, "deny", ev.AuthStatus)
			return
		}
	}
	t.Fatal("GET not found audit event not found")
}
