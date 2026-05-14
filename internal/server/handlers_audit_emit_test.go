// internal/server/handlers_audit_emit_test.go
package server

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
)

func TestAuditEmitHook_Put(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	base := setupTestServerWithOptions(t, WithAuditOutbox(outbox))

	req, _ := http.NewRequest(http.MethodPut, base+"/emit-test-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodPut, base+"/emit-test-bucket/hello.txt",
		bytes.NewReader([]byte("world")))
	resp, err = http.DefaultClient.Do(req)
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
	base := setupTestServerWithOptions(t, WithAuditOutbox(outbox))

	// create bucket and object
	req, _ := http.NewRequest(http.MethodPut, base+"/gd-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	req, _ = http.NewRequest(http.MethodPut, base+"/gd-bucket/obj", bytes.NewReader([]byte("data")))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err = http.DefaultClient.Do(req)
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
	base := setupTestServerWithOptions(t, WithAuditOutbox(outbox))

	// create bucket
	req, _ := http.NewRequest(http.MethodPut, base+"/list-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// LIST
	resp, err = http.Get(base + "/list-bucket?list-type=2")
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
	// server without audit emitter must not panic
	base := setupTestServerWithOptions(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/nopanic-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	// just verifying no panic occurs
}

func TestAuditEnvelope_RecordsHeadAndRequestID(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	base := setupTestServerWithOptions(t, WithAuditOutbox(outbox))

	req, _ := http.NewRequest(http.MethodPut, base+"/audit-head-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodPut, base+"/audit-head-bucket/obj", bytes.NewReader([]byte("body")))
	req.Header.Set("x-amz-acl", "public-read")
	resp, err = http.DefaultClient.Do(req)
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
