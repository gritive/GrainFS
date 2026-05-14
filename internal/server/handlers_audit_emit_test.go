// internal/server/handlers_audit_emit_test.go
package server

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
)

func TestAuditEmitHook_Put(t *testing.T) {
	emitter := audit.NewEmitter("node-test")
	base := setupTestServerWithOptions(t, WithAuditEmitter(emitter))

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

	events := emitter.Ring().DrainInto(nil)
	require.NotEmpty(t, events, "PUT must emit an audit event")
	var found bool
	for _, e := range events {
		if e.Method == "PUT" && e.Bucket == "emit-test-bucket" && e.Key == "hello.txt" {
			found = true
			assert.Equal(t, int32(200), e.Status)
			assert.NotZero(t, e.Ts)
		}
	}
	require.True(t, found, "PUT event for emit-test-bucket/hello.txt must be present")
}

func TestAuditEmitHook_GetAndDelete(t *testing.T) {
	emitter := audit.NewEmitter("node-test")
	base := setupTestServerWithOptions(t, WithAuditEmitter(emitter))

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

	// drain setup events
	emitter.Ring().DrainInto(nil)

	// GET
	resp, err = http.Get(base + "/gd-bucket/obj")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	eventsGet := emitter.Ring().DrainInto(nil)
	var gotGet bool
	for _, e := range eventsGet {
		if e.Method == "GET" && e.Bucket == "gd-bucket" {
			gotGet = true
			assert.Equal(t, int32(200), e.Status)
		}
	}
	require.True(t, gotGet, "GET event expected")

	// DELETE
	req, _ = http.NewRequest(http.MethodDelete, base+"/gd-bucket/obj", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	eventsDel := emitter.Ring().DrainInto(nil)
	var gotDel bool
	for _, e := range eventsDel {
		if e.Method == "DELETE" && e.Bucket == "gd-bucket" {
			gotDel = true
			assert.Equal(t, int32(204), e.Status)
		}
	}
	require.True(t, gotDel, "DELETE event expected")
}

func TestAuditEmitHook_List(t *testing.T) {
	emitter := audit.NewEmitter("node-test")
	base := setupTestServerWithOptions(t, WithAuditEmitter(emitter))

	// create bucket
	req, _ := http.NewRequest(http.MethodPut, base+"/list-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// drain setup events
	emitter.Ring().DrainInto(nil)

	// LIST
	resp, err = http.Get(base + "/list-bucket?list-type=2")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	events := emitter.Ring().DrainInto(nil)
	var found bool
	for _, e := range events {
		if e.Method == "LIST" && e.Bucket == "list-bucket" {
			found = true
			assert.Equal(t, int32(200), e.Status)
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
