package server

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupECAuthServer returns a stub that skips the test. ACL E2E coverage
// relies on ECBackend's ACLSetter; DistributedBackend ACL port is a
// post-unification follow-up (tracked in TODOS.md). Re-enable this helper
// once SetObjectACL is Raft-replicated.
func setupECAuthServer(t *testing.T) (baseURL string, sign func(*http.Request)) {
	t.Helper()
	t.Skip("ACL support on DistributedBackend is a post-unification follow-up")
	return "", func(*http.Request) {}
}

// TestACL_PublicRead_AnonymousGetAllowed: PUT with x-amz-acl:public-read → anonymous GET → 200
func TestACL_PublicRead_AnonymousGetAllowed(t *testing.T) {
	base, sign := setupECAuthServer(t)

	// Create bucket
	req, _ := http.NewRequest(http.MethodPut, base+"/testbucket", nil)
	sign(req)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// PUT object with public-read ACL
	body := []byte("hello world")
	req, _ = http.NewRequest(http.MethodPut, base+"/testbucket/public.txt", bytes.NewReader(body))
	req.Header.Set("x-amz-acl", "public-read")
	sign(req)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Anonymous GET → should succeed (public-read ACL)
	req, _ = http.NewRequest(http.MethodGet, base+"/testbucket/public.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	data, _ := io.ReadAll(resp.Body)
	assert.Equal(t, body, data)
}

// TestACL_Private_AnonymousGetDenied: private object → anonymous GET → 403
func TestACL_Private_AnonymousGetDenied(t *testing.T) {
	base, sign := setupECAuthServer(t)

	// Create bucket + PUT private object
	req, _ := http.NewRequest(http.MethodPut, base+"/testbucket", nil)
	sign(req)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodPut, base+"/testbucket/private.txt", bytes.NewReader([]byte("secret")))
	sign(req)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Anonymous GET → 403
	req, _ = http.NewRequest(http.MethodGet, base+"/testbucket/private.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// TestACL_PublicRead_AuthenticatedGetAllowed: public-read object → authenticated GET → 200
func TestACL_PublicRead_AuthenticatedGetAllowed(t *testing.T) {
	base, sign := setupECAuthServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/testbucket", nil)
	sign(req)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	body := []byte("public data")
	req, _ = http.NewRequest(http.MethodPut, base+"/testbucket/pub.txt", bytes.NewReader(body))
	req.Header.Set("x-amz-acl", "public-read")
	sign(req)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Authenticated GET still works
	req, _ = http.NewRequest(http.MethodGet, base+"/testbucket/pub.txt", nil)
	sign(req)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
