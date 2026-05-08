package server

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
)

func TestHandleCopyObject_PrivateSource_AnonymousIsDenied(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)

	// Create source bucket.
	createBucket(t, base, "src-bkt")

	// Seed source object with default private ACL.
	require.NoError(t, putWithACL(t, base, "src-bkt", "src-key", []byte("secret"), s3auth.ACLPrivate))
	// Force ACL on backend (PutObject default is ACLPrivate but be explicit for the test contract).
	require.NoError(t, backend.SetObjectACL("src-bkt", "src-key", uint8(s3auth.ACLPrivate)))

	// Anonymous CopyObject from src-bkt/src-key → dst-bkt/dst-key.
	req, err := http.NewRequest(http.MethodPut, base+"/dst-bkt/dst-key", strings.NewReader(""))
	require.NoError(t, err)
	req.Header.Set("x-amz-copy-source", "/src-bkt/src-key")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, http.StatusForbidden, resp.StatusCode,
		"anonymous copy of a private source must be denied; body=%s", string(body))
}

func TestHandleCopyObject_PublicReadSource_IsAllowed(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)

	// Create source and destination buckets.
	createBucket(t, base, "src-bkt")
	createBucket(t, base, "dst-bkt")

	require.NoError(t, putWithACL(t, base, "src-bkt", "src-key", []byte("public"), s3auth.ACLPublicRead))
	require.NoError(t, backend.SetObjectACL("src-bkt", "src-key", uint8(s3auth.ACLPublicRead)))

	req, err := http.NewRequest(http.MethodPut, base+"/dst-bkt/dst-key", strings.NewReader(""))
	require.NoError(t, err)
	req.Header.Set("x-amz-copy-source", "/src-bkt/src-key")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"copy of public-read source must succeed; body=%s", string(body))
}

// createBucket creates a bucket via the S3 API and fails the test on error.
func createBucket(t *testing.T, base, bucket string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, base+"/"+bucket, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "create bucket %s", bucket)
}

// putWithACL is a helper that PUTs an object with the given ACL via the S3 API.
func putWithACL(t *testing.T, base, bucket, key string, body []byte, acl s3auth.ACLGrant) error {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, base+"/"+bucket+"/"+key, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	switch acl {
	case s3auth.ACLPublicRead:
		req.Header.Set("x-amz-acl", "public-read")
	case s3auth.ACLPublicReadWrite:
		req.Header.Set("x-amz-acl", "public-read-write")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT %s/%s returned %d: %s", bucket, key, resp.StatusCode, string(b))
	}
	return nil
}
