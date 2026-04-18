package erasure

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
)

func TestSetObjectACL_RoundTrip(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))
	_, err := b.PutObject("bucket", "key", strings.NewReader("data"), "text/plain")
	require.NoError(t, err)

	// Default ACL should be private
	obj, err := b.HeadObject("bucket", "key")
	require.NoError(t, err)
	assert.Equal(t, s3auth.ACLPrivate, s3auth.ACLGrant(obj.ACL))

	// Set public-read
	require.NoError(t, b.SetObjectACL("bucket", "key", uint8(s3auth.ACLPublicRead)))

	obj, err = b.HeadObject("bucket", "key")
	require.NoError(t, err)
	assert.Equal(t, s3auth.ACLPublicRead, s3auth.ACLGrant(obj.ACL))
}

func TestGetObject_ReturnsACL(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))
	_, err := b.PutObject("bucket", "k", strings.NewReader("hi"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, b.SetObjectACL("bucket", "k", uint8(s3auth.ACLPublicReadWrite)))

	rc, obj, err := b.GetObject("bucket", "k")
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, s3auth.ACLPublicReadWrite, s3auth.ACLGrant(obj.ACL))
}

func TestSetObjectACL_NotFound(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))
	err := b.SetObjectACL("bucket", "nonexistent", uint8(s3auth.ACLPublicRead))
	assert.Error(t, err)
}

func TestPutObject_ACLPreservedOnOverwrite(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBucket("bucket"))
	_, err := b.PutObject("bucket", "k", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, b.SetObjectACL("bucket", "k", uint8(s3auth.ACLPublicRead)))

	// Overwrite — ACL is reset to private (new object, new metadata)
	_, err = b.PutObject("bucket", "k", strings.NewReader("v2"), "text/plain")
	require.NoError(t, err)

	obj, err := b.HeadObject("bucket", "k")
	require.NoError(t, err)
	assert.Equal(t, s3auth.ACLPrivate, s3auth.ACLGrant(obj.ACL), "overwrite resets ACL to private")
}
