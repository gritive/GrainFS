package storage_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func ctx() context.Context          { return context.Background() }
func body(s string) *strings.Reader { return strings.NewReader(s) }

func TestLocalBackend_SetObjectTags_CurrentVersion(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))
	_, err := b.PutObject(ctx(), "b", "k", body("hello"), "text/plain")
	require.NoError(t, err)

	tags := []storage.Tag{{Key: "env", Value: "prod"}}
	require.NoError(t, b.SetObjectTags("b", "k", "", tags))

	got, err := b.GetObjectTags("b", "k", "")
	require.NoError(t, err)
	require.Equal(t, tags, got)
}

func TestLocalBackend_SetObjectTags_NoSuchKey(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))
	err := b.SetObjectTags("b", "missing", "", []storage.Tag{{Key: "k", Value: "v"}})
	require.Error(t, err)
}

func TestLocalBackend_SetObjectTags_ClearsOnNil(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))
	_, err := b.PutObject(ctx(), "b", "k", body("hello"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, b.SetObjectTags("b", "k", "", []storage.Tag{{Key: "k", Value: "v"}}))
	require.NoError(t, b.SetObjectTags("b", "k", "", nil))

	got, err := b.GetObjectTags("b", "k", "")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestLocalBackend_SetObjectTags_DoesNotChangeETag(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))
	pre, err := b.PutObject(ctx(), "b", "k", body("hello"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, b.SetObjectTags("b", "k", "", []storage.Tag{{Key: "x", Value: "y"}}))

	post, err := b.HeadObject(ctx(), "b", "k")
	require.NoError(t, err)
	require.Equal(t, pre.ETag, post.ETag)
	require.Equal(t, pre.LastModified, post.LastModified)
}

func TestCachedBackend_SetObjectTags(t *testing.T) {
	inner := newBackend(t)
	require.NoError(t, inner.CreateBucket(ctx(), "b"))
	_, err := inner.PutObject(ctx(), "b", "k", body("body"), "text/plain")
	require.NoError(t, err)

	cached := storage.NewCachedBackend(inner)
	tags := []storage.Tag{{Key: "team", Value: "platform"}}
	require.NoError(t, cached.SetObjectTags("b", "k", "", tags))

	got, err := inner.GetObjectTags("b", "k", "")
	require.NoError(t, err)
	require.Equal(t, tags, got)
}

func TestRecoveryWriteGate_SetObjectTags_Blocks(t *testing.T) {
	t.Skip("wire to the existing gate-closed harness; mirror SetObjectACL gate test exactly")
}
