package wal_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/wal"
)

// TestWALBackend_CreateMultipartUploadWithTags_DelegatesToInner verifies that
// wal.Backend explicitly forwards CreateMultipartUploadWithTags to its inner
// backend. Regression guard for Phase 2 v0.0.267.0: Operations dispatches via
// `(tagsCreator)` type assertion, and an embedded storage.Backend field does
// NOT promote methods from the underlying concrete type. Without an explicit
// pass-through on wal.Backend, the assertion fails on the wrapper and
// Operations silently falls back to CreateMultipartUpload — dropping
// x-amz-tagging on multipart-initiate in the production hot path.
func TestWALBackend_CreateMultipartUploadWithTags_DelegatesToInner(t *testing.T) {
	root := t.TempDir()
	inner, err := storage.NewLocalBackend(root)
	require.NoError(t, err)

	w, err := wal.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })

	wrapped := wal.NewBackend(inner, w)

	// The type assertion that Operations.CreateMultipartUploadWithTags performs
	// must succeed on the wal.Backend wrapper. This is the bug: without the
	// pass-through, ok is false.
	tc, ok := any(wrapped).(interface {
		CreateMultipartUploadWithTags(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (string, error)
	})
	require.True(t, ok, "wal.Backend must expose CreateMultipartUploadWithTags so Operations' type assertion reaches inner")

	ctx := context.Background()
	require.NoError(t, wrapped.CreateBucket(ctx, "b"))

	tags := []storage.Tag{
		{Key: "env", Value: "prod"},
		{Key: "team", Value: "storage"},
	}
	uploadID, err := tc.CreateMultipartUploadWithTags(ctx, "b", "k", "text/plain", tags)
	require.NoError(t, err)
	require.NotEmpty(t, uploadID)

	part, err := wrapped.UploadPart(ctx, "b", "k", uploadID, 1, strings.NewReader("hello"))
	require.NoError(t, err)

	_, err = wrapped.CompleteMultipartUpload(ctx, "b", "k", uploadID, []storage.Part{
		{PartNumber: 1, ETag: part.ETag, Size: part.Size},
	})
	require.NoError(t, err)

	got, err := inner.GetObjectTags("b", "k", "")
	require.NoError(t, err)
	require.Equal(t, tags, got, "tags from CreateMultipartUploadWithTags must materialise on the completed object")
}
