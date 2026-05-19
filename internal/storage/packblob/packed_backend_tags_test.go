package packblob

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestPackedBackend_CreateMultipartUploadWithTags_DelegatesToInner is the
// regression guard for the single-node packed hot path. PackedBackend wraps
// inner via a NON-embedded field, so unlike interface-embedded wrappers it
// promotes NO methods automatically — `CreateMultipartUploadWithTags` must be
// declared explicitly or the wal.Backend sitting above it would fail the
// (tagsCreator) type assertion when PackThreshold > 0 is enabled.
func TestPackedBackend_CreateMultipartUploadWithTags_DelegatesToInner(t *testing.T) {
	dir := t.TempDir()
	inner, err := storage.NewLocalBackend(dir + "/local")
	require.NoError(t, err)
	pb, err := NewPackedBackend(inner, dir+"/blobs", 64*1024)
	require.NoError(t, err)
	t.Cleanup(func() { pb.Close() })

	tc, ok := any(pb).(interface {
		CreateMultipartUploadWithTags(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (string, error)
	})
	require.True(t, ok, "PackedBackend must expose CreateMultipartUploadWithTags so wal.Backend's (tagsCreator) type assertion reaches inner")

	ctx := context.Background()
	require.NoError(t, pb.CreateBucket(ctx, "b"))

	tags := []storage.Tag{
		{Key: "env", Value: "prod"},
		{Key: "team", Value: "storage"},
	}
	uploadID, err := tc.CreateMultipartUploadWithTags(ctx, "b", "k", "text/plain", tags)
	require.NoError(t, err)
	require.NotEmpty(t, uploadID)

	part, err := pb.UploadPart(ctx, "b", "k", uploadID, 1, strings.NewReader("hello"))
	require.NoError(t, err)

	_, err = pb.CompleteMultipartUpload(ctx, "b", "k", uploadID, []storage.Part{
		{PartNumber: 1, ETag: part.ETag, Size: part.Size},
	})
	require.NoError(t, err)

	// PackedBackend doesn't expose GetObjectTags directly; verify via inner.
	got, err := inner.GetObjectTags("b", "k", "")
	require.NoError(t, err)
	require.Equal(t, tags, got, "tags from CreateMultipartUploadWithTags must materialise via the packed wrapper")
}
