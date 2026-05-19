package pullthrough_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/pullthrough"
)

// TestPullthroughBackend_CreateMultipartUploadWithTags_DelegatesToInner is the
// pullthrough sibling of TestWALBackend_CreateMultipartUploadWithTags_DelegatesToInner.
// The production S3 hot path wraps storage.Backend inside pullthrough.Backend;
// embedded-interface fields do not promote tagsCreator from the underlying
// concrete type, so without this pass-through Operations silently falls back
// to the no-tags overload and drops x-amz-tagging on multipart-initiate.
func TestPullthroughBackend_CreateMultipartUploadWithTags_DelegatesToInner(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)

	pt := pullthrough.NewBackend(local, &staticResolver{})

	tc, ok := any(pt).(interface {
		CreateMultipartUploadWithTags(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (string, error)
	})
	require.True(t, ok, "pullthrough.Backend must expose CreateMultipartUploadWithTags so Operations' type assertion reaches inner")

	ctx := context.Background()
	require.NoError(t, pt.CreateBucket(ctx, "b"))

	tags := []storage.Tag{
		{Key: "env", Value: "prod"},
		{Key: "team", Value: "storage"},
	}
	uploadID, err := tc.CreateMultipartUploadWithTags(ctx, "b", "k", "text/plain", tags)
	require.NoError(t, err)
	require.NotEmpty(t, uploadID)

	part, err := pt.UploadPart(ctx, "b", "k", uploadID, 1, strings.NewReader("hello"))
	require.NoError(t, err)

	_, err = pt.CompleteMultipartUpload(ctx, "b", "k", uploadID, []storage.Part{
		{PartNumber: 1, ETag: part.ETag, Size: part.Size},
	})
	require.NoError(t, err)

	got, err := local.GetObjectTags("b", "k", "")
	require.NoError(t, err)
	require.Equal(t, tags, got, "tags from CreateMultipartUploadWithTags must materialise on the completed object via the pullthrough wrapper")
}
