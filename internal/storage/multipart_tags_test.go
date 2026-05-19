package storage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestMultipart_TagsFromCreate_MaterialiseOnComplete(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))

	tags := []storage.Tag{{Key: "env", Value: "prod"}}
	id, err := b.CreateMultipartUploadWithTags(ctx(), "b", "k", "text/plain", tags)
	require.NoError(t, err)

	part, err := b.UploadPart(ctx(), "b", "k", id, 1, body("hello"))
	require.NoError(t, err)

	parts := []storage.Part{{PartNumber: 1, ETag: part.ETag, Size: part.Size}}
	_, err = b.CompleteMultipartUpload(ctx(), "b", "k", id, parts)
	require.NoError(t, err)

	got, err := b.GetObjectTags("b", "k", "")
	require.NoError(t, err)
	require.Equal(t, tags, got)
}
