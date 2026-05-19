package storage_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestCopyObject_Copy_PreservesTags(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "src"))
	require.NoError(t, b.CreateBucket(ctx(), "dst"))
	_, err := b.PutObject(ctx(), "src", "k", body("hi"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, b.SetObjectTags("src", "k", "", []storage.Tag{{Key: "env", Value: "prod"}}))

	ops := storage.NewOperations(b)
	_, err = ops.CopyObject(context.Background(), storage.CopyObjectRequest{
		Source:           storage.ObjectRef{Bucket: "src", Key: "k"},
		Destination:      storage.ObjectRef{Bucket: "dst", Key: "k"},
		TaggingDirective: storage.TaggingDirectiveCopy,
	})
	require.NoError(t, err)

	got, err := ops.GetObjectTags("dst", "k", "")
	require.NoError(t, err)
	require.Equal(t, []storage.Tag{{Key: "env", Value: "prod"}}, got)
}

func TestCopyObject_Replace_NoHeader_ClearsTags(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "src"))
	require.NoError(t, b.CreateBucket(ctx(), "dst"))
	_, err := b.PutObject(ctx(), "src", "k", body("hi"), "text/plain")
	require.NoError(t, err)
	_, err = b.PutObject(ctx(), "dst", "k", body("dst body"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, b.SetObjectTags("dst", "k", "", []storage.Tag{{Key: "existing", Value: "tag"}}))

	ops := storage.NewOperations(b)
	_, err = ops.CopyObject(context.Background(), storage.CopyObjectRequest{
		Source:           storage.ObjectRef{Bucket: "src", Key: "k"},
		Destination:      storage.ObjectRef{Bucket: "dst", Key: "k"},
		TaggingDirective: storage.TaggingDirectiveReplace,
		// Tags: nil -- no x-amz-tagging header; REPLACE must clear destination tags
	})
	require.NoError(t, err)

	got, err := ops.GetObjectTags("dst", "k", "")
	require.NoError(t, err)
	require.Empty(t, got) // REPLACE with no Tags must clear destination's existing tags
}

func TestCopyObject_Replace_UsesRequestTags(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "src"))
	require.NoError(t, b.CreateBucket(ctx(), "dst"))
	_, err := b.PutObject(ctx(), "src", "k", body("hi"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, b.SetObjectTags("src", "k", "", []storage.Tag{{Key: "old", Value: "1"}}))

	ops := storage.NewOperations(b)
	_, err = ops.CopyObject(context.Background(), storage.CopyObjectRequest{
		Source:           storage.ObjectRef{Bucket: "src", Key: "k"},
		Destination:      storage.ObjectRef{Bucket: "dst", Key: "k"},
		TaggingDirective: storage.TaggingDirectiveReplace,
		Tags:             []storage.Tag{{Key: "new", Value: "2"}},
	})
	require.NoError(t, err)

	got, err := ops.GetObjectTags("dst", "k", "")
	require.NoError(t, err)
	require.Equal(t, []storage.Tag{{Key: "new", Value: "2"}}, got)
}
