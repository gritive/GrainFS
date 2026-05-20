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

// TestPackedBackend_PutObjectThenSetTags_R1Regression guards against R1
// (Phase 2 unblock fixes): in single-node mode the default --pack-threshold
// (65537) packs small objects into PackedBackend's blob+index, completely
// bypassing the inner ClusterCoordinator. The capability walker in
// Operations.buildOperationsPlan unwraps past PackedBackend (no method
// promotion through a non-embedded inner field) and binds tagsSetter to
// ClusterCoordinator, which has no record of the packed object — returning
// ErrObjectNotFound. The fix declares SetObjectTags/GetObjectTags directly
// on PackedBackend, dispatching to its own index for packed objects and
// delegating to inner for above-threshold objects.
func TestPackedBackend_PutObjectThenSetTags_R1Regression(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	inner, err := storage.NewLocalBackend(dir + "/local")
	require.NoError(t, err)
	pb, err := NewPackedBackend(inner, dir+"/blobs", 64*1024)
	require.NoError(t, err)

	require.NoError(t, pb.CreateBucket(ctx, "b"))

	// Packed branch — body well below the 64KiB threshold.
	smallTags := []storage.Tag{{Key: "expire", Value: "yes"}}
	_, err = pb.PutObject(ctx, "b", "small", strings.NewReader("hi"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, pb.SetObjectTags("b", "small", "", smallTags),
		"SetObjectTags must find packed objects in PackedBackend's index (R1 regression guard)")
	gotSmall, err := pb.GetObjectTags("b", "small", "")
	require.NoError(t, err)
	require.Equal(t, smallTags, gotSmall)

	// Above-threshold branch — body larger than 64KiB lands on inner.
	bigTags := []storage.Tag{{Key: "tier", Value: "cold"}}
	big := strings.Repeat("x", 65*1024)
	_, err = pb.PutObject(ctx, "b", "big", strings.NewReader(big), "text/plain")
	require.NoError(t, err)
	require.NoError(t, pb.SetObjectTags("b", "big", "", bigTags))
	gotBig, err := pb.GetObjectTags("b", "big", "")
	require.NoError(t, err)
	require.Equal(t, bigTags, gotBig)

	// SaveIndex / LoadIndex round-trip — packed-object tags must survive
	// restart. Save, close, reopen, confirm the tag set replays from index.bin.
	require.NoError(t, pb.SaveIndex())
	require.NoError(t, pb.Close())
	pb2, err := NewPackedBackend(inner, dir+"/blobs", 64*1024)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pb2.Close() })
	require.NoError(t, pb2.LoadIndex())
	replay, err := pb2.GetObjectTags("b", "small", "")
	require.NoError(t, err)
	require.Equal(t, smallTags, replay, "packed-object tags must persist via index.bin")
}
