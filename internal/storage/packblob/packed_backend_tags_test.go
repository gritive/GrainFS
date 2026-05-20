package packblob

import (
	"context"
	"fmt"
	"strings"
	"sync"
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

// TestPackedBackend_PutObjectThenSetTags_R1Regression covers the
// SetObjectTags/GetObjectTags dispatch on PackedBackend: packed objects
// (below threshold) read/write via the local index, above-threshold objects
// delegate to inner, and packed-object tags survive a SaveIndex/LoadIndex
// round-trip.
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

// TestPackedBackend_SetObjectTags_RejectsVersionID guards parity with
// LocalBackend.SetObjectTags: versionID != "" is not supported.
func TestPackedBackend_SetObjectTags_RejectsVersionID(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	inner, err := storage.NewLocalBackend(dir + "/local")
	require.NoError(t, err)
	pb, err := NewPackedBackend(inner, dir+"/blobs", 64*1024)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pb.Close() })
	require.NoError(t, pb.CreateBucket(ctx, "b"))
	_, err = pb.PutObject(ctx, "b", "k", strings.NewReader("hi"), "text/plain")
	require.NoError(t, err)

	err = pb.SetObjectTags("b", "k", "some-version", []storage.Tag{{Key: "k", Value: "v"}})
	require.Error(t, err)
	var unsupported storage.UnsupportedOperationError
	require.ErrorAs(t, err, &unsupported)

	_, err = pb.GetObjectTags("b", "k", "some-version")
	require.Error(t, err)
	require.ErrorAs(t, err, &unsupported)
}

// TestPackedBackend_SetObjectTags_ConcurrentCAS verifies the CAS retry loop:
// N concurrent writers, each assigning a distinct tag set, must all land
// (no lost updates) — the final read returns one of the N expected sets.
// Run with -race to catch any read-modify-write that bypasses the CAS.
func TestPackedBackend_SetObjectTags_ConcurrentCAS(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	inner, err := storage.NewLocalBackend(dir + "/local")
	require.NoError(t, err)
	pb, err := NewPackedBackend(inner, dir+"/blobs", 64*1024)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pb.Close() })
	require.NoError(t, pb.CreateBucket(ctx, "b"))
	_, err = pb.PutObject(ctx, "b", "k", strings.NewReader("hi"), "text/plain")
	require.NoError(t, err)

	const writers = 32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			tags := []storage.Tag{{Key: "writer", Value: fmt.Sprintf("%d", i)}}
			require.NoError(t, pb.SetObjectTags("b", "k", "", tags))
		}()
	}
	close(start)
	wg.Wait()

	got, err := pb.GetObjectTags("b", "k", "")
	require.NoError(t, err)
	require.Len(t, got, 1, "exactly one writer's tag set must win")
	require.Equal(t, "writer", got[0].Key)
}
