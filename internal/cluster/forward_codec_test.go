package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestForwardCodec_ListPartsArgs(t *testing.T) {
	args := buildListPartsArgs("bucket", "key", "upload-1", 500)

	decoded := raftpb.GetRootAsListPartsArgs(args, 0)
	require.Equal(t, "bucket", string(decoded.Bucket()))
	require.Equal(t, "key", string(decoded.Key()))
	require.Equal(t, "upload-1", string(decoded.UploadId()))
	require.Equal(t, int32(500), decoded.MaxParts())
}

func TestForwardCodec_ForwardReplyParts(t *testing.T) {
	reply := buildPartsReply([]storage.Part{
		{PartNumber: 1, ETag: "etag-1", Size: 10},
		{PartNumber: 2, ETag: "etag-2", Size: 20},
	})

	parts, err := partsFromReply(reply)
	require.NoError(t, err)
	require.Equal(t, []storage.Part{
		{PartNumber: 1, ETag: "etag-1", Size: 10},
		{PartNumber: 2, ETag: "etag-2", Size: 20},
	}, parts)
}

func TestForwardCodec_ListMultipartUploadsArgs(t *testing.T) {
	args := buildListMultipartUploadsArgs("bucket", "prefix/", 500)

	decoded := raftpb.GetRootAsListMultipartUploadsArgs(args, 0)
	require.Equal(t, "bucket", string(decoded.Bucket()))
	require.Equal(t, "prefix/", string(decoded.Prefix()))
	require.Equal(t, int32(500), decoded.MaxUploads())
}

func TestForwardCodec_ForwardReplyMultipartUploads(t *testing.T) {
	reply := buildMultipartUploadsReply([]*storage.MultipartUpload{
		{Bucket: "bucket", Key: "prefix/a.bin", UploadID: "upload-1", ContentType: "text/plain", CreatedAt: 11},
		{Bucket: "bucket", Key: "prefix/b.bin", UploadID: "upload-2", ContentType: "application/octet-stream", CreatedAt: 22},
	})

	uploads, err := multipartUploadsFromReply(reply)
	require.NoError(t, err)
	require.Equal(t, []*storage.MultipartUpload{
		{Bucket: "bucket", Key: "prefix/a.bin", UploadID: "upload-1", ContentType: "text/plain", CreatedAt: 11},
		{Bucket: "bucket", Key: "prefix/b.bin", UploadID: "upload-2", ContentType: "application/octet-stream", CreatedAt: 22},
	}, uploads)
}

func TestForwardStatusAppendObjectTooLargeRoundTrip(t *testing.T) {
	require.Equal(t, raftpb.ForwardStatusAppendObjectTooLarge,
		mapErrorToStatus(storage.ErrAppendObjectTooLarge))
}

// Regression: forwarded HEAD/CompleteMultipartUpload replies must carry
// storage.Object.Parts so the request-side ClusterCoordinator can drive
// `?partNumber=N` resolution. Before parts was wired into ForwardObjectMeta
// the cluster path returned PartsCount=1 and PartNumber>=2 → 416 even though
// the underlying object meta had two parts.
func TestForwardCodec_ObjectReply_PartsRoundTrip(t *testing.T) {
	parts := []storage.MultipartPartEntry{
		{PartNumber: 1, Size: 5 * 1024 * 1024, ETag: "etag-1"},
		{PartNumber: 2, Size: 5 * 1024 * 1024, ETag: "etag-2"},
	}
	src := &storage.Object{
		Key:          "warp-multipart.bin",
		Size:         10 * 1024 * 1024,
		ETag:         "complete-etag",
		ContentType:  "application/octet-stream",
		LastModified: 1715000000000,
		VersionID:    "v0",
		Parts:        parts,
	}

	got, err := objectFromReply(buildObjectReply(src, "bucket"))
	require.NoError(t, err)
	require.Equal(t, parts, got.Parts, "buildObjectReply must round-trip Parts")
	require.Equal(t, src.Size, got.Size)
	require.Equal(t, src.ETag, got.ETag)
}

func TestForwardCodec_GetObjectReply_PartsRoundTrip(t *testing.T) {
	parts := []storage.MultipartPartEntry{
		{PartNumber: 1, Size: 7, ETag: "p1"},
		{PartNumber: 2, Size: 13, ETag: "p2"},
		{PartNumber: 3, Size: 0, ETag: "p3-empty"},
	}
	body := []byte("hello world!\x00trailing-body-bytes")
	src := &storage.Object{
		Key:          "mp.bin",
		Size:         int64(len(body)),
		ETag:         "ce",
		ContentType:  "text/plain",
		LastModified: 1,
		VersionID:    "v1",
		Parts:        parts,
	}

	reply := buildGetObjectReply(src, "bucket", body)
	got, err := objectFromReply(reply)
	require.NoError(t, err)
	require.Equal(t, parts, got.Parts)

	gotBody := raftpb.GetRootAsForwardReply(reply, 0).ReadBodyBytes()
	require.Equal(t, body, gotBody, "GetObject body must remain alongside Parts")
}

// Back-compat: an Object with no Parts (single-PUT, append, legacy) must
// round-trip with Parts=nil, never an empty non-nil slice — partRange's
// "no parts" fast-path relies on len==0.
func TestForwardCodec_ObjectReply_NoParts(t *testing.T) {
	src := &storage.Object{Key: "single.bin", Size: 100, ETag: "e", LastModified: 1}
	got, err := objectFromReply(buildObjectReply(src, "bucket"))
	require.NoError(t, err)
	require.Nil(t, got.Parts)
}

// TestForwardObjectMeta_CarriesTags is the regression guard for adversarial
// review pass #3: ForwardObjectMeta / ForwardObjectVersionMeta / ForwardReply
// previously carried no tags field, so every cross-node forwarded read
// returned storage.Object.Tags=nil even when tags were persisted on the
// source node. Tests round-trip for buildObjectReply, buildGetObjectReply,
// buildObjectsReply, buildObjectVersionsReply, and buildGetObjectTagsReply.
func TestForwardObjectMeta_CarriesTags(t *testing.T) {
	tags := []storage.Tag{
		{Key: "env", Value: "prod"},
		{Key: "team", Value: "storage"},
	}

	t.Run("buildObjectReply", func(t *testing.T) {
		src := &storage.Object{Key: "k", Size: 5, ETag: "e", LastModified: 1, Tags: tags}
		got, err := objectFromReply(buildObjectReply(src, "bucket"))
		require.NoError(t, err)
		require.Equal(t, tags, got.Tags)
	})

	t.Run("buildGetObjectReply", func(t *testing.T) {
		src := &storage.Object{Key: "k", Size: 5, ETag: "e", LastModified: 1, Tags: tags}
		got, err := objectFromReply(buildGetObjectReply(src, "bucket", []byte("hello")))
		require.NoError(t, err)
		require.Equal(t, tags, got.Tags)
	})

	t.Run("buildObjectsReply", func(t *testing.T) {
		objs := []*storage.Object{
			{Key: "a", Size: 1, ETag: "ea", Tags: tags},
			{Key: "b", Size: 2, ETag: "eb"}, // no tags — must stay nil
		}
		got, err := objectsFromReply(buildObjectsReply("bucket", objs))
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Equal(t, tags, got[0].Tags, "element 0 must round-trip Tags")
		require.Nil(t, got[1].Tags, "element 1 must round-trip nil Tags as nil (not empty slice)")
	})

	t.Run("buildObjectVersionsReply", func(t *testing.T) {
		versions := []*storage.ObjectVersion{
			{Key: "a", VersionID: "v1", IsLatest: true, ETag: "ea", Size: 1, Tags: tags},
			{Key: "a", VersionID: "v0", ETag: "eb", Size: 2}, // no tags
		}
		got, err := objectVersionsFromReply(buildObjectVersionsReply(versions))
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Equal(t, tags, got[0].Tags)
		require.Nil(t, got[1].Tags)
	})

	t.Run("buildGetObjectTagsReply", func(t *testing.T) {
		got, err := tagsFromReply(buildGetObjectTagsReply(tags))
		require.NoError(t, err)
		require.Equal(t, tags, got)
	})

	t.Run("buildGetObjectTagsReply_empty", func(t *testing.T) {
		got, err := tagsFromReply(buildGetObjectTagsReply(nil))
		require.NoError(t, err)
		require.Nil(t, got, "empty tags must round-trip as nil")
	})

	t.Run("buildGetObjectTagsArgs_roundtrip", func(t *testing.T) {
		bytes := buildGetObjectTagsArgs("bk", "k", "vid-9")
		args := raftpb.GetRootAsGetObjectTagsArgs(bytes, 0)
		require.Equal(t, "bk", string(args.Bucket()))
		require.Equal(t, "k", string(args.Key()))
		require.Equal(t, "vid-9", string(args.VersionId()))
	})
}

func TestBuildHeadObjectVersionArgs_Roundtrip(t *testing.T) {
	bytes := buildHeadObjectVersionArgs("bk", "k", "vid-1")
	args := raftpb.GetRootAsHeadObjectVersionArgs(bytes, 0)
	require.Equal(t, "bk", string(args.Bucket()))
	require.Equal(t, "k", string(args.Key()))
	require.Equal(t, "vid-1", string(args.VersionId()))
}
