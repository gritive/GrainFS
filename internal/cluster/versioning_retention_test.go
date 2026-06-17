package cluster

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// TestVersionedBucket_OldVersionReadable proves versioning retains old versions:
// on a versioning-enabled bucket, PUT v1 then PUT v2; GET ?versionId=v1 must
// still return v1's bytes. RED before the fix: simple PUTs commit only to
// quorum-meta (latest-only), so v1's per-version FSM metadata is never written
// and GetObjectVersion(v1) returns 404 (ErrObjectNotFound).
func TestVersionedBucket_OldVersionReadable(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	put := func(body string) string {
		sz := int64(len(body))
		obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
			Bucket: bkt, Key: key, Body: bytes.NewReader([]byte(body)),
			ContentType: "text/plain", SizeHint: &sz,
		})
		require.NoError(t, err)
		require.NotEmpty(t, obj.VersionID, "versioned PUT must return a versionID")
		return obj.VersionID
	}

	vid1 := put("content-v1")
	vid2 := put("content-v2")
	require.NotEqual(t, vid1, vid2)

	// Latest read returns v2.
	rcL, _, err := b.GetObject(ctx, bkt, key)
	require.NoError(t, err)
	gotL, _ := io.ReadAll(rcL)
	rcL.Close()
	require.Equal(t, []byte("content-v2"), gotL)

	// Old-version read returns v1 (RED today: 404).
	rc1, _, err := b.GetObjectVersion(context.Background(), bkt, key, vid1)
	require.NoError(t, err, "GET of an older version must not 404 — versioning must retain it")
	got1, _ := io.ReadAll(rc1)
	rc1.Close()
	require.Equal(t, []byte("content-v1"), got1)

	// Newer version still readable by id too.
	rc2, _, err := b.GetObjectVersion(context.Background(), bkt, key, vid2)
	require.NoError(t, err)
	got2, _ := io.ReadAll(rc2)
	rc2.Close()
	require.Equal(t, []byte("content-v2"), got2)

	// ListObjectVersions must enumerate BOTH versions (old + latest), exactly
	// one marked IsLatest (vid2). Old versions live as per-version FSM keys.
	versions, err := b.ListObjectVersions(bkt, "", 0)
	require.NoError(t, err)
	seen := map[string]bool{}
	latest := 0
	for _, v := range versions {
		seen[v.VersionID] = true
		if v.IsLatest {
			latest++
			require.Equal(t, vid2, v.VersionID, "IsLatest must be vid2")
		}
	}
	require.True(t, seen[vid1], "vid1 missing from ListObjectVersions")
	require.True(t, seen[vid2], "vid2 missing from ListObjectVersions")
	require.Equal(t, 1, latest, "exactly one IsLatest expected")
}
