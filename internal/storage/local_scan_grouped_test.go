package storage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestLocalBackend_ScanObjectsGrouped_OneVersionPerKey(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))

	for _, key := range []string{"a", "c", "b"} {
		_, err := b.PutObject(ctx(), "b", key, body("payload-"+key), "text/plain")
		require.NoError(t, err)
	}

	ch, err := b.ScanObjectsGrouped("b")
	require.NoError(t, err)

	var groups []storage.ObjectKeyGroup
	for g := range ch {
		groups = append(groups, storage.ObjectKeyGroup{
			Bucket: g.Bucket, Key: g.Key,
			Versions: append([]storage.ObjectVersionRecord(nil), g.Versions...),
		})
	}

	require.Len(t, groups, 3)
	require.Equal(t, "a", groups[0].Key)
	require.Equal(t, "b", groups[1].Key)
	require.Equal(t, "c", groups[2].Key)

	for _, g := range groups {
		require.Len(t, g.Versions, 1, "LocalBackend is unversioned")
		v := g.Versions[0]
		require.True(t, v.IsLatest)
		require.Empty(t, v.VersionID)
		require.False(t, v.IsDeleteMarker)
		require.Positive(t, v.LastModified)
		require.Positive(t, v.Size)
	}
}

func TestLocalBackend_ScanObjectsGrouped_PopulatesTagsAndSize(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))
	_, err := b.PutObject(ctx(), "b", "k", body("hello"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, b.SetObjectTags("b", "k", "", []storage.Tag{{Key: "env", Value: "prod"}}))

	ch, err := b.ScanObjectsGrouped("b")
	require.NoError(t, err)
	var first storage.ObjectKeyGroup
	for g := range ch {
		first = storage.ObjectKeyGroup{
			Bucket: g.Bucket, Key: g.Key,
			Versions: append([]storage.ObjectVersionRecord(nil), g.Versions...),
		}
		break
	}
	require.Len(t, first.Versions, 1)
	require.Equal(t, int64(5), first.Versions[0].Size)
	require.Equal(t, []storage.Tag{{Key: "env", Value: "prod"}}, first.Versions[0].Tags)
}

func TestLocalBackend_ScanObjectsGrouped_EmptyBucket(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))
	ch, err := b.ScanObjectsGrouped("b")
	require.NoError(t, err)
	var got []storage.ObjectKeyGroup
	for g := range ch {
		got = append(got, g)
	}
	require.Empty(t, got)
}
