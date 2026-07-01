package storage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

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
