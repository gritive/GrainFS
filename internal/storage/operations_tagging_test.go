package storage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestOperations_SetGetDeleteObjectTags(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))
	_, err := b.PutObject(ctx(), "b", "k", body("body"), "text/plain")
	require.NoError(t, err)

	ops := storage.NewOperations(b)
	tags := []storage.Tag{{Key: "team", Value: "platform"}}
	require.NoError(t, ops.SetObjectTags("b", "k", "", tags))

	got, err := ops.GetObjectTags("b", "k", "")
	require.NoError(t, err)
	require.Equal(t, tags, got)

	require.NoError(t, ops.DeleteObjectTags("b", "k", ""))
	got, err = ops.GetObjectTags("b", "k", "")
	require.NoError(t, err)
	require.Empty(t, got)
}

type tagsNoCapabilityBackend struct {
	storage.Backend
}

func TestOperations_SetObjectTags_NoAdapter(t *testing.T) {
	ops := storage.NewOperations(&tagsNoCapabilityBackend{})

	err := ops.SetObjectTags("b", "k", "", []storage.Tag{{Key: "x", Value: "y"}})
	require.ErrorIs(t, err, storage.ErrUnsupportedOperation)
	var typed storage.UnsupportedOperationError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, "SetObjectTags", typed.Op)
	require.Equal(t, storage.UnsupportedReasonNoAdapter, typed.Reason)

	var typed2 storage.UnsupportedOperationError
	_, err = ops.GetObjectTags("b", "k", "")
	require.ErrorIs(t, err, storage.ErrUnsupportedOperation)
	require.ErrorAs(t, err, &typed2)
	require.Equal(t, "GetObjectTags", typed2.Op)

	err = ops.DeleteObjectTags("b", "k", "")
	require.ErrorIs(t, err, storage.ErrUnsupportedOperation)
}
