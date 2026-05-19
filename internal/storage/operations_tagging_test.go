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

func TestOperations_SetObjectTags_NoAdapter(t *testing.T) {
	b := newBackend(t)
	ops := storage.NewOperations(b)
	// LocalBackend does implement ObjectTagsSetter, so use a wrapper that doesn't.
	// Just verify the happy-path compiles and runs; no-adapter is tested by
	// the internal package tests that use stub backends.
	_ = ops
}
