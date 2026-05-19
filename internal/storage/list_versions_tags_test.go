package storage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestHeadObject_AfterSetObjectTags(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))
	_, err := b.PutObject(ctx(), "b", "k", body("hi"), "text/plain")
	require.NoError(t, err)
	require.NoError(t, b.SetObjectTags("b", "k", "", []storage.Tag{{Key: "x", Value: "y"}}))

	obj, err := b.HeadObject(ctx(), "b", "k")
	require.NoError(t, err)
	require.Equal(t, []storage.Tag{{Key: "x", Value: "y"}}, obj.Tags)
}
