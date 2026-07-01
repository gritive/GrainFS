package storage_test

import (
	"context"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestOperationsCopyObjectReplaceUserMetadata(t *testing.T) {
	backend := cluster.NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	_, err := backend.PutObjectWithUserMetadata(
		context.Background(),
		"b",
		"src",
		strings.NewReader("data"),
		"text/plain",
		map[string]string{"x-amz-meta-owner": "old"},
	)
	require.NoError(t, err)
	ops := storage.NewOperations(backend)

	_, err = ops.CopyObject(context.Background(), storage.CopyObjectRequest{
		Source:            storage.ObjectRef{Bucket: "b", Key: "src"},
		Destination:       storage.ObjectRef{Bucket: "b", Key: "dst"},
		MetadataDirective: storage.CopyMetadataReplace,
		UserMetadata:      map[string]string{"x-amz-meta-owner": "me"},
	})

	require.NoError(t, err)
	obj, err := backend.HeadObject(context.Background(), "b", "dst")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"x-amz-meta-owner": "me"}, obj.UserMetadata)
}

func TestOperationsCopyObjectCopyPreservesUserMetadata(t *testing.T) {
	backend := cluster.NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	_, err := backend.PutObjectWithUserMetadata(
		context.Background(),
		"b",
		"src",
		strings.NewReader("data"),
		"text/plain",
		map[string]string{"x-amz-meta-mtime": "1710000000"},
	)
	require.NoError(t, err)
	ops := storage.NewOperations(backend)

	_, err = ops.CopyObject(context.Background(), storage.CopyObjectRequest{
		Source:      storage.ObjectRef{Bucket: "b", Key: "src"},
		Destination: storage.ObjectRef{Bucket: "b", Key: "dst"},
	})

	require.NoError(t, err)
	obj, err := backend.HeadObject(context.Background(), "b", "dst")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"x-amz-meta-mtime": "1710000000"}, obj.UserMetadata)
}

func TestOperationsPutObjectWithRequestResultPersistsSSESystemMetadata(t *testing.T) {
	backend := cluster.NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	ops := storage.NewOperations(backend)

	_, err := ops.PutObjectWithRequestResult(context.Background(), storage.PutObjectRequest{
		Bucket:         "b",
		Key:            "sse",
		Body:           strings.NewReader("data"),
		ContentType:    "text/plain",
		UserMetadata:   map[string]string{"x-amz-meta-owner": "me"},
		SystemMetadata: storage.ObjectSystemMetadata{SSEAlgorithm: "AES256"},
	})
	require.NoError(t, err)

	obj, err := backend.HeadObject(context.Background(), "b", "sse")
	require.NoError(t, err)
	require.Equal(t, "AES256", obj.SSEAlgorithm)
	require.Equal(t, map[string]string{"x-amz-meta-owner": "me"}, obj.UserMetadata)
}

func TestOperationsCopyObjectCopyPreservesSSESystemMetadata(t *testing.T) {
	backend := cluster.NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	ops := storage.NewOperations(backend)

	_, err := ops.PutObjectWithRequestResult(context.Background(), storage.PutObjectRequest{
		Bucket:         "b",
		Key:            "src",
		Body:           strings.NewReader("data"),
		ContentType:    "text/plain",
		SystemMetadata: storage.ObjectSystemMetadata{SSEAlgorithm: "AES256"},
	})
	require.NoError(t, err)

	_, err = ops.CopyObject(context.Background(), storage.CopyObjectRequest{
		Source:      storage.ObjectRef{Bucket: "b", Key: "src"},
		Destination: storage.ObjectRef{Bucket: "b", Key: "dst"},
	})
	require.NoError(t, err)

	obj, err := backend.HeadObject(context.Background(), "b", "dst")
	require.NoError(t, err)
	require.Equal(t, "AES256", obj.SSEAlgorithm)
}

func TestOperationsCopyObjectReplaceUsesExplicitSSESystemMetadata(t *testing.T) {
	backend := cluster.NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	ops := storage.NewOperations(backend)

	_, err := ops.PutObjectWithRequestResult(context.Background(), storage.PutObjectRequest{
		Bucket:         "b",
		Key:            "src",
		Body:           strings.NewReader("data"),
		ContentType:    "text/plain",
		SystemMetadata: storage.ObjectSystemMetadata{SSEAlgorithm: "AES256"},
	})
	require.NoError(t, err)

	_, err = ops.CopyObject(context.Background(), storage.CopyObjectRequest{
		Source:            storage.ObjectRef{Bucket: "b", Key: "src"},
		Destination:       storage.ObjectRef{Bucket: "b", Key: "dst"},
		MetadataDirective: storage.CopyMetadataReplace,
		ContentType:       "application/octet-stream",
		SystemMetadata:    storage.ObjectSystemMetadata{SSEAlgorithm: ""},
	})
	require.NoError(t, err)

	obj, err := backend.HeadObject(context.Background(), "b", "dst")
	require.NoError(t, err)
	require.Empty(t, obj.SSEAlgorithm)
}
