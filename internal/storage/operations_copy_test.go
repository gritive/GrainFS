package storage

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationsCopyObjectUsesOptimizedCopierWithoutACLOverride(t *testing.T) {
	backend := &copyBackend{}
	ops := NewOperations(backend)

	result, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		SourceBucket:      "src",
		SourceKey:         "k",
		DestinationBucket: "dst",
		DestinationKey:    "k2",
	})

	require.NoError(t, err)
	require.Equal(t, "copied", result.Object.ETag)
	require.Equal(t, []string{"copy:src/k:dst/k2"}, backend.calls)
}

func TestOperationsCopyObjectFallsBackStreamingAndPreservesContentType(t *testing.T) {
	backend := &copyFallbackBackend{}
	ops := NewOperations(backend)

	result, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		SourceBucket:      "src",
		SourceKey:         "k",
		DestinationBucket: "dst",
		DestinationKey:    "k2",
	})

	require.NoError(t, err)
	require.Equal(t, "fallback", result.Object.ETag)
	require.Equal(t, []string{"get:src/k", "put:dst/k2:text/plain:data"}, backend.calls)
}

func TestOperationsCopyObjectWithACLUsesACLWritePath(t *testing.T) {
	backend := &copyFallbackBackend{}
	ops := NewOperations(backend)
	acl := uint8(7)

	result, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		SourceBucket:      "src",
		SourceKey:         "k",
		DestinationBucket: "dst",
		DestinationKey:    "k2",
		ACL:               &acl,
	})

	require.NoError(t, err)
	require.Equal(t, "fallback-acl", result.Object.ETag)
	require.Equal(t, []string{"get:src/k", "putacl:dst/k2:text/plain:7:data"}, backend.calls)
}

func TestOperationsCopyObjectDoesNotBypassCachedBackendInvalidation(t *testing.T) {
	inner, err := NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { inner.Close() })
	require.NoError(t, inner.CreateBucket(context.Background(), "b"))
	_, err = inner.PutObject(context.Background(), "b", "src", strings.NewReader("new"), "text/plain")
	require.NoError(t, err)
	_, err = inner.PutObject(context.Background(), "b", "dst", strings.NewReader("old"), "text/plain")
	require.NoError(t, err)

	cached := NewCachedBackend(inner)
	ops := NewOperations(cached)

	rc, _, err := cached.GetObject(context.Background(), "b", "dst")
	require.NoError(t, err)
	oldData, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, "old", string(oldData))

	_, err = ops.CopyObject(context.Background(), CopyObjectRequest{
		SourceBucket:      "b",
		SourceKey:         "src",
		DestinationBucket: "b",
		DestinationKey:    "dst",
	})
	require.NoError(t, err)

	rc, _, err = cached.GetObject(context.Background(), "b", "dst")
	require.NoError(t, err)
	defer rc.Close()
	newData, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "new", string(newData))
}

type copyBackend struct {
	Backend
	calls []string
}

func (b *copyBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*Object, error) {
	b.calls = append(b.calls, "copy:"+srcBucket+"/"+srcKey+":"+dstBucket+"/"+dstKey)
	return &Object{Key: dstKey, ETag: "copied"}, nil
}

type copyFallbackBackend struct {
	Backend
	calls []string
}

func (b *copyFallbackBackend) GetObject(_ context.Context, bucket, key string) (io.ReadCloser, *Object, error) {
	b.calls = append(b.calls, "get:"+bucket+"/"+key)
	return io.NopCloser(strings.NewReader("data")), &Object{Key: key, ContentType: "text/plain"}, nil
}

func (b *copyFallbackBackend) PutObject(_ context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	b.calls = append(b.calls, "put:"+bucket+"/"+key+":"+contentType+":"+string(data))
	return &Object{Key: key, ETag: "fallback"}, nil
}

func (b *copyFallbackBackend) PutObjectWithACL(bucket, key string, r io.Reader, contentType string, acl uint8) (*Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	b.calls = append(b.calls, "putacl:"+bucket+"/"+key+":"+contentType+":"+string(rune('0'+acl))+":"+string(data))
	return &Object{Key: key, ETag: "fallback-acl", ACL: acl}, nil
}
