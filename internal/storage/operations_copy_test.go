package storage

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOperationsCopyObjectUsesOptimizedCopierWithoutACLOverride(t *testing.T) {
	backend := &copyBackend{}
	ops := NewOperations(backend)

	result, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		Source:      ObjectRef{Bucket: "src", Key: "k"},
		Destination: ObjectRef{Bucket: "dst", Key: "k2"},
	})

	require.NoError(t, err)
	require.Equal(t, "copied", result.Object.ETag)
	require.Equal(t, []string{"head:src/k", "head:dst/k2", "copy:src/k:dst/k2"}, backend.calls)
}

func TestOperationsCopyObjectFallsBackStreamingAndPreservesContentType(t *testing.T) {
	backend := &copyFallbackBackend{}
	ops := NewOperations(backend)

	result, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		Source:      ObjectRef{Bucket: "src", Key: "k"},
		Destination: ObjectRef{Bucket: "dst", Key: "k2"},
	})

	require.NoError(t, err)
	require.Equal(t, "fallback", result.Object.ETag)
	require.Equal(t, []string{"head:src/k", "head:dst/k2", "get:src/k", "put:dst/k2:text/plain:data"}, backend.calls)
}

func TestOperationsCopyObjectWithACLUsesACLWritePath(t *testing.T) {
	backend := &copyFallbackBackend{}
	ops := NewOperations(backend)
	acl := uint8(7)

	result, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		Source:      ObjectRef{Bucket: "src", Key: "k"},
		Destination: ObjectRef{Bucket: "dst", Key: "k2"},
		ACL:         &acl,
	})

	require.NoError(t, err)
	require.Equal(t, "fallback-acl", result.Object.ETag)
	require.Equal(t, []string{"head:src/k", "head:dst/k2", "get:src/k", "putacl:dst/k2:text/plain:7:data"}, backend.calls)
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
		Source:      ObjectRef{Bucket: "b", Key: "src"},
		Destination: ObjectRef{Bucket: "b", Key: "dst"},
	})
	require.NoError(t, err)

	rc, _, err = cached.GetObject(context.Background(), "b", "dst")
	require.NoError(t, err)
	defer rc.Close()
	newData, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "new", string(newData))
}

func TestOperationsCopyObjectHeadsSourceBeforeOpeningBodyAndAppliesReplaceContentType(t *testing.T) {
	backend := &semanticCopyBackend{
		head: &Object{Key: "k", ContentType: "text/plain", ETag: "src-etag", LastModified: 100},
		body: "data",
	}
	ops := NewOperations(backend)

	_, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		Source:            ObjectRef{Bucket: "src", Key: "k"},
		Destination:       ObjectRef{Bucket: "dst", Key: "k2"},
		MetadataDirective: CopyMetadataReplace,
		ContentType:       "application/json",
	})

	require.NoError(t, err)
	require.Equal(t, []string{"head:src/k", "head:dst/k2", "get:src/k", "put:dst/k2:application/json:data"}, backend.calls)
}

func TestOperationsCopyObjectEvaluatesSourcePreconditionsBeforeBodyOpen(t *testing.T) {
	backend := &semanticCopyBackend{
		head: &Object{Key: "k", ETag: "abc123", LastModified: time.Date(2026, 5, 6, 12, 0, 1, 900, time.UTC).Unix()},
		body: "data",
	}
	ops := NewOperations(backend)

	_, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		Source:      ObjectRef{Bucket: "src", Key: "k"},
		Destination: ObjectRef{Bucket: "dst", Key: "k2"},
		Preconditions: CopyPreconditions{
			IfMatch:           `"abc123"`,
			IfUnmodifiedSince: ptrTime(time.Date(2026, 5, 6, 12, 0, 1, 0, time.UTC)),
			IfNoneMatch:       `"abc123"`,
		},
	})

	require.ErrorIs(t, err, ErrPreconditionFailed)
	var typed PreconditionFailedError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, "CopyObject", typed.Op)
	require.Equal(t, CopyConditionIfNoneMatch, typed.Condition)
	require.Equal(t, []string{"head:src/k"}, backend.calls)
}

func TestOperationsCopyObjectRejectsUnsupportedETagConditionSelector(t *testing.T) {
	backend := &semanticCopyBackend{
		head: &Object{Key: "k", ETag: "abc123", LastModified: 100},
		body: "data",
	}
	ops := NewOperations(backend)

	_, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		Source:        ObjectRef{Bucket: "src", Key: "k"},
		Destination:   ObjectRef{Bucket: "dst", Key: "k2"},
		Preconditions: CopyPreconditions{IfMatch: `W/"abc123"`},
	})

	var invalid InvalidCopySourceError
	require.ErrorAs(t, err, &invalid)
	require.Equal(t, CopySourceUnsupportedETagSelector, invalid.Reason)
	require.Equal(t, []string{"head:src/k"}, backend.calls)
}

func TestOperationsCopyObjectReplaceUserMetadata(t *testing.T) {
	backend, err := NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	_, err = backend.PutObjectWithUserMetadata(
		context.Background(),
		"b",
		"src",
		strings.NewReader("data"),
		"text/plain",
		map[string]string{"x-amz-meta-owner": "old"},
	)
	require.NoError(t, err)
	ops := NewOperations(backend)

	_, err = ops.CopyObject(context.Background(), CopyObjectRequest{
		Source:            ObjectRef{Bucket: "b", Key: "src"},
		Destination:       ObjectRef{Bucket: "b", Key: "dst"},
		MetadataDirective: CopyMetadataReplace,
		UserMetadata:      map[string]string{"x-amz-meta-owner": "me"},
	})

	require.NoError(t, err)
	obj, err := backend.HeadObject(context.Background(), "b", "dst")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"x-amz-meta-owner": "me"}, obj.UserMetadata)
}

func TestOperationsCopyObjectCopyPreservesUserMetadata(t *testing.T) {
	backend, err := NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	_, err = backend.PutObjectWithUserMetadata(
		context.Background(),
		"b",
		"src",
		strings.NewReader("data"),
		"text/plain",
		map[string]string{"x-amz-meta-mtime": "1710000000"},
	)
	require.NoError(t, err)
	ops := NewOperations(backend)

	_, err = ops.CopyObject(context.Background(), CopyObjectRequest{
		Source:      ObjectRef{Bucket: "b", Key: "src"},
		Destination: ObjectRef{Bucket: "b", Key: "dst"},
	})

	require.NoError(t, err)
	obj, err := backend.HeadObject(context.Background(), "b", "dst")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"x-amz-meta-mtime": "1710000000"}, obj.UserMetadata)
}

func TestOperationsCopyObjectRejectsSameDestinationNoopButAllowsExplicitVersionRestore(t *testing.T) {
	backend := &semanticCopyBackend{head: &Object{Key: "k", ETag: "etag", ContentType: "text/plain", LastModified: 100}, body: "data"}
	ops := NewOperations(backend)

	_, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		Source:      ObjectRef{Bucket: "b", Key: "k"},
		Destination: ObjectRef{Bucket: "b", Key: "k"},
	})
	var invalid InvalidCopySourceError
	require.ErrorAs(t, err, &invalid)
	require.Equal(t, CopySourceSameAsDestinationNoop, invalid.Reason)

	backend.calls = nil
	_, err = ops.CopyObject(context.Background(), CopyObjectRequest{
		Source:      ObjectRef{Bucket: "b", Key: "k", VersionID: "v1"},
		Destination: ObjectRef{Bucket: "b", Key: "k"},
	})

	require.NoError(t, err)
	require.Equal(t, []string{"headversion:b/k:v1", "head:b/k", "getversion:b/k:v1", "put:b/k:text/plain:data"}, backend.calls)
}

func TestOperationsCopyObjectRejectsExplicitDeleteMarkerSource(t *testing.T) {
	backend := &semanticCopyBackend{
		headVersion: &Object{Key: "k", VersionID: "dm1", IsDeleteMarker: true, LastModified: 100},
	}
	ops := NewOperations(backend)

	_, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		Source:      ObjectRef{Bucket: "b", Key: "k", VersionID: "dm1"},
		Destination: ObjectRef{Bucket: "b", Key: "copy"},
	})

	var invalid InvalidCopySourceError
	require.ErrorAs(t, err, &invalid)
	require.Equal(t, CopySourceIsDeleteMarker, invalid.Reason)
	require.Equal(t, []string{"headversion:b/k:dm1"}, backend.calls)
}

func TestOperationsCopyObjectDoesNotBypassRecoveryWriteGateForInnerAdapter(t *testing.T) {
	inner := &adapterCopyBackend{
		semanticCopyBackend: semanticCopyBackend{
			head: &Object{Key: "k", ETag: "etag", ContentType: "text/plain", LastModified: 100},
			body: "data",
		},
	}
	gateErr := errors.New("writes blocked")
	ops := NewOperations(NewRecoveryWriteGate(inner, gateErr))

	_, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		Source:      ObjectRef{Bucket: "src", Key: "k"},
		Destination: ObjectRef{Bucket: "dst", Key: "k2"},
	})

	require.ErrorIs(t, err, gateErr)
	require.Equal(t, []string{"head:src/k", "head:dst/k2"}, inner.calls)
}

func TestOperationsCopyObjectDoesNotBypassMutatingWrapperForInnerAdapter(t *testing.T) {
	inner := &adapterCopyBackend{
		semanticCopyBackend: semanticCopyBackend{
			head: &Object{Key: "k", ETag: "etag", ContentType: "text/plain", LastModified: 100},
			body: "data",
		},
	}
	wrapper := &copySideEffectWrapper{inner: inner}
	ops := NewOperations(wrapper)

	_, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		Source:      ObjectRef{Bucket: "src", Key: "k"},
		Destination: ObjectRef{Bucket: "dst", Key: "k2"},
	})

	require.NoError(t, err)
	require.Equal(t, []string{"head:src/k", "head:dst/k2", "get:src/k", "wrapper-put:dst/k2", "put:dst/k2:text/plain:data"}, inner.calls)
}

type copyBackend struct {
	Backend
	calls []string
}

func (b *copyBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*Object, error) {
	b.calls = append(b.calls, "copy:"+srcBucket+"/"+srcKey+":"+dstBucket+"/"+dstKey)
	return &Object{Key: dstKey, ETag: "copied"}, nil
}

func (b *copyBackend) HeadObject(_ context.Context, bucket, key string) (*Object, error) {
	b.calls = append(b.calls, "head:"+bucket+"/"+key)
	return &Object{Key: key, ContentType: "text/plain", ETag: "src-etag", LastModified: 100}, nil
}

type copyFallbackBackend struct {
	Backend
	calls []string
}

func (b *copyFallbackBackend) HeadObject(_ context.Context, bucket, key string) (*Object, error) {
	b.calls = append(b.calls, "head:"+bucket+"/"+key)
	return &Object{Key: key, ContentType: "text/plain", ETag: "src-etag", LastModified: 100}, nil
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

type semanticCopyBackend struct {
	Backend
	calls       []string
	head        *Object
	headVersion *Object
	body        string
}

func (b *semanticCopyBackend) HeadObject(_ context.Context, bucket, key string) (*Object, error) {
	b.calls = append(b.calls, "head:"+bucket+"/"+key)
	if b.head == nil {
		return nil, ErrObjectNotFound
	}
	return cloneObject(b.head), nil
}

func (b *semanticCopyBackend) GetObject(_ context.Context, bucket, key string) (io.ReadCloser, *Object, error) {
	b.calls = append(b.calls, "get:"+bucket+"/"+key)
	if b.head == nil {
		return nil, nil, ErrObjectNotFound
	}
	return io.NopCloser(strings.NewReader(b.body)), cloneObject(b.head), nil
}

func (b *semanticCopyBackend) HeadObjectVersion(bucket, key, versionID string) (*Object, error) {
	b.calls = append(b.calls, "headversion:"+bucket+"/"+key+":"+versionID)
	if b.headVersion != nil {
		return cloneObject(b.headVersion), nil
	}
	if b.head == nil {
		return nil, ErrObjectNotFound
	}
	obj := cloneObject(b.head)
	obj.VersionID = versionID
	return obj, nil
}

func (b *semanticCopyBackend) GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, *Object, error) {
	b.calls = append(b.calls, "getversion:"+bucket+"/"+key+":"+versionID)
	if b.headVersion != nil {
		return io.NopCloser(strings.NewReader(b.body)), cloneObject(b.headVersion), nil
	}
	if b.head == nil {
		return nil, nil, ErrObjectNotFound
	}
	obj := cloneObject(b.head)
	obj.VersionID = versionID
	return io.NopCloser(strings.NewReader(b.body)), obj, nil
}

func (b *semanticCopyBackend) PutObject(_ context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	b.calls = append(b.calls, "put:"+bucket+"/"+key+":"+contentType+":"+string(data))
	return &Object{Key: key, ContentType: contentType, ETag: "dst-etag", LastModified: 200}, nil
}

func cloneObject(obj *Object) *Object {
	if obj == nil {
		return nil
	}
	cp := *obj
	return &cp
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

type adapterCopyBackend struct {
	semanticCopyBackend
}

func (b *adapterCopyBackend) CopyObjectWithRequest(_ context.Context, _ CopyObjectAccelerationRequest) (*Object, error) {
	b.calls = append(b.calls, "adapter")
	return &Object{Key: "k2", ETag: "adapter"}, nil
}

type copySideEffectWrapper struct {
	Backend
	inner *adapterCopyBackend
}

func (w *copySideEffectWrapper) Unwrap() Backend {
	return w.inner
}

func (w *copySideEffectWrapper) HeadObject(ctx context.Context, bucket, key string) (*Object, error) {
	return w.inner.HeadObject(ctx, bucket, key)
}

func (w *copySideEffectWrapper) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *Object, error) {
	return w.inner.GetObject(ctx, bucket, key)
}

func (w *copySideEffectWrapper) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	w.inner.calls = append(w.inner.calls, "wrapper-put:"+bucket+"/"+key)
	return w.inner.PutObject(ctx, bucket, key, r, contentType)
}
