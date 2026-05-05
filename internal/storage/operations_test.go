package storage

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnsupportedOperationErrorMatchesSentinel(t *testing.T) {
	err := UnsupportedOperationError{Op: "DeleteObjectVersion", Reason: UnsupportedReasonNoAdapter}

	require.ErrorIs(t, err, ErrUnsupportedOperation)
	require.Equal(t, "DeleteObjectVersion", err.Op)
	require.Equal(t, UnsupportedReasonNoAdapter, err.Reason)
	require.Contains(t, err.Error(), "DeleteObjectVersion")
	require.Contains(t, err.Error(), string(UnsupportedReasonNoAdapter))
}

func TestUnsupportedOperationErrorsAsTyped(t *testing.T) {
	err := error(UnsupportedOperationError{Op: "CopyObject", Reason: UnsupportedReasonUnsafeFallback})
	var typed UnsupportedOperationError

	require.True(t, errors.As(err, &typed))
	require.Equal(t, "CopyObject", typed.Op)
	require.Equal(t, UnsupportedReasonUnsafeFallback, typed.Reason)
}

func TestOperationsRefreshesPlanAfterSwappableBackendSwap(t *testing.T) {
	swappable := NewSwappableBackend(&aclNoCapabilityBackend{})
	ops := NewOperations(swappable)

	err := ops.SetObjectACL("b", "k", 7)
	requireUnsupportedOp(t, err, "SetObjectACL", UnsupportedReasonNoAdapter)

	backend := &dynamicACLBackend{}
	swappable.Swap(backend)

	require.NoError(t, ops.SetObjectACL("b", "k", 7))
	require.Equal(t, []string{"setacl:b/k:7"}, backend.calls)
}

func TestOperationsRefreshesPlanAfterNestedSwappableBackendSwap(t *testing.T) {
	swappable := NewSwappableBackend(&basicBackend{})
	cached := NewCachedBackend(swappable)
	ops := NewOperations(cached)

	err := ops.SetObjectACL("b", "k", 7)
	requireUnsupportedOp(t, err, "SetObjectACL", UnsupportedReasonNoAdapter)

	backend := &dynamicACLBackend{}
	swappable.Swap(backend)

	require.NoError(t, ops.SetObjectACL("b", "k", 7))
	require.Equal(t, []string{"setacl:b/k:7"}, backend.calls)
}

func TestOperationsPutObjectDelegatesToBackend(t *testing.T) {
	backend := &recordingPutBackend{}
	ops := NewOperations(backend)

	obj, err := ops.PutObject(context.Background(), "b", "k", strings.NewReader("data"), "text/plain")

	require.NoError(t, err)
	require.Equal(t, "put", obj.ETag)
	require.Equal(t, "b/k:text/plain:data", backend.putCall)
}

func TestOperationsObjectReadsDelegateToBackend(t *testing.T) {
	backend := &recordingObjectReadBackend{}
	ops := NewOperations(backend)

	rc, obj, err := ops.GetObject(context.Background(), "b", "k")
	require.NoError(t, err)
	require.Equal(t, "get", obj.ETag)
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, "body", string(data))

	obj, err = ops.HeadObject(context.Background(), "b", "k")
	require.NoError(t, err)
	require.Equal(t, "head", obj.ETag)

	objects, err := ops.ListObjects(context.Background(), "b", "pre", 3)
	require.NoError(t, err)
	require.Len(t, objects, 1)
	require.Equal(t, "listed", objects[0].Key)
	require.Equal(t, []string{
		"get:b/k",
		"head:b/k",
		"list:b:pre:3",
	}, backend.calls)
}

func TestOperationsBucketsDelegateToBackend(t *testing.T) {
	backend := &recordingBucketBackend{}
	ops := NewOperations(backend)

	require.NoError(t, ops.CreateBucket(context.Background(), "b"))
	require.NoError(t, ops.HeadBucket(context.Background(), "b"))
	require.NoError(t, ops.DeleteBucket(context.Background(), "b"))

	buckets, err := ops.ListBuckets(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, buckets)
	require.Equal(t, []string{
		"create:b",
		"head:b",
		"delete:b",
		"list",
	}, backend.calls)
}

func TestOperationsMultipartDelegatesToBackend(t *testing.T) {
	backend := &recordingMultipartBackend{}
	ops := NewOperations(backend)

	upload, err := ops.CreateMultipartUpload(context.Background(), "b", "k", "text/plain")
	require.NoError(t, err)
	require.Equal(t, "u1", upload.UploadID)

	part, err := ops.UploadPart(context.Background(), "b", "k", "u1", 2, strings.NewReader("part"))
	require.NoError(t, err)
	require.Equal(t, 2, part.PartNumber)

	obj, err := ops.CompleteMultipartUpload(context.Background(), "b", "k", "u1", []Part{*part})
	require.NoError(t, err)
	require.Equal(t, "complete", obj.ETag)

	require.NoError(t, ops.AbortMultipartUpload(context.Background(), "b", "k", "u2"))
	require.Equal(t, []string{
		"create:b/k:text/plain",
		"upload:b/k:u1:2:part",
		"complete:b/k:u1:1",
		"abort:b/k:u2",
	}, backend.calls)
}

type recordingPutBackend struct {
	basicBackend
	putCall string
}

func (b *recordingPutBackend) PutObject(_ context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	b.putCall = bucket + "/" + key + ":" + contentType + ":" + string(data)
	return &Object{Key: key, ETag: "put", Size: int64(len(data)), ContentType: contentType}, nil
}

type recordingObjectReadBackend struct {
	basicBackend
	calls []string
}

func (b *recordingObjectReadBackend) GetObject(_ context.Context, bucket, key string) (io.ReadCloser, *Object, error) {
	b.calls = append(b.calls, "get:"+bucket+"/"+key)
	return io.NopCloser(strings.NewReader("body")), &Object{Key: key, ETag: "get"}, nil
}

func (b *recordingObjectReadBackend) HeadObject(_ context.Context, bucket, key string) (*Object, error) {
	b.calls = append(b.calls, "head:"+bucket+"/"+key)
	return &Object{Key: key, ETag: "head"}, nil
}

func (b *recordingObjectReadBackend) ListObjects(_ context.Context, bucket, prefix string, maxKeys int) ([]*Object, error) {
	b.calls = append(b.calls, "list:"+bucket+":"+prefix+":"+strconv.Itoa(maxKeys))
	return []*Object{{Key: "listed"}}, nil
}

type recordingBucketBackend struct {
	basicBackend
	calls []string
}

func (b *recordingBucketBackend) CreateBucket(_ context.Context, bucket string) error {
	b.calls = append(b.calls, "create:"+bucket)
	return nil
}

func (b *recordingBucketBackend) HeadBucket(_ context.Context, bucket string) error {
	b.calls = append(b.calls, "head:"+bucket)
	return nil
}

func (b *recordingBucketBackend) DeleteBucket(_ context.Context, bucket string) error {
	b.calls = append(b.calls, "delete:"+bucket)
	return nil
}

func (b *recordingBucketBackend) ListBuckets(_ context.Context) ([]string, error) {
	b.calls = append(b.calls, "list")
	return []string{"b"}, nil
}

type recordingMultipartBackend struct {
	basicBackend
	calls []string
}

func (b *recordingMultipartBackend) CreateMultipartUpload(_ context.Context, bucket, key, contentType string) (*MultipartUpload, error) {
	b.calls = append(b.calls, "create:"+bucket+"/"+key+":"+contentType)
	return &MultipartUpload{UploadID: "u1", Bucket: bucket, Key: key, ContentType: contentType}, nil
}

func (b *recordingMultipartBackend) UploadPart(
	_ context.Context,
	bucket, key, uploadID string,
	partNumber int,
	r io.Reader,
) (*Part, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	b.calls = append(b.calls, "upload:"+bucket+"/"+key+":"+uploadID+":"+strconv.Itoa(partNumber)+":"+string(data))
	return &Part{PartNumber: partNumber, ETag: "part"}, nil
}

func (b *recordingMultipartBackend) CompleteMultipartUpload(
	_ context.Context,
	bucket, key, uploadID string,
	parts []Part,
) (*Object, error) {
	b.calls = append(b.calls, "complete:"+bucket+"/"+key+":"+uploadID+":"+strconv.Itoa(len(parts)))
	return &Object{Key: key, ETag: "complete"}, nil
}

func (b *recordingMultipartBackend) AbortMultipartUpload(_ context.Context, bucket, key, uploadID string) error {
	b.calls = append(b.calls, "abort:"+bucket+"/"+key+":"+uploadID)
	return nil
}

type dynamicACLBackend struct {
	basicBackend
	calls []string
}

func (b *dynamicACLBackend) SetObjectACL(bucket, key string, acl uint8) error {
	b.calls = append(b.calls, "setacl:"+bucket+"/"+key+":"+string(rune('0'+acl)))
	return nil
}

type basicBackend struct{}

func (b *basicBackend) CreateBucket(context.Context, string) error { return nil }
func (b *basicBackend) HeadBucket(context.Context, string) error   { return nil }
func (b *basicBackend) DeleteBucket(context.Context, string) error { return nil }
func (b *basicBackend) ListBuckets(context.Context) ([]string, error) {
	return nil, nil
}
func (b *basicBackend) PutObject(context.Context, string, string, io.Reader, string) (*Object, error) {
	return &Object{}, nil
}
func (b *basicBackend) GetObject(context.Context, string, string) (io.ReadCloser, *Object, error) {
	return io.NopCloser(strings.NewReader("")), &Object{}, nil
}
func (b *basicBackend) HeadObject(context.Context, string, string) (*Object, error) {
	return &Object{}, nil
}
func (b *basicBackend) DeleteObject(context.Context, string, string) error { return nil }
func (b *basicBackend) ListObjects(context.Context, string, string, int) ([]*Object, error) {
	return nil, nil
}
func (b *basicBackend) WalkObjects(context.Context, string, string, func(*Object) error) error {
	return nil
}
func (b *basicBackend) CreateMultipartUpload(context.Context, string, string, string) (*MultipartUpload, error) {
	return &MultipartUpload{}, nil
}
func (b *basicBackend) UploadPart(context.Context, string, string, string, int, io.Reader) (*Part, error) {
	return &Part{}, nil
}
func (b *basicBackend) CompleteMultipartUpload(context.Context, string, string, string, []Part) (*Object, error) {
	return &Object{}, nil
}
func (b *basicBackend) AbortMultipartUpload(context.Context, string, string, string) error {
	return nil
}
