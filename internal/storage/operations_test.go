package storage

import (
	"context"
	"errors"
	"io"
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
