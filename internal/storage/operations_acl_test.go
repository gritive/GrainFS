package storage

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationsPutObjectWithACLUsesAtomicAdapter(t *testing.T) {
	backend := &atomicACLBackend{}
	ops := NewOperations(backend)

	obj, err := ops.PutObjectWithACL(context.Background(), "b", "k", strings.NewReader("data"), "text/plain", 7)

	require.NoError(t, err)
	require.Equal(t, "atomic", obj.ETag)
	require.Equal(t, []string{"atomic:b/k:7"}, backend.calls)
}

func TestOperationsPutObjectWithACLFallsBackToPutThenSetACL(t *testing.T) {
	backend := &aclFallbackBackend{}
	ops := NewOperations(backend)

	obj, err := ops.PutObjectWithACL(context.Background(), "b", "k", strings.NewReader("data"), "text/plain", 7)

	require.NoError(t, err)
	require.Equal(t, "v1", obj.VersionID)
	require.Equal(t, []string{"put:b/k", "setacl:b/k:7"}, backend.calls)
}

func TestOperationsPutObjectWithACLRollsBackNewVersionWhenSetACLFails(t *testing.T) {
	setErr := errors.New("set acl failed")
	backend := &aclFallbackBackend{setACLErr: setErr}
	ops := NewOperations(backend)

	_, err := ops.PutObjectWithACL(context.Background(), "b", "k", strings.NewReader("data"), "text/plain", 7)

	require.ErrorIs(t, err, setErr)
	require.Equal(t, []string{"put:b/k", "setacl:b/k:7", "deleteversion:b/k:v1"}, backend.calls)
}

func TestOperationsPutObjectWithACLReportsRollbackFailure(t *testing.T) {
	backend := &aclFallbackBackend{
		setACLErr:         errors.New("set acl failed"),
		deleteVersionErr:  errors.New("delete version failed"),
		putObjectVersion:  "v1",
		putObjectResponse: &Object{Key: "k", VersionID: "v1"},
	}
	ops := NewOperations(backend)

	_, err := ops.PutObjectWithACL(context.Background(), "b", "k", strings.NewReader("data"), "text/plain", 7)

	require.ErrorIs(t, err, ErrUnsupportedOperation)
	var typed UnsupportedOperationError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, "PutObjectWithACL", typed.Op)
	require.Equal(t, UnsupportedReasonRollbackFailed, typed.Reason)
	require.Equal(t, []string{"put:b/k", "setacl:b/k:7", "deleteversion:b/k:v1"}, backend.calls)
}

func TestOperationsPutObjectWithACLReportsRollbackFailureWhenVersionUnavailable(t *testing.T) {
	backend := &aclFallbackBackend{
		setACLErr:         errors.New("set acl failed"),
		putObjectVersion:  "none",
		putObjectResponse: &Object{Key: "k"},
	}
	ops := NewOperations(backend)

	_, err := ops.PutObjectWithACL(context.Background(), "b", "k", strings.NewReader("data"), "text/plain", 7)

	requireUnsupportedOp(t, err, "PutObjectWithACL", UnsupportedReasonRollbackFailed)
	require.Equal(t, []string{"put:b/k", "setacl:b/k:7"}, backend.calls)
}

func TestOperationsSetObjectACLUnsupportedWhenNoAdapter(t *testing.T) {
	ops := NewOperations(&aclNoCapabilityBackend{})

	err := ops.SetObjectACL("b", "k", 7)

	require.ErrorIs(t, err, ErrUnsupportedOperation)
	var typed UnsupportedOperationError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, "SetObjectACL", typed.Op)
	require.Equal(t, UnsupportedReasonNoAdapter, typed.Reason)
}

type aclNoCapabilityBackend struct {
	Backend
}

type atomicACLBackend struct {
	Backend
	calls []string
}

func (b *atomicACLBackend) PutObjectWithACL(bucket, key string, _ io.Reader, _ string, acl uint8) (*Object, error) {
	b.calls = append(b.calls, "atomic:"+bucket+"/"+key+":"+string(rune('0'+acl)))
	return &Object{Key: key, ETag: "atomic", ACL: acl}, nil
}

type aclFallbackBackend struct {
	Backend
	calls             []string
	setACLErr         error
	deleteVersionErr  error
	putObjectVersion  string
	putObjectResponse *Object
}

func (b *aclFallbackBackend) PutObject(_ context.Context, bucket, key string, _ io.Reader, _ string) (*Object, error) {
	b.calls = append(b.calls, "put:"+bucket+"/"+key)
	if b.putObjectResponse != nil {
		return b.putObjectResponse, nil
	}
	version := b.putObjectVersion
	if version == "" {
		version = "v1"
	}
	return &Object{Key: key, VersionID: version}, nil
}

func (b *aclFallbackBackend) SetObjectACL(bucket, key string, acl uint8) error {
	b.calls = append(b.calls, "setacl:"+bucket+"/"+key+":"+string(rune('0'+acl)))
	return b.setACLErr
}

func (b *aclFallbackBackend) DeleteObjectVersion(bucket, key, versionID string) error {
	b.calls = append(b.calls, "deleteversion:"+bucket+"/"+key+":"+versionID)
	return b.deleteVersionErr
}
