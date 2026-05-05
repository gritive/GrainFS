package storage

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationsVersioningDelegatesToAdapters(t *testing.T) {
	backend := &versioningBackend{}
	ops := NewOperations(backend)

	require.NoError(t, ops.SetBucketVersioning("b", "Enabled"))
	state, err := ops.GetBucketVersioning("b")
	require.NoError(t, err)
	require.Equal(t, "Enabled", state)

	rc, obj, err := ops.GetObjectVersion("b", "k", "v1")
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, "v1", obj.VersionID)

	head, err := ops.HeadObjectVersion("b", "k", "v1")
	require.NoError(t, err)
	require.Equal(t, "v1", head.VersionID)

	versions, err := ops.ListObjectVersions("b", "k", 1000)
	require.NoError(t, err)
	require.Len(t, versions, 1)

	require.NoError(t, ops.DeleteObjectVersion("b", "k", "v1"))
	marker, err := ops.DeleteObjectReturningMarker(context.Background(), "b", "k")
	require.NoError(t, err)
	require.Equal(t, "dm1", marker)

	require.Equal(t, []string{
		"setversioning:b:Enabled",
		"getversioning:b",
		"getversion:b/k:v1",
		"headversion:b/k:v1",
		"listversions:b:k:1000",
		"deleteversion:b/k:v1",
		"deletemarker:b/k",
	}, backend.calls)
}

func TestOperationsDeleteObjectReturningMarkerFallsBackToDeleteObject(t *testing.T) {
	backend := &deleteOnlyBackend{}
	ops := NewOperations(backend)

	marker, err := ops.DeleteObjectReturningMarker(context.Background(), "b", "k")

	require.NoError(t, err)
	require.Empty(t, marker)
	require.Equal(t, []string{"delete:b/k"}, backend.calls)
}

func TestOperationsVersioningUnsupportedWithoutAdapters(t *testing.T) {
	ops := NewOperations(&aclNoCapabilityBackend{})

	_, err := ops.GetBucketVersioning("b")
	requireUnsupportedOp(t, err, "GetBucketVersioning", UnsupportedReasonNoAdapter)

	err = ops.SetBucketVersioning("b", "Enabled")
	requireUnsupportedOp(t, err, "SetBucketVersioning", UnsupportedReasonNoAdapter)

	_, _, err = ops.GetObjectVersion("b", "k", "v1")
	requireUnsupportedOp(t, err, "GetObjectVersion", UnsupportedReasonNoAdapter)

	_, err = ops.HeadObjectVersion("b", "k", "v1")
	requireUnsupportedOp(t, err, "HeadObjectVersion", UnsupportedReasonNoAdapter)

	_, err = ops.ListObjectVersions("b", "", 1000)
	requireUnsupportedOp(t, err, "ListObjectVersions", UnsupportedReasonNoAdapter)

	err = ops.DeleteObjectVersion("b", "k", "v1")
	requireUnsupportedOp(t, err, "DeleteObjectVersion", UnsupportedReasonNoAdapter)
}

func requireUnsupportedOp(t *testing.T, err error, op string, reason UnsupportedReason) {
	t.Helper()
	require.ErrorIs(t, err, ErrUnsupportedOperation)
	var typed UnsupportedOperationError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, op, typed.Op)
	require.Equal(t, reason, typed.Reason)
}

type versioningBackend struct {
	Backend
	calls []string
}

func (b *versioningBackend) SetBucketVersioning(bucket, state string) error {
	b.calls = append(b.calls, "setversioning:"+bucket+":"+state)
	return nil
}

func (b *versioningBackend) GetBucketVersioning(bucket string) (string, error) {
	b.calls = append(b.calls, "getversioning:"+bucket)
	return "Enabled", nil
}

func (b *versioningBackend) GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, *Object, error) {
	b.calls = append(b.calls, "getversion:"+bucket+"/"+key+":"+versionID)
	return io.NopCloser(strings.NewReader("")), &Object{Key: key, VersionID: versionID}, nil
}

func (b *versioningBackend) HeadObjectVersion(bucket, key, versionID string) (*Object, error) {
	b.calls = append(b.calls, "headversion:"+bucket+"/"+key+":"+versionID)
	return &Object{Key: key, VersionID: versionID}, nil
}

func (b *versioningBackend) ListObjectVersions(bucket, prefix string, maxKeys int) ([]*ObjectVersion, error) {
	b.calls = append(b.calls, "listversions:"+bucket+":"+prefix+":1000")
	return []*ObjectVersion{{Key: prefix, VersionID: "v1"}}, nil
}

func (b *versioningBackend) DeleteObjectVersion(bucket, key, versionID string) error {
	b.calls = append(b.calls, "deleteversion:"+bucket+"/"+key+":"+versionID)
	return nil
}

func (b *versioningBackend) DeleteObjectReturningMarker(bucket, key string) (string, error) {
	b.calls = append(b.calls, "deletemarker:"+bucket+"/"+key)
	return "dm1", nil
}

type deleteOnlyBackend struct {
	Backend
	calls []string
}

func (b *deleteOnlyBackend) DeleteObject(_ context.Context, bucket, key string) error {
	b.calls = append(b.calls, "delete:"+bucket+"/"+key)
	return nil
}
