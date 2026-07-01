package storage_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestRecoveryWriteGateBlocksMutators(t *testing.T) {
	inner := cluster.NewSingletonBackendForTest(t)
	require.NoError(t, inner.CreateBucket(context.Background(), "b"))

	gate := storage.NewRecoveryWriteGate(inner, nil)
	require.ErrorIs(t, gate.CreateBucket(context.Background(), "x"), storage.ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.DeleteBucket(context.Background(), "b"), storage.ErrRecoveryWriteDisabled)
	_, err := gate.PutObject(context.Background(), "b", "k", strings.NewReader("data"), "text/plain")
	require.ErrorIs(t, err, storage.ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.DeleteObject(context.Background(), "b", "k"), storage.ErrRecoveryWriteDisabled)
	_, err = gate.CreateMultipartUpload(context.Background(), "b", "k", "text/plain")
	require.ErrorIs(t, err, storage.ErrRecoveryWriteDisabled)
	_, err = gate.UploadPart(context.Background(), "b", "k", "u", 1, strings.NewReader("data"), "")
	require.ErrorIs(t, err, storage.ErrRecoveryWriteDisabled)
	_, err = gate.CompleteMultipartUpload(context.Background(), "b", "k", "u", nil)
	require.ErrorIs(t, err, storage.ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.AbortMultipartUpload(context.Background(), "b", "k", "u"), storage.ErrRecoveryWriteDisabled)
	_, err = gate.CopyObject("b", "k", "b", "copy")
	require.ErrorIs(t, err, storage.ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.SetObjectACL("b", "k", 1), storage.ErrRecoveryWriteDisabled)
	_, err = gate.PutObjectWithACL("b", "k", strings.NewReader("data"), "text/plain", 1)
	require.ErrorIs(t, err, storage.ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.Truncate(context.Background(), "b", "k", 0), storage.ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.SetBucketPolicy("b", []byte(`{}`)), storage.ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.DeleteBucketPolicy("b"), storage.ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.SetBucketVersioning("b", "Enabled"), storage.ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.DeleteObjectVersion("b", "k", "v1"), storage.ErrRecoveryWriteDisabled)
	_, err = gate.DeleteObjectReturningMarker("b", "k")
	require.ErrorIs(t, err, storage.ErrRecoveryWriteDisabled)
}

func TestRecoveryWriteGateAllowsReads(t *testing.T) {
	inner := cluster.NewSingletonBackendForTest(t)
	require.NoError(t, inner.CreateBucket(context.Background(), "b"))

	gate := storage.NewRecoveryWriteGate(inner, errors.New("blocked"))
	require.NoError(t, gate.HeadBucket(context.Background(), "b"))
	buckets, err := gate.ListBuckets(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, buckets)
}

func TestRecoveryWriteGateDelegatesPolicyReads(t *testing.T) {
	inner := cluster.NewSingletonBackendForTest(t)
	require.NoError(t, inner.CreateBucket(context.Background(), "b"))
	require.NoError(t, inner.SetBucketPolicy("b", []byte(`{"Version":"2012-10-17"}`)))

	gate := storage.NewRecoveryWriteGate(inner, nil)
	policy, err := gate.GetBucketPolicy("b")
	require.NoError(t, err)
	require.JSONEq(t, `{"Version":"2012-10-17"}`, string(policy))
}
