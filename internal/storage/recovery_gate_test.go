package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoveryWriteGateBlocksMutators(t *testing.T) {
	inner, err := NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, inner.CreateBucket(context.Background(), "b"))

	gate := NewRecoveryWriteGate(inner, nil)
	require.ErrorIs(t, gate.CreateBucket(context.Background(), "x"), ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.DeleteBucket(context.Background(), "b"), ErrRecoveryWriteDisabled)
	_, err = gate.PutObject(context.Background(), "b", "k", strings.NewReader("data"), "text/plain")
	require.ErrorIs(t, err, ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.DeleteObject(context.Background(), "b", "k"), ErrRecoveryWriteDisabled)
	_, err = gate.CreateMultipartUpload(context.Background(), "b", "k", "text/plain")
	require.ErrorIs(t, err, ErrRecoveryWriteDisabled)
	_, err = gate.UploadPart(context.Background(), "b", "k", "u", 1, strings.NewReader("data"))
	require.ErrorIs(t, err, ErrRecoveryWriteDisabled)
	_, err = gate.CompleteMultipartUpload(context.Background(), "b", "k", "u", nil)
	require.ErrorIs(t, err, ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.AbortMultipartUpload(context.Background(), "b", "k", "u"), ErrRecoveryWriteDisabled)
	_, err = gate.CopyObject("b", "k", "b", "copy")
	require.ErrorIs(t, err, ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.SetObjectACL("b", "k", 1), ErrRecoveryWriteDisabled)
	_, err = gate.PutObjectWithACL("b", "k", strings.NewReader("data"), "text/plain", 1)
	require.ErrorIs(t, err, ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.Truncate(context.Background(), "b", "k", 0), ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.SetBucketPolicy("b", []byte(`{}`)), ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.DeleteBucketPolicy("b"), ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.SetBucketVersioning("b", "Enabled"), ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.DeleteObjectVersion("b", "k", "v1"), ErrRecoveryWriteDisabled)
	_, err = gate.DeleteObjectReturningMarker("b", "k")
	require.ErrorIs(t, err, ErrRecoveryWriteDisabled)
	_, _, err = gate.RestoreObjects(nil)
	require.ErrorIs(t, err, ErrRecoveryWriteDisabled)
	require.ErrorIs(t, gate.RestoreBuckets(nil), ErrRecoveryWriteDisabled)
}

func TestRecoveryWriteGateAllowsReads(t *testing.T) {
	inner, err := NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, inner.CreateBucket(context.Background(), "b"))

	gate := NewRecoveryWriteGate(inner, errors.New("blocked"))
	require.NoError(t, gate.HeadBucket(context.Background(), "b"))
	buckets, err := gate.ListBuckets(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, buckets)
}

func TestRecoveryWriteGateDelegatesPolicyReads(t *testing.T) {
	inner, err := NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, inner.CreateBucket(context.Background(), "b"))
	require.NoError(t, inner.SetBucketPolicy("b", []byte(`{"Version":"2012-10-17"}`)))

	gate := NewRecoveryWriteGate(inner, nil)
	policy, err := gate.GetBucketPolicy("b")
	require.NoError(t, err)
	require.JSONEq(t, `{"Version":"2012-10-17"}`, string(policy))
}
