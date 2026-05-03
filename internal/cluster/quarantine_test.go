package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestQuarantine_BlocksAffectedObjectOnly(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "b"))
	_, err := b.PutObject(context.Background(), "b", "bad", bytes.NewReader([]byte("bad")), "application/octet-stream")
	require.NoError(t, err)
	_, err = b.PutObject(context.Background(), "b", "good", bytes.NewReader([]byte("good")), "application/octet-stream")
	require.NoError(t, err)

	require.NoError(t, b.QuarantineObject(context.Background(), "b", "bad", "corrupt_blob", "CRC mismatch"))

	_, _, err = b.GetObject(context.Background(), "b", "bad")
	require.ErrorIs(t, err, ErrObjectQuarantined)

	rc, _, err := b.GetObject(context.Background(), "b", "good")
	require.NoError(t, err)
	defer rc.Close()
}

func TestQuarantine_BlocksWritesToAffectedObject(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "b"))
	require.NoError(t, b.QuarantineObject(context.Background(), "b", "bad", "corrupt_blob", "CRC mismatch"))

	_, err := b.PutObject(context.Background(), "b", "bad", bytes.NewReader([]byte("new")), "application/octet-stream")
	require.ErrorIs(t, err, ErrObjectQuarantined)

	_, err = b.PutObject(context.Background(), "b", "good", bytes.NewReader([]byte("good")), "application/octet-stream")
	assert.NoError(t, err)
}

func TestQuarantine_UnknownObjectStillReturnsNotFound(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "b"))
	_, _, err := b.GetObject(context.Background(), "b", "missing")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}
