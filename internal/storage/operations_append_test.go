package storage_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// appendCapableBackend embeds a Backend and adds AppendObject support.
type appendCapableBackend struct {
	storage.Backend
	gotBucket string
	gotKey    string
	gotOffset int64
	ret       *storage.Object
}

func (b *appendCapableBackend) AppendObject(_ context.Context, bucket, key string, off int64, _ io.Reader) (*storage.Object, error) {
	b.gotBucket, b.gotKey, b.gotOffset = bucket, key, off
	return b.ret, nil
}

func TestOperationsAppendObject_DelegatesToCapableBackend(t *testing.T) {
	want := &storage.Object{Key: "k", Size: 4, ETag: "etag"}
	be := &appendCapableBackend{Backend: newBackend(t), ret: want}
	ops := storage.NewOperations(be)

	got, err := ops.AppendObject(context.Background(), "b", "k", 7, strings.NewReader("data"))

	require.NoError(t, err)
	require.Same(t, want, got)
	require.Equal(t, "b", be.gotBucket)
	require.Equal(t, "k", be.gotKey)
	require.Equal(t, int64(7), be.gotOffset)
}

func TestOperationsAppendObject_UnsupportedBackend(t *testing.T) {
	// stripAppend wraps a Backend without promoting AppendObjecter, so Operations
	// sees no append capability even though the inner backend may have one.
	ops := storage.NewOperations(stripAppend{Backend: newBackend(t)})

	_, err := ops.AppendObject(context.Background(), "b", "k", 0, strings.NewReader("x"))

	var unsupported storage.UnsupportedOperationError
	require.ErrorAs(t, err, &unsupported)
	require.Equal(t, "AppendObject", unsupported.Op)
}

// stripAppend wraps a Backend WITHOUT promoting AppendObjecter, so the plan sees
// no append capability even though the inner backend has one.
type stripAppend struct{ storage.Backend }
