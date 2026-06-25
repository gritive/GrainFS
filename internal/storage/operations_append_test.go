package storage

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// appendCapableBackend embeds a Backend and adds AppendObject support.
type appendCapableBackend struct {
	Backend
	gotBucket string
	gotKey    string
	gotOffset int64
	ret       *Object
}

func (b *appendCapableBackend) AppendObject(_ context.Context, bucket, key string, off int64, _ io.Reader) (*Object, error) {
	b.gotBucket, b.gotKey, b.gotOffset = bucket, key, off
	return b.ret, nil
}

func TestOperationsAppendObject_DelegatesToCapableBackend(t *testing.T) {
	want := &Object{Key: "k", Size: 4, ETag: "etag"}
	be := &appendCapableBackend{Backend: newTestLocalBackend(t), ret: want}
	ops := NewOperations(be)

	got, err := ops.AppendObject(context.Background(), "b", "k", 7, strings.NewReader("data"))

	require.NoError(t, err)
	require.Same(t, want, got)
	require.Equal(t, "b", be.gotBucket)
	require.Equal(t, "k", be.gotKey)
	require.Equal(t, int64(7), be.gotOffset)
}

func TestOperationsAppendObject_UnsupportedBackend(t *testing.T) {
	// LocalBackend implements AppendObjecter; wrap to strip it so the plan sees
	// no append capability.
	ops := NewOperations(stripAppend{Backend: newTestLocalBackend(t)})

	_, err := ops.AppendObject(context.Background(), "b", "k", 0, strings.NewReader("x"))

	var unsupported UnsupportedOperationError
	require.ErrorAs(t, err, &unsupported)
	require.Equal(t, "AppendObject", unsupported.Op)
}

// stripAppend wraps a Backend WITHOUT promoting AppendObjecter, so the plan sees
// no append capability even though the inner backend has one.
type stripAppend struct{ Backend }
