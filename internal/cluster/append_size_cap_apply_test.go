package cluster

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestApplyAppendObjectSizeCapRejects pins the FSM-side authoritative cap
// check. SizeCapBytes is set deliberately tiny so the second append blows
// the cap regardless of coordinator pre-check.
func TestApplyAppendObjectSizeCapRejects(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	b.SetCoalesceConfig(CoalesceConfig{
		SegmentCount: 1 << 30, SizeBytes: 1 << 60, IdleTimeout: 1 << 60,
		SizeCapBytes: 8,
	})

	bucket, key := "b", "k"
	_, err := b.AppendObject(ctx, bucket, key, 0, bytes.NewReader([]byte("hello")))
	require.NoError(t, err)
	_, err = b.AppendObject(ctx, bucket, key, 5, bytes.NewReader([]byte("more")))
	require.True(t, errors.Is(err, storage.ErrAppendObjectTooLarge),
		"second append over cap must surface ErrAppendObjectTooLarge, got %v", err)
}

// TestApplyAppendObjectAtCapBoundary verifies an append filling cap exactly
// succeeds, and the next +1 byte rejects. Tolerance contract check.
func TestApplyAppendObjectAtCapBoundary(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	b.SetCoalesceConfig(CoalesceConfig{
		SegmentCount: 1 << 30, SizeBytes: 1 << 60, IdleTimeout: 1 << 60,
		SizeCapBytes: 8,
	})
	bucket, key := "b", "exactly8"
	_, err := b.AppendObject(ctx, bucket, key, 0, bytes.NewReader([]byte("abcdefgh")))
	require.NoError(t, err, "append filling cap exactly must succeed")
	_, err = b.AppendObject(ctx, bucket, key, 8, bytes.NewReader([]byte("x")))
	require.True(t, errors.Is(err, storage.ErrAppendObjectTooLarge))
}
