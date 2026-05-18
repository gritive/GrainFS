package cluster

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestAppendCoordinatorPreCheckRejectsOversize asserts the coordinator
// pre-check fast-rejects when existing.Size+chunk > SizeCapBytes. This is
// a best-effort fast path; the FSM check (Task 14) is authoritative.
func TestAppendCoordinatorPreCheckRejectsOversize(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	b.SetCoalesceConfig(CoalesceConfig{
		SegmentCount: 1 << 30, SizeBytes: 1 << 60, IdleTimeout: 1 << 60,
		SizeCapBytes: 8,
	})
	bucket, key := "b", "k"
	_, err := b.AppendObject(ctx, bucket, key, 0, bytes.NewReader([]byte("abcdefg"))) // 7 bytes
	require.NoError(t, err)
	// 7 + 5 = 12 > 8: must reject as ErrAppendObjectTooLarge.
	_, err = b.AppendObject(ctx, bucket, key, 7, bytes.NewReader([]byte("12345")))
	require.True(t, errors.Is(err, storage.ErrAppendObjectTooLarge),
		"over-cap append must surface ErrAppendObjectTooLarge, got %v", err)
}
