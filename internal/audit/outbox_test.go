package audit_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
)

func TestOutboxAppendFinalizeListAck(t *testing.T) {
	ctx := context.Background()
	box, err := audit.OpenOutbox(filepath.Join(t.TempDir(), "audit-outbox"))
	require.NoError(t, err)
	defer box.Close()

	ev := audit.S3Event{
		EventID:   "evt-1",
		RequestID: "req-1",
		Ts:        time.Now().UnixMicro(),
		Method:    "PUT",
		Operation: "PutObject",
		Bucket:    "b",
		Key:       "k",
		Status:    0,
	}
	require.NoError(t, box.AppendAttempt(ctx, ev))

	pending, err := box.Pending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "evt-1", pending[0].EventID)
	require.Equal(t, int32(0), pending[0].Status)

	ev.Status = 200
	ev.AuthStatus = "allow"
	require.NoError(t, box.Finalize(ctx, ev))

	pending, err = box.Pending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, int32(200), pending[0].Status)
	require.Equal(t, "allow", pending[0].AuthStatus)

	require.NoError(t, box.Ack(ctx, []string{"evt-1"}))
	pending, err = box.Pending(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestOutboxSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "audit-outbox")
	box, err := audit.OpenOutbox(dir)
	require.NoError(t, err)
	require.NoError(t, box.AppendAttempt(ctx, audit.S3Event{EventID: "evt-1", RequestID: "req-1", Ts: 1}))
	require.NoError(t, box.Close())

	box, err = audit.OpenOutbox(dir)
	require.NoError(t, err)
	defer box.Close()
	pending, err := box.Pending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "evt-1", pending[0].EventID)
}

func TestOutboxAppendRequiresEventID(t *testing.T) {
	box, err := audit.OpenOutbox(filepath.Join(t.TempDir(), "audit-outbox"))
	require.NoError(t, err)
	defer box.Close()

	err = box.AppendAttempt(context.Background(), audit.S3Event{RequestID: "req-1"})
	require.True(t, errors.Is(err, audit.ErrOutboxInvalidEvent))
}
