package audit_test

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
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
	require.Empty(t, pending, "attempt rows must not be committed before final response status is recorded")

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
	require.NoError(t, box.AppendFinalized(ctx, audit.S3Event{EventID: "evt-1", RequestID: "req-1", Ts: 1}))
	require.NoError(t, box.Close())

	box, err = audit.OpenOutbox(dir)
	require.NoError(t, err)
	defer box.Close()
	pending, err := box.Pending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "evt-1", pending[0].EventID)
}

func TestOutboxPendingReturnsStaleAttemptsAsIncomplete(t *testing.T) {
	ctx := context.Background()
	box, err := audit.OpenOutbox(filepath.Join(t.TempDir(), "audit-outbox"))
	require.NoError(t, err)
	defer box.Close()

	require.NoError(t, box.AppendAttempt(ctx, audit.S3Event{
		EventID: "evt-stale",
		Ts:      time.Now().Add(-10 * time.Minute).UnixMicro(),
		Bucket:  "b",
		Key:     "k",
		Method:  "PUT",
	}))

	pending, err := box.Pending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "evt-stale", pending[0].EventID)
	require.Equal(t, "incomplete", pending[0].AuthStatus)
	require.Equal(t, "request_incomplete", pending[0].ErrReason)
}

func TestOutboxFinalizeAfterStaleAckRequeuesFinalOutcome(t *testing.T) {
	ctx := context.Background()
	box, err := audit.OpenOutbox(filepath.Join(t.TempDir(), "audit-outbox"))
	require.NoError(t, err)
	defer box.Close()

	ev := audit.S3Event{
		EventID: "evt-long",
		Ts:      time.Now().Add(-10 * time.Minute).UnixMicro(),
		Bucket:  "b",
		Key:     "large.bin",
		Method:  "PUT",
	}
	require.NoError(t, box.AppendAttempt(ctx, ev))

	pending, err := box.Pending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "request_incomplete", pending[0].ErrReason)
	require.NoError(t, box.AckEvents(ctx, pending))

	ev.Status = 200
	ev.AuthStatus = "allow"
	ev.BytesOut = 123
	ev.LatencyMs = 400000
	require.NoError(t, box.Finalize(ctx, ev))

	pending, err = box.Pending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, int32(200), pending[0].Status)
	require.Equal(t, "allow", pending[0].AuthStatus)
	require.Equal(t, int64(123), pending[0].BytesOut)
	require.Equal(t, int32(400000), pending[0].LatencyMs)
}

func TestOutboxAppendFinalizedIgnoresCommittedEventID(t *testing.T) {
	box, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer box.Close()

	ctx := context.Background()
	ev := audit.S3Event{EventID: "evt-retry", RequestID: "req-1", Ts: 1, Status: 200}
	require.NoError(t, box.AppendFinalized(ctx, ev))
	require.NoError(t, box.Ack(ctx, []string{ev.EventID}))
	require.NoError(t, box.AppendFinalized(ctx, ev))

	pending, err := box.Pending(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestOutboxAckEventsChunksLargeBatches(t *testing.T) {
	box, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer box.Close()

	ctx := context.Background()
	events := make([]audit.S3Event, 2500)
	for i := range events {
		events[i] = audit.S3Event{
			EventID:    "evt-large-" + strconv.Itoa(i),
			RequestID:  "req-large-" + strconv.Itoa(i),
			Ts:         int64(i + 1),
			Status:     200,
			Finalized:  true,
			AuthStatus: "allow",
		}
		require.NoError(t, box.AppendFinalized(ctx, events[i]))
	}

	require.NoError(t, box.AckEvents(ctx, events))
	pending, err := box.Pending(ctx, len(events))
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestOutboxAppendRequiresEventID(t *testing.T) {
	box, err := audit.OpenOutbox(filepath.Join(t.TempDir(), "audit-outbox"))
	require.NoError(t, err)
	defer box.Close()

	err = box.AppendAttempt(context.Background(), audit.S3Event{RequestID: "req-1"})
	require.True(t, errors.Is(err, audit.ErrOutboxInvalidEvent))
}
