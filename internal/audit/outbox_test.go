package audit_test

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"sync/atomic"
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

func BenchmarkOutboxAppendAttemptFinalize(b *testing.B) {
	box, err := audit.OpenOutbox(filepath.Join(b.TempDir(), "audit-outbox"))
	require.NoError(b, err)
	defer box.Close()

	ctx := context.Background()
	var seq atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := seq.Add(1)
			ev := audit.S3Event{
				EventID:    "evt-" + strconv.FormatUint(id, 10),
				RequestID:  "req-" + strconv.FormatUint(id, 10),
				Ts:         time.Now().UnixMicro(),
				Method:     "PUT",
				Operation:  "PutObject",
				Bucket:     "bench",
				Key:        "obj-" + strconv.FormatUint(id, 10),
				Status:     0,
				AuthStatus: "allow",
			}
			if err := box.AppendAttempt(ctx, ev); err != nil {
				b.Fatal(err)
			}
			ev.Status = 200
			ev.BytesIn = 64 << 10
			ev.LatencyMs = 10
			if err := box.Finalize(ctx, ev); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// TestOutbox_DenyOnly_DropsAllowEvents verifies that with the deny-only filter
// engaged, Finalize drops allow + anon_allow rows at the durable-write boundary.
// The pre-finalize AppendAttempt row is also cleaned up so the stale-attempt
// reaper does not later resurrect it as "incomplete".
func TestOutbox_DenyOnly_DropsAllowEvents(t *testing.T) {
	ctx := context.Background()
	box, err := audit.OpenOutbox(filepath.Join(t.TempDir(), "audit-outbox"))
	require.NoError(t, err)
	defer box.Close()

	box.SetDenyOnly(true)
	require.True(t, box.DenyOnly())

	cases := []struct {
		name       string
		authStatus string
	}{
		{name: "allow", authStatus: "allow"},
		{name: "anon_allow", authStatus: "anon_allow"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := "evt-allow-" + strconv.Itoa(i)
			attempt := audit.S3Event{EventID: id, RequestID: id, Ts: time.Now().UnixMicro(), Method: "GET", Bucket: "b", Key: "k"}
			require.NoError(t, box.AppendAttempt(ctx, attempt))

			ev := attempt
			ev.Status = 200
			ev.AuthStatus = tc.authStatus
			require.NoError(t, box.Finalize(ctx, ev))

			pending, err := box.Pending(ctx, 10)
			require.NoError(t, err)
			require.Empty(t, pending, "deny-only must drop %s row, and the prior AppendAttempt row must be cleaned so the reaper does not surface it later", tc.authStatus)

			// AppendFinalized (follower-ship path) is also filtered.
			followerID := id + "-follower"
			follower := audit.S3Event{EventID: followerID, RequestID: followerID, Ts: time.Now().UnixMicro(), Method: "GET", Bucket: "b", Key: "k", AuthStatus: tc.authStatus, Status: 200}
			require.NoError(t, box.AppendFinalized(ctx, follower))
			pending, err = box.Pending(ctx, 10)
			require.NoError(t, err)
			require.Empty(t, pending)
		})
	}
}

// TestOutbox_DenyOnly_KeepsDenyEvents verifies that deny rows (and incomplete
// reaper tombstones) remain committable when the filter is engaged.
func TestOutbox_DenyOnly_KeepsDenyEvents(t *testing.T) {
	ctx := context.Background()
	box, err := audit.OpenOutbox(filepath.Join(t.TempDir(), "audit-outbox"))
	require.NoError(t, err)
	defer box.Close()

	box.SetDenyOnly(true)

	denyEv := audit.S3Event{EventID: "evt-deny", RequestID: "r-deny", Ts: time.Now().UnixMicro(), Method: "GET", Bucket: "b", Key: "k", AuthStatus: "deny", Status: 403}
	require.NoError(t, box.AppendAttempt(ctx, denyEv))
	require.NoError(t, box.Finalize(ctx, denyEv))

	// Follower-ship path: deny row from a follower must also flow.
	followerDeny := audit.S3Event{EventID: "evt-deny-follower", RequestID: "r-deny-follower", Ts: time.Now().UnixMicro(), Method: "PUT", Bucket: "b", Key: "k2", AuthStatus: "deny", Status: 403}
	require.NoError(t, box.AppendFinalized(ctx, followerDeny))

	pending, err := box.Pending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 2)
	statuses := map[string]bool{}
	for _, ev := range pending {
		statuses[ev.AuthStatus] = true
	}
	require.True(t, statuses["deny"])
}

// TestOutbox_DenyOnly_AtomicFlip verifies that the filter is a write-time
// gate: rows persisted before the flip remain committable, and rows persisted
// after the flip respect the new policy. Toggling on/off must not lose
// in-flight events that were already durably written.
func TestOutbox_DenyOnly_AtomicFlip(t *testing.T) {
	ctx := context.Background()
	box, err := audit.OpenOutbox(filepath.Join(t.TempDir(), "audit-outbox"))
	require.NoError(t, err)
	defer box.Close()

	// Filter OFF: allow row is persisted.
	ev1 := audit.S3Event{EventID: "evt-1", RequestID: "r1", Ts: time.Now().UnixMicro(), Method: "GET", Bucket: "b", Key: "k1", AuthStatus: "allow", Status: 200}
	require.NoError(t, box.AppendFinalized(ctx, ev1))

	// Flip ON: previously-persisted row remains readable; new allow row is dropped.
	box.SetDenyOnly(true)
	ev2 := audit.S3Event{EventID: "evt-2", RequestID: "r2", Ts: time.Now().UnixMicro(), Method: "GET", Bucket: "b", Key: "k2", AuthStatus: "allow", Status: 200}
	require.NoError(t, box.AppendFinalized(ctx, ev2))

	pending, err := box.Pending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "evt-1", pending[0].EventID, "in-flight rows persisted before the flip must not be lost")

	// Flip OFF again: new allow row now flows through.
	box.SetDenyOnly(false)
	ev3 := audit.S3Event{EventID: "evt-3", RequestID: "r3", Ts: time.Now().UnixMicro(), Method: "GET", Bucket: "b", Key: "k3", AuthStatus: "allow", Status: 200}
	require.NoError(t, box.AppendFinalized(ctx, ev3))

	pending, err = box.Pending(ctx, 10)
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, ev := range pending {
		ids[ev.EventID] = true
	}
	require.True(t, ids["evt-1"], "evt-1 must still be present")
	require.False(t, ids["evt-2"], "evt-2 was dropped while filter was on")
	require.True(t, ids["evt-3"], "evt-3 must flow after filter is turned off")
}

// TestOutbox_DenyOnly_NilSafe verifies that the toggle is nil-safe — production
// wiring may be called against a nil outbox when audit.iceberg is disabled.
func TestOutbox_DenyOnly_NilSafe(t *testing.T) {
	var box *audit.Outbox
	require.NotPanics(t, func() { box.SetDenyOnly(true) })
	require.False(t, box.DenyOnly())
}
