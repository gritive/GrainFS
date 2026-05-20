package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/badgerutil"
)

var ErrOutboxInvalidEvent = errors.New("audit outbox invalid event")

const staleAttemptAfter = 5 * time.Minute
const committedEventTombstoneTTL = 7 * 24 * time.Hour
const ackEventsChunkSize = 1000

// Outbox stores audit events durably until the Iceberg committer acks them.
type Outbox struct {
	db *badger.DB
	// denyOnly, when true, drops finalized allow/anon_allow events at the
	// durable-write boundary (Finalize, AppendFinalized). See SetDenyOnly.
	denyOnly atomic.Bool
}

type OutboxStats struct {
	Backlog         int   `json:"backlog"`
	OldestPendingUS int64 `json:"oldest_pending_us"`
}

// OpenOutbox opens or creates a local durable audit outbox.
func OpenOutbox(dir string) (*Outbox, error) {
	opts := badgerutil.SmallOptions(dir)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Outbox{db: db}, nil
}

func (o *Outbox) Close() error {
	if o == nil || o.db == nil {
		return nil
	}
	return o.db.Close()
}

// AppendAttempt writes the initial durable audit attempt. Replays are idempotent.
func (o *Outbox) AppendAttempt(ctx context.Context, ev S3Event) error {
	if ev.EventID == "" {
		return ErrOutboxInvalidEvent
	}
	ev.Finalized = false
	return o.db.Update(func(txn *badger.Txn) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		key := outboxKey(ev.EventID)
		if _, err := txn.Get(key); err == nil {
			return nil
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		raw, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		return txn.Set(key, raw)
	})
}

// SetDenyOnly toggles deny-only filtering. When true, Finalize and
// AppendFinalized drop allow/anon_allow events at the durable-write
// boundary. The toggle is wired to the audit.deny-only config reload
// hook (see internal/serveruntime/boot_phases_raft.go).
//
// Filtering is applied only at write time, not at read time: rows that
// were already persisted before a true→false flip remain committable,
// and rows already persisted before a false→true flip continue to flow.
// This is the "AtomicFlip doesn't lose in-flight events" guarantee
// exercised by TestOutbox_DenyOnly_AtomicFlip.
func (o *Outbox) SetDenyOnly(v bool) {
	if o == nil {
		return
	}
	o.denyOnly.Store(v)
}

// DenyOnly reports the current filter state. EXPORTED FOR TESTS AND
// DIAGNOSTICS ONLY. Production code must never branch on this — it's a
// write-path filter, not a decision gate. The atomic load is not
// synchronized with any in-flight write, so a caller using DenyOnly() to
// decide whether to record an event would race with the filter at the
// durable-write point.
func (o *Outbox) DenyOnly() bool {
	if o == nil {
		return false
	}
	return o.denyOnly.Load()
}

// denyOnlyDropsFinalized reports whether the deny-only filter would drop ev
// at the durable-write boundary. Only finalized rows with AuthStatus in
// {"allow","anon_allow"} are dropped; "deny", "incomplete", and any unknown
// status are kept (deny is the row we want to keep; incomplete is the
// reaper's own tombstone; unknown statuses are conservatively retained).
func (o *Outbox) denyOnlyDropsFinalized(ev S3Event) bool {
	if !o.denyOnly.Load() {
		return false
	}
	return ev.AuthStatus == "allow" || ev.AuthStatus == "anon_allow"
}

// Finalize overwrites an attempt with the final request outcome.
//
// When deny-only filtering is enabled, finalized allow/anon_allow rows are
// dropped here — we also delete any pre-finalize AppendAttempt row for the
// same EventID so the stale-attempt reaper does not later resurrect them as
// "incomplete". AppendAttempt itself is intentionally NOT filtered: at that
// point the auth outcome is not yet known.
func (o *Outbox) Finalize(ctx context.Context, ev S3Event) error {
	if ev.EventID == "" {
		return ErrOutboxInvalidEvent
	}
	ev.Finalized = true
	if o.denyOnlyDropsFinalized(ev) {
		return o.dropAttempt(ctx, ev.EventID)
	}
	return o.put(ctx, ev)
}

// AppendFinalized writes an already-final audit event. It is used when a
// follower ships rows that were finalized in its local outbox.
//
// Deny-only filtering applies here too — without this, allow rows shipped
// from followers would bypass the leader's filter (committer.AppendFromFollower
// routes through this method).
func (o *Outbox) AppendFinalized(ctx context.Context, ev S3Event) error {
	if ev.EventID == "" {
		return ErrOutboxInvalidEvent
	}
	ev.Finalized = true
	if o.denyOnlyDropsFinalized(ev) {
		return nil
	}
	return o.put(ctx, ev)
}

// dropAttempt removes any pre-finalize AppendAttempt row for the given
// EventID. Used when Finalize decides to drop the finalized row but a
// provisional attempt row may already exist on disk.
func (o *Outbox) dropAttempt(ctx context.Context, eventID string) error {
	return o.db.Update(func(txn *badger.Txn) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := txn.Delete(outboxKey(eventID)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	})
}

func (o *Outbox) put(ctx context.Context, ev S3Event) error {
	return o.db.Update(func(txn *badger.Txn) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if ev.Finalized {
			if _, err := txn.Get(committedOutboxKey(ev.EventID)); err == nil {
				return nil
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		raw, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		return txn.Set(outboxKey(ev.EventID), raw)
	})
}

// Pending returns up to limit durable events awaiting commit.
func (o *Outbox) Pending(ctx context.Context, limit int) ([]S3Event, error) {
	if limit <= 0 {
		limit = 1000
	}
	out := make([]S3Event, 0, limit)
	now := time.Now()
	err := o.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("audit/outbox/")
		for it.Seek(prefix); it.ValidForPrefix(prefix) && len(out) < limit; it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			item := it.Item()
			if err := item.Value(func(raw []byte) error {
				var ev S3Event
				if err := json.Unmarshal(raw, &ev); err != nil {
					return err
				}
				ready, ok := outboxCommitReady(ev, now)
				if !ok {
					return nil
				}
				out = append(out, ready)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return out, err
}

// Ack removes committed events from the outbox.
func (o *Outbox) Ack(ctx context.Context, eventIDs []string) error {
	return o.db.Update(func(txn *badger.Txn) error {
		for _, id := range eventIDs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if id == "" {
				return ErrOutboxInvalidEvent
			}
			entry := badger.NewEntry(committedOutboxKey(id), []byte{1}).WithTTL(committedEventTombstoneTTL)
			if err := txn.SetEntry(entry); err != nil {
				return err
			}
			if err := txn.Delete(outboxKey(id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		return nil
	})
}

// AckEvents removes committed events from the outbox. Finalized events receive a
// short-lived tombstone so follower replays do not duplicate rows. Provisional
// stale-attempt rows intentionally do not get tombstoned; a still-running
// request may finalize later with the real status, bytes, and latency.
func (o *Outbox) AckEvents(ctx context.Context, events []S3Event) error {
	for len(events) > 0 {
		n := ackEventsChunkSize
		if len(events) < n {
			n = len(events)
		}
		if err := o.ackEventsChunk(ctx, events[:n]); err != nil {
			return err
		}
		events = events[n:]
	}
	return nil
}

func (o *Outbox) ackEventsChunk(ctx context.Context, events []S3Event) error {
	return o.db.Update(func(txn *badger.Txn) error {
		for _, ev := range events {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if ev.EventID == "" {
				return ErrOutboxInvalidEvent
			}
			if shouldTombstoneCommittedEvent(ev) {
				entry := badger.NewEntry(committedOutboxKey(ev.EventID), []byte{1}).WithTTL(committedEventTombstoneTTL)
				if err := txn.SetEntry(entry); err != nil {
					return err
				}
			}
			if err := txn.Delete(outboxKey(ev.EventID)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		return nil
	})
}

func (o *Outbox) Stats(ctx context.Context) (OutboxStats, error) {
	var oldest int64
	var backlog int
	now := time.Now()
	err := o.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("audit/outbox/")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			item := it.Item()
			if err := item.Value(func(raw []byte) error {
				var ev S3Event
				if err := json.Unmarshal(raw, &ev); err != nil {
					return err
				}
				ready, ok := outboxCommitReady(ev, now)
				if !ok {
					return nil
				}
				if ready.Ts != 0 && (oldest == 0 || ready.Ts < oldest) {
					oldest = ready.Ts
				}
				backlog++
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return OutboxStats{}, err
	}
	stats := OutboxStats{Backlog: backlog, OldestPendingUS: oldest}
	auditOutboxBacklog.Set(float64(stats.Backlog))
	auditOutboxOldestPendingUS.Set(float64(stats.OldestPendingUS))
	return stats, nil
}

func shouldTombstoneCommittedEvent(ev S3Event) bool {
	return ev.Finalized && !(ev.AuthStatus == "incomplete" && ev.ErrReason == "request_incomplete")
}

func outboxCommitReady(ev S3Event, now time.Time) (S3Event, bool) {
	if ev.Finalized {
		return ev, true
	}
	if ev.Ts == 0 {
		return S3Event{}, false
	}
	created := time.UnixMicro(ev.Ts)
	if now.Sub(created) < staleAttemptAfter {
		return S3Event{}, false
	}
	ev.Finalized = true
	ev.AuthStatus = "incomplete"
	ev.ErrClass = "Incomplete"
	ev.ErrReason = "request_incomplete"
	return ev, true
}

func outboxKey(eventID string) []byte {
	return []byte(fmt.Sprintf("audit/outbox/%s", eventID))
}

func committedOutboxKey(eventID string) []byte {
	return []byte(fmt.Sprintf("audit/committed/%s", eventID))
}
