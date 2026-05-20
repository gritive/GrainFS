package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// Finalize overwrites an attempt with the final request outcome.
func (o *Outbox) Finalize(ctx context.Context, ev S3Event) error {
	if ev.EventID == "" {
		return ErrOutboxInvalidEvent
	}
	ev.Finalized = true
	return o.put(ctx, ev)
}

// AppendFinalized writes an already-final audit event. It is used when a
// follower ships rows that were finalized in its local outbox.
func (o *Outbox) AppendFinalized(ctx context.Context, ev S3Event) error {
	if ev.EventID == "" {
		return ErrOutboxInvalidEvent
	}
	ev.Finalized = true
	return o.put(ctx, ev)
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
