package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

var ErrOutboxInvalidEvent = errors.New("audit outbox invalid event")

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
	opts := badger.DefaultOptions(dir).WithLogger(nil)
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
	return o.db.Update(func(txn *badger.Txn) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
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
				out = append(out, ev)
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
			if err := txn.Delete(outboxKey(id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		return nil
	})
}

func (o *Outbox) Stats(ctx context.Context) (OutboxStats, error) {
	events, err := o.Pending(ctx, 100000)
	if err != nil {
		return OutboxStats{}, err
	}
	var oldest int64
	for _, ev := range events {
		if ev.Ts != 0 && (oldest == 0 || ev.Ts < oldest) {
			oldest = ev.Ts
		}
	}
	stats := OutboxStats{Backlog: len(events), OldestPendingUS: oldest}
	auditOutboxBacklog.Set(float64(stats.Backlog))
	auditOutboxOldestPendingUS.Set(float64(stats.OldestPendingUS))
	return stats, nil
}

func outboxKey(eventID string) []byte {
	return []byte(fmt.Sprintf("audit/outbox/%s", eventID))
}
