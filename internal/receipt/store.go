package receipt

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	keyPrefix = "receipt:"

	defaultRetention      = 30 * 24 * time.Hour
	defaultFlushThreshold = 100
	defaultFlushInterval  = 50 * time.Millisecond
)

// ErrNotFound is returned by Get when the key has no receipt.
var ErrNotFound = errors.New("receipt: not found")

// ErrUnsigned is returned by Put when the receipt has no signature. The Phase
// 16 audit invariant forbids persisting unsigned receipts.
var ErrUnsigned = errors.New("receipt: unsigned receipt rejected")

// StoreOptions tunes the batching + retention behavior. Zero values fall back
// to package defaults.
type StoreOptions struct {
	// Retention controls the BadgerDB TTL for every receipt. Defaults to 30 days.
	Retention time.Duration
	// FlushThreshold triggers a flush when buffered receipts reach this count.
	FlushThreshold int
	// FlushInterval triggers a flush after this much time passes with buffered receipts.
	FlushInterval time.Duration
}

// Store persists signed receipts in BadgerDB with keys of the form
// "receipt:<ReceiptID>". Writes are buffered and flushed whenever FlushThreshold
// or FlushInterval triggers first; Close drains the buffer synchronously.
//
// Local-only: the Raft FSM never replicates these keys. Callers wire receipt
// availability across peers through gossip (Slice 2).
type Store struct {
	db      *badger.DB
	ttl     time.Duration
	flushAt int
	tick    time.Duration

	mu     sync.Mutex
	buffer []*HealReceipt

	flushReq chan struct{}
	stop     chan struct{}
	done     chan struct{}
	closed   bool
}

// NewStore starts the background batching loop. The caller must invoke Close
// to drain pending writes before process shutdown.
func NewStore(db *badger.DB, opts StoreOptions) (*Store, error) {
	if db == nil {
		return nil, errors.New("receipt: nil db")
	}
	s := &Store{
		db:       db,
		ttl:      opts.Retention,
		flushAt:  opts.FlushThreshold,
		tick:     opts.FlushInterval,
		flushReq: make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	if s.ttl <= 0 {
		s.ttl = defaultRetention
	}
	if s.flushAt <= 0 {
		s.flushAt = defaultFlushThreshold
	}
	if s.tick <= 0 {
		s.tick = defaultFlushInterval
	}

	go s.loop()
	return s, nil
}

// Put enqueues a signed receipt for persistence. Unsigned receipts are
// rejected per the Phase 16 audit invariant (no unsigned receipts persisted).
func (s *Store) Put(r *HealReceipt) error {
	if r == nil {
		return errors.New("receipt: nil receipt")
	}
	if r.Signature == "" || r.CanonicalPayload == "" {
		return ErrUnsigned
	}
	if r.ReceiptID == "" {
		return errors.New("receipt: ReceiptID must not be empty")
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("receipt: store is closed")
	}
	s.buffer = append(s.buffer, r)
	shouldSignal := len(s.buffer) >= s.flushAt
	s.mu.Unlock()

	if shouldSignal {
		// Non-blocking signal: the loop coalesces rapid triggers.
		select {
		case s.flushReq <- struct{}{}:
		default:
		}
	}
	return nil
}

// Get fetches a receipt by ReceiptID. Returns ErrNotFound when absent (which
// includes the TTL-expired case).
func (s *Store) Get(id string) (*HealReceipt, error) {
	if id == "" {
		return nil, errors.New("receipt: empty id")
	}
	var out *HealReceipt
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(receiptKey(id))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("receipt: badger get: %w", err)
		}
		return item.Value(func(val []byte) error {
			var r HealReceipt
			if err := json.Unmarshal(val, &r); err != nil {
				return fmt.Errorf("receipt: decode %q: %w", id, err)
			}
			out = &r
			return nil
		})
	})
	return out, err
}

// Flush writes any buffered receipts immediately. Tests use this to bypass
// the timer.
func (s *Store) Flush() error {
	return s.drain()
}

// Close stops the background loop and drains any remaining buffered receipts.
// Calling Close more than once is safe; subsequent calls return nil.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	close(s.stop)
	<-s.done
	return s.drain()
}

func (s *Store) loop() {
	defer close(s.done)
	t := time.NewTicker(s.tick)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-s.flushReq:
			_ = s.drain()
		case <-t.C:
			_ = s.drain()
		}
	}
}

func (s *Store) drain() error {
	s.mu.Lock()
	if len(s.buffer) == 0 {
		s.mu.Unlock()
		return nil
	}
	batch := s.buffer
	s.buffer = nil
	s.mu.Unlock()

	err := s.db.Update(func(txn *badger.Txn) error {
		for _, r := range batch {
			data, err := json.Marshal(r)
			if err != nil {
				return fmt.Errorf("receipt: encode %q: %w", r.ReceiptID, err)
			}
			entry := badger.NewEntry(receiptKey(r.ReceiptID), data).WithTTL(s.ttl)
			if err := txn.SetEntry(entry); err != nil {
				return fmt.Errorf("receipt: badger set %q: %w", r.ReceiptID, err)
			}
		}
		return nil
	})
	if err != nil {
		// Retry the whole batch once; BadgerDB txn-conflict is the realistic
		// transient failure and a second attempt usually clears it.
		if retryErr := s.db.Update(func(txn *badger.Txn) error {
			for _, r := range batch {
				data, err := json.Marshal(r)
				if err != nil {
					return err
				}
				entry := badger.NewEntry(receiptKey(r.ReceiptID), data).WithTTL(s.ttl)
				if err := txn.SetEntry(entry); err != nil {
					return err
				}
			}
			return nil
		}); retryErr != nil {
			return fmt.Errorf("receipt: flush (after retry): %w", retryErr)
		}
	}
	return nil
}

func receiptKey(id string) []byte {
	return []byte(keyPrefix + id)
}
