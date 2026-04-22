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
	keyPrefix     = "receipt:"
	tsIndexPrefix = "ts:"
	cidxPrefix    = "cidx:"

	// tsNanoWidth pads the decimal unix-nano timestamp so lexicographic key
	// order matches chronological order. int64 max (9223372036854775807) is
	// 19 digits; narrower values are left-padded with zeros.
	tsNanoWidth = 19

	defaultRetention      = 30 * 24 * time.Hour
	defaultFlushThreshold = 100
	defaultFlushInterval  = 50 * time.Millisecond
)

// ErrNotFound is returned by Get when the key has no receipt.
var ErrNotFound = errors.New("receipt: not found")

// ErrUnsigned is returned by Put when the receipt has no signature. The Phase
// 16 audit invariant forbids persisting unsigned receipts.
var ErrUnsigned = errors.New("receipt: unsigned receipt rejected")

// ErrInvalidTimestamp is returned by Put when Timestamp is zero/negative.
// tsIndexKey padding (%019d) assumes a non-negative unix-nano value;
// negative values print "-…" which sorts before digits and would corrupt
// both chronological List ordering and RecentReceiptIDs reverse scan.
var ErrInvalidTimestamp = errors.New("receipt: Timestamp must be non-zero and after epoch")

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

	// drainMu serializes drain() calls so the background ticker and an
	// explicit Flush() never issue overlapping BadgerDB write transactions.
	// Overlapping txns lose writes silently under high-signal rates.
	drainMu sync.Mutex

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
	if r.Timestamp.IsZero() || r.Timestamp.UnixNano() < 0 {
		return ErrInvalidTimestamp
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
	s.drainMu.Lock()
	defer s.drainMu.Unlock()

	s.mu.Lock()
	if len(s.buffer) == 0 {
		s.mu.Unlock()
		return nil
	}
	batch := s.buffer
	s.buffer = nil
	s.mu.Unlock()

	err := s.db.Update(func(txn *badger.Txn) error {
		return writeBatch(txn, batch, s.ttl)
	})
	if err != nil {
		// Retry the whole batch once; BadgerDB txn-conflict is the realistic
		// transient failure and a second attempt usually clears it.
		if retryErr := s.db.Update(func(txn *badger.Txn) error {
			return writeBatch(txn, batch, s.ttl)
		}); retryErr != nil {
			return fmt.Errorf("receipt: flush (after retry): %w", retryErr)
		}
	}
	return nil
}

// writeBatch writes each receipt's primary key (receipt:<id> → JSON),
// secondary time-index key (ts:<unix_nano>:<id> → id), and optional
// correlation-index key (cidx:<correlation_id> → id) inside the same txn.
func writeBatch(txn *badger.Txn, batch []*HealReceipt, ttl time.Duration) error {
	for _, r := range batch {
		data, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("receipt: encode %q: %w", r.ReceiptID, err)
		}
		primary := badger.NewEntry(receiptKey(r.ReceiptID), data).WithTTL(ttl)
		if err := txn.SetEntry(primary); err != nil {
			return fmt.Errorf("receipt: badger set %q: %w", r.ReceiptID, err)
		}
		idx := badger.NewEntry(tsIndexKey(r.Timestamp, r.ReceiptID), []byte(r.ReceiptID)).WithTTL(ttl)
		if err := txn.SetEntry(idx); err != nil {
			return fmt.Errorf("receipt: badger set ts-index %q: %w", r.ReceiptID, err)
		}
		if r.CorrelationID != "" {
			cidx := badger.NewEntry(cidxKey(r.CorrelationID), []byte(r.ReceiptID)).WithTTL(ttl)
			if err := txn.SetEntry(cidx); err != nil {
				return fmt.Errorf("receipt: badger set cidx %q: %w", r.ReceiptID, err)
			}
		}
	}
	return nil
}

// LookupReceiptJSON fetches the raw JSON-encoded receipt bytes for id.
// Used by the cluster's ReceiptQueryHandler to answer broadcast-fallback
// queries; returns (nil, false) when the id is unknown or its TTL expired.
// Cheaper than Get when the caller only needs to forward bytes across the
// network — no json.Unmarshal → re-marshal round trip.
func (s *Store) LookupReceiptJSON(id string) ([]byte, bool) {
	if id == "" {
		return nil, false
	}
	var out []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(receiptKey(id))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			out = make([]byte, len(v))
			copy(out, v)
			return nil
		})
	})
	if err != nil {
		return nil, false
	}
	return out, true
}

// RecentReceiptIDs returns the most recent up-to-max receipt IDs, ordered
// newest-first. Used by the gossip sender to build a rolling window payload
// every tick. Walks the ts:* secondary index in reverse; cost is O(max).
//
// Fresh slice per call — safe for the gossip sender to hand to transport
// without copying.
func (s *Store) RecentReceiptIDs(max int) []string {
	if max <= 0 {
		return nil
	}
	out := make([]string, 0, max)
	_ = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		// Seek to just-past-end of ts:* space. A key one byte greater than
		// "ts:" but still matching the prefix would work; simplest is to
		// seek to the lexicographically-last possible ts: key.
		it.Seek([]byte(tsIndexPrefix + "\xff"))
		prefix := []byte(tsIndexPrefix)
		for ; it.ValidForPrefix(prefix) && len(out) < max; it.Next() {
			var id string
			if err := it.Item().Value(func(v []byte) error {
				id = string(v)
				return nil
			}); err != nil {
				return err
			}
			out = append(out, id)
		}
		return nil
	})
	return out
}

// List returns receipts whose Timestamp falls in [from, to), ordered by
// ascending timestamp. limit caps the result size; pass 0 for no limit.
//
// The scan walks the ts:<unix_nano>:<id> secondary index then fetches each
// primary key, so cost is O(result size) rather than O(total receipts).
func (s *Store) List(from, to time.Time, limit int) ([]*HealReceipt, error) {
	if !to.After(from) {
		return nil, nil
	}
	startKey := tsIndexKey(from, "")
	// Exclusive upper bound: use to's nano as key prefix; any matching entry
	// must have a key strictly less than tsIndexKey(to, "").
	endKey := tsIndexKey(to, "")

	var out []*HealReceipt
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(tsIndexPrefix)
		for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
			if limit > 0 && len(out) >= limit {
				break
			}
			k := it.Item().Key()
			if bytesGTE(k, endKey) {
				break
			}
			var id string
			if err := it.Item().Value(func(v []byte) error {
				id = string(v)
				return nil
			}); err != nil {
				return fmt.Errorf("receipt: read ts-index value: %w", err)
			}
			item, err := txn.Get(receiptKey(id))
			if errors.Is(err, badger.ErrKeyNotFound) {
				// Primary gone but index lingered (TTL skew). Skip defensively.
				continue
			}
			if err != nil {
				return fmt.Errorf("receipt: fetch primary %q: %w", id, err)
			}
			var r HealReceipt
			if err := item.Value(func(v []byte) error {
				return json.Unmarshal(v, &r)
			}); err != nil {
				return fmt.Errorf("receipt: decode %q: %w", id, err)
			}
			out = append(out, &r)
		}
		return nil
	})
	return out, err
}

func bytesGTE(a, b []byte) bool {
	// bytes.Compare returns -1,0,1; GTE means a >= b.
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] > b[i]
		}
	}
	return len(a) >= len(b)
}

// GetByCorrelationID returns the receipt whose CorrelationID matches, or
// ErrNotFound when no such receipt exists or its TTL has expired.
// Both the cidx lookup and the primary receipt read occur in a single
// transaction to avoid a TOCTOU window where the primary expires between reads.
func (s *Store) GetByCorrelationID(correlationID string) (*HealReceipt, error) {
	if correlationID == "" {
		return nil, errors.New("receipt: empty correlation_id")
	}
	var out *HealReceipt
	err := s.db.View(func(txn *badger.Txn) error {
		cidxItem, err := txn.Get(cidxKey(correlationID))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("receipt: badger get cidx: %w", err)
		}
		var receiptID string
		if err := cidxItem.Value(func(v []byte) error {
			receiptID = string(v)
			return nil
		}); err != nil {
			return err
		}
		item, err := txn.Get(receiptKey(receiptID))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("receipt: badger get receipt: %w", err)
		}
		return item.Value(func(val []byte) error {
			var r HealReceipt
			if err := json.Unmarshal(val, &r); err != nil {
				return fmt.Errorf("receipt: decode %q: %w", receiptID, err)
			}
			out = &r
			return nil
		})
	})
	return out, err
}

func receiptKey(id string) []byte {
	return []byte(keyPrefix + id)
}

func cidxKey(correlationID string) []byte {
	return []byte(cidxPrefix + correlationID)
}

// tsIndexKey produces "ts:<unix_nano_padded>:<id>" so BadgerDB's lexicographic
// iteration yields chronological order. Pass id="" to form a seek boundary
// (start/end of a time range).
func tsIndexKey(ts time.Time, id string) []byte {
	// %019d pads to 19 digits, matching int64 max width. Unix-nano fits int64.
	return fmt.Appendf(nil, "%s%0*d:%s", tsIndexPrefix, tsNanoWidth, ts.UnixNano(), id)
}
