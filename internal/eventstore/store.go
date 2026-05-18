package eventstore

import (
	"encoding/binary"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	EventTypeS3     = "s3"
	EventTypeSystem = "system"

	EventActionPut          = "put"
	EventActionGet          = "get"
	EventActionDelete       = "delete"
	EventActionCreateBucket = "create-bucket"
	EventActionDeleteBucket = "delete-bucket"

	EventActionSnapshotCreate    = "snapshot-create"
	EventActionSnapshotRestore   = "snapshot-restore"
	EventActionSnapshotDelete    = "snapshot-delete"
	EventActionClusterJoin       = "cluster-join"
	EventActionClusterRemovePeer = "cluster-remove-peer"
	EventActionScrubComplete     = "scrub-complete"

	keyPrefix = "ev:"
	ttl       = 7 * 24 * time.Hour
)

// Event represents a single auditable event.
//
// Fields below the first 6 are optional and only populated for specific event
// types (heal events, cluster membership). They are typed top-level fields
// rather than a polymorphic map[string]any so the FB schema, the BadgerDB
// storage format, and the /api/eventlog wire format share one definition.
type Event struct {
	Timestamp int64  `json:"ts"`
	Type      string `json:"type"`
	Action    string `json:"action"`
	Bucket    string `json:"bucket,omitempty"`
	Key       string `json:"key,omitempty"`
	Size      int64  `json:"size,omitempty"`

	// Heal-event fields (populated by healEmitter). Mirror scrubber.HealEvent
	// minus Timestamp/Bucket/Key which already live on the parent Event.
	ID            string `json:"id,omitempty"`
	Phase         string `json:"phase,omitempty"`
	Outcome       string `json:"outcome,omitempty"`
	ShardID       int32  `json:"shard_id,omitempty"`
	PeerID        string `json:"peer_id,omitempty"`
	BytesRepaired int64  `json:"bytes_repaired,omitempty"`
	DurationMs    int64  `json:"duration_ms,omitempty"`
	ErrCode       string `json:"err_code,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	VersionID     string `json:"version_id,omitempty"`

	// Cluster-membership fields (populated by removeClusterPeer).
	RemovedID string `json:"removed_id,omitempty"`
	Force     bool   `json:"force,omitempty"`
}

// Store persists events in BadgerDB with "ev:" key prefix.
type Store struct{ db *badger.DB }

// New returns a Store backed by the given BadgerDB instance.
func New(db *badger.DB) *Store { return &Store{db: db} }

// Append writes e to the store with a 7-day TTL.
// The key is ev:{big-endian uint64 unix nanoseconds}.
func (s *Store) Append(e Event) error {
	if e.Timestamp == 0 {
		e.Timestamp = time.Now().UnixNano()
	}
	data, err := encodeEvent(e)
	if err != nil {
		return err
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(e.Timestamp))
	key := append([]byte(keyPrefix), buf[:]...)

	return s.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(key, data).WithTTL(ttl)
		return txn.SetEntry(entry)
	})
}

// Query returns events in [since, until] with optional type filter.
// Returns at most limit events (0 = empty). nil types = all types.
func (s *Store) Query(since, until time.Time, limit int, types []string) ([]Event, error) {
	if limit <= 0 {
		return nil, nil
	}

	var sinceKey [8]byte
	var untilKey [8]byte
	binary.BigEndian.PutUint64(sinceKey[:], uint64(since.UnixNano()))
	binary.BigEndian.PutUint64(untilKey[:], uint64(until.UnixNano()))

	prefix := []byte(keyPrefix)
	sinceK := append([]byte(keyPrefix), sinceKey[:]...)
	untilK := append([]byte(keyPrefix), untilKey[:]...)

	typeSet := make(map[string]bool, len(types))
	for _, t := range types {
		typeSet[t] = true
	}
	allTypes := len(types) == 0

	var results []Event
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(sinceK); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()

			// Stop when past until
			if string(k) > string(untilK) {
				break
			}

			var e Event
			if err := item.Value(func(v []byte) error {
				var decErr error
				e, decErr = decodeEventStorage(v)
				return decErr
			}); err != nil {
				continue
			}

			if !allTypes && !typeSet[e.Type] {
				continue
			}

			results = append(results, e)
			if len(results) >= limit {
				break
			}
		}
		return nil
	})
	return results, err
}
