package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/chunkref"
)

const segmentOrphanPrefix = "sgc:"

// SegmentOrphanLog persists t_zero (first-observed-unreferenced time) for raw
// segment blobs, keyed by blob filename. Node-local: writes go straight to
// Badger (never through Raft propose), because orphan status is a fact about
// THIS node's local disk. Keys live under a top-level "sgc:" prefix, outside
// any group's binary FSM prefix, so FSM.Restore's DropPrefix(groupPrefix) leaves
// them intact.
type SegmentOrphanLog struct {
	db      *badger.DB
	groupID string
}

// NewSegmentOrphanLog returns a node-local orphan-segment t_zero log bound to
// the given Badger handle and group.
func NewSegmentOrphanLog(db *badger.DB, groupID string) *SegmentOrphanLog {
	return &SegmentOrphanLog{db: db, groupID: groupID}
}

func (l *SegmentOrphanLog) key(c chunkref.ChunkID) []byte {
	return []byte(segmentOrphanPrefix + l.groupID + ":" + string(c))
}

// Observe records t_zero=now the FIRST time c is seen unreferenced. Idempotent
// first-wins: a later Observe never moves t_zero forward.
func (l *SegmentOrphanLog) Observe(c chunkref.ChunkID, now time.Time) error {
	return l.db.Update(func(txn *badger.Txn) error {
		k := l.key(c)
		if _, err := txn.Get(k); err == nil {
			return nil
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("segment orphan log: get: %w", err)
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(now.UnixNano()))
		if err := txn.Set(k, buf[:]); err != nil {
			return fmt.Errorf("segment orphan log: set: %w", err)
		}
		return nil
	})
}

// Forget removes the orphan record for c. Idempotent: forgetting a non-existent
// entry is not an error.
func (l *SegmentOrphanLog) Forget(c chunkref.ChunkID) error {
	return l.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(l.key(c)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("segment orphan log: delete: %w", err)
		}
		return nil
	})
}

// TombstoneTime returns (t_zero, true, nil) if c has been observed as orphaned,
// or (zero, false, nil) if it has not. Returns a non-nil error only on I/O failure.
func (l *SegmentOrphanLog) TombstoneTime(c chunkref.ChunkID) (time.Time, bool, error) {
	var t time.Time
	var ok bool
	err := l.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(l.key(c))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("segment orphan log: get: %w", err)
		}
		return item.Value(func(v []byte) error {
			if len(v) != 8 {
				return fmt.Errorf("segment orphan log: corrupt value len %d", len(v))
			}
			t = time.Unix(0, int64(binary.BigEndian.Uint64(v)))
			ok = true
			return nil
		})
	})
	return t, ok, err
}

// NewSegmentOrphanLog returns a node-local orphan-segment t_zero log bound to
// this backend's Badger handle and group.
func (b *DistributedBackend) NewSegmentOrphanLog() *SegmentOrphanLog {
	return NewSegmentOrphanLog(b.db, b.groupID)
}
