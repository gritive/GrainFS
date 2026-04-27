// Package dedup implements block-level deduplication for GrainFS volumes.
// Inline SHA-256 hash check at write time; BadgerDB tracks refcounts.
package dedup

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
)

// DedupIndex tracks which S3 objects are shared by multiple blocks.
// A nil DedupIndex disables deduplication (all writes pass through unchanged).
type DedupIndex interface {
	// LookupOrRegister checks whether a block with the given SHA-256 hash
	// already exists. If it does, it increments the refcount and returns the
	// canonical objectKey (isNew=false). If not, it registers newKey as the
	// canonical key (isNew=true, refcount=1).
	//
	// Callers must call backend.PutObject only when isNew=true.
	LookupOrRegister(hash [32]byte, newKey string) (canonical string, isNew bool, err error)

	// Release decrements the refcount for objectKey.
	// Returns shouldDelete=true when refcount reaches zero — the caller is
	// responsible for deleting the S3 object.
	// Returns shouldDelete=false if objectKey is not tracked by dedup
	// (legacy or non-dedup object); the caller should use its existing GC path.
	Release(objectKey string) (shouldDelete bool, err error)
}

const (
	hashPrefix = "vd:h:" // vd:h:{sha256hex} → objectKey
	refPrefix  = "vd:r:" // vd:r:{objectKey} → {refcount int32 BE, hash [32]byte}
	maxRetries = 3
)

type badgerIndex struct {
	db *badger.DB
	// mu serializes LookupOrRegister to prevent BadgerDB ErrConflict on
	// concurrent writes with the same hash. In production, Manager.mu already
	// serializes WriteAt calls; this mutex is a safety net for concurrent
	// cross-volume dedup on a shared DB.
	mu sync.Mutex
}

// NewBadgerIndex returns a DedupIndex backed by the given BadgerDB instance.
// The caller owns the db lifecycle; NewBadgerIndex does not close it.
func NewBadgerIndex(db *badger.DB) DedupIndex {
	return &badgerIndex{db: db}
}

func (b *badgerIndex) LookupOrRegister(hash [32]byte, newKey string) (string, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	hashKey := hashPrefix + hex.EncodeToString(hash[:])

	var canonical string
	var isNew bool

	err := b.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(hashKey))
		if err == nil {
			// Existing hash → increment refcount
			return item.Value(func(existingKey []byte) error {
				canonical = string(existingKey)
				isNew = false
				return incrRefcount(txn, canonical)
			})
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		// New block — register
		canonical = newKey
		isNew = true
		if err := txn.Set([]byte(hashKey), []byte(newKey)); err != nil {
			return err
		}
		return setRefcount(txn, newKey, 1, hash)
	})
	return canonical, isNew, err
}

func (b *badgerIndex) Release(objectKey string) (bool, error) {
	refKey := []byte(refPrefix + objectKey)

	var shouldDelete bool
	err := retry(maxRetries, func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get(refKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				shouldDelete = false
				return nil // not a dedup object
			}
			if err != nil {
				return err
			}

			return item.Value(func(val []byte) error {
				rc, hash := decodeRefVal(val)
				if rc > 1 {
					shouldDelete = false
					return setRefcount(txn, objectKey, rc-1, hash)
				}
				// Last reference — clean up both index entries
				shouldDelete = true
				hashKey := []byte(hashPrefix + hex.EncodeToString(hash[:]))
				if err := txn.Delete(hashKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				return txn.Delete(refKey)
			})
		})
	})
	return shouldDelete, err
}

// retry executes fn up to n times, retrying on badger.ErrConflict.
func retry(n int, fn func() error) error {
	var err error
	for i := 0; i < n; i++ {
		err = fn()
		if !errors.Is(err, badger.ErrConflict) {
			return err
		}
	}
	return err
}

// encodeRefVal encodes {refcount int32 BE, hash [32]byte} into 36 bytes.
func encodeRefVal(rc int32, hash [32]byte) []byte {
	buf := make([]byte, 36)
	binary.BigEndian.PutUint32(buf[:4], uint32(rc))
	copy(buf[4:], hash[:])
	return buf
}

// decodeRefVal decodes a 36-byte ref value.
func decodeRefVal(val []byte) (int32, [32]byte) {
	var hash [32]byte
	if len(val) < 36 {
		return 0, hash
	}
	rc := int32(binary.BigEndian.Uint32(val[:4]))
	copy(hash[:], val[4:36])
	return rc, hash
}

func setRefcount(txn *badger.Txn, objectKey string, rc int32, hash [32]byte) error {
	return txn.Set([]byte(refPrefix+objectKey), encodeRefVal(rc, hash))
}

func incrRefcount(txn *badger.Txn, objectKey string) error {
	item, err := txn.Get([]byte(refPrefix + objectKey))
	if err != nil {
		return err
	}
	return item.Value(func(val []byte) error {
		rc, hash := decodeRefVal(val)
		return setRefcount(txn, objectKey, rc+1, hash)
	})
}
