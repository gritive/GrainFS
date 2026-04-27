// Package dedup implements block-level deduplication for GrainFS volumes.
// Block mappings are stored in BadgerDB (vd:b: key space) to avoid per-write
// S3 live_map serialization. SHA-256 hash check at write time; BadgerDB tracks
// refcounts per canonical S3 object key.
package dedup

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
)

// WriteResult holds the outcome of a WriteBlock call.
type WriteResult struct {
	Canonical string // objectKey to use; call backend.PutObject when IsNew=true
	IsNew     bool   // true = first time this hash is seen; caller must store the object
	ToDelete  string // non-empty = previous block's object has refcount 0; caller must DeleteObject
}

// DedupIndex tracks block-level deduplication state in BadgerDB.
// A nil DedupIndex disables deduplication (all writes pass through unchanged).
//
// Volume names must not contain ':' (used as the key separator for vd:b: entries).
type DedupIndex interface {
	// WriteBlock atomically records a write to (vol, blkNum) with the given hash.
	// Canonical is the S3 key to use for this block.
	// When IsNew=true, the caller must call backend.PutObject(Canonical, data).
	// When ToDelete is non-empty, the previous block object is unreferenced; caller must DeleteObject(ToDelete).
	WriteBlock(vol string, blkNum int64, hash [32]byte, newKey string) (WriteResult, error)

	// ReadBlock returns the canonical S3 object key for (vol, blkNum).
	// Returns found=false when the block has never been written.
	ReadBlock(vol string, blkNum int64) (canonical string, found bool, err error)

	// FreeBlock removes the block mapping for (vol, blkNum) and decrements the
	// corresponding object's refcount. Returns objectKey="" when the block was never written.
	// Returns shouldDelete=true when refcount reaches zero — caller must DeleteObject.
	FreeBlock(vol string, blkNum int64) (objectKey string, shouldDelete bool, err error)

	// DeleteVolume removes all block mappings for vol and decrements refcounts.
	// Returns the S3 object keys that must be deleted (those whose refcount reached zero).
	DeleteVolume(vol string) (toDelete []string, err error)
}

const (
	hashPrefix  = "vd:h:" // vd:h:{sha256hex} → canonicalKey
	refPrefix   = "vd:r:" // vd:r:{objectKey} → {refcount int32 BE, hash [32]byte}
	blockPrefix = "vd:b:" // vd:b:{vol}:{blkNum:12-digit} → canonicalKey
	maxRetries  = 3
)

type badgerIndex struct {
	db *badger.DB
	// mu serializes WriteBlock/FreeBlock to prevent BadgerDB ErrConflict on
	// concurrent cross-volume operations sharing the same hash.
	// Within a single volume, Manager.mu already serializes all calls.
	mu sync.Mutex
}

// NewBadgerIndex returns a DedupIndex backed by the given BadgerDB instance.
// The caller owns the db lifecycle; NewBadgerIndex does not close it.
func NewBadgerIndex(db *badger.DB) DedupIndex {
	return &badgerIndex{db: db}
}

func blockBadgerKey(vol string, blkNum int64) []byte {
	return []byte(fmt.Sprintf("%s%s:%012d", blockPrefix, vol, blkNum))
}

func blockBadgerPrefix(vol string) []byte {
	return []byte(blockPrefix + vol + ":")
}

func (b *badgerIndex) WriteBlock(vol string, blkNum int64, hash [32]byte, newKey string) (WriteResult, error) {
	if strings.Contains(vol, ":") {
		return WriteResult{}, fmt.Errorf("dedup: volume name %q must not contain ':'", vol)
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	blkKey := blockBadgerKey(vol, blkNum)
	hashKey := []byte(hashPrefix + hex.EncodeToString(hash[:]))

	var result WriteResult

	err := retry(maxRetries, func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			result = WriteResult{}

			// 1. Load previous block mapping
			var prevCanonical string
			item, err := txn.Get(blkKey)
			if err == nil {
				if err := item.Value(func(v []byte) error {
					prevCanonical = string(v)
					return nil
				}); err != nil {
					return err
				}
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}

			// 2. Resolve canonical key via hash index
			item, err = txn.Get(hashKey)
			if err == nil {
				// Existing hash — reuse canonical key
				if err := item.Value(func(v []byte) error {
					result.Canonical = string(v)
					return nil
				}); err != nil {
					return err
				}
				result.IsNew = false
				if prevCanonical != result.Canonical {
					// Different object: increment refcount for the new canonical
					if err := incrRefcount(txn, result.Canonical); err != nil {
						return err
					}
				}
				// prevCanonical == result.Canonical: same content, no-op (refcount unchanged)
			} else if errors.Is(err, badger.ErrKeyNotFound) {
				// New hash — register newKey as canonical
				result.Canonical = newKey
				result.IsNew = true
				if err := txn.Set(hashKey, []byte(newKey)); err != nil {
					return err
				}
				if err := setRefcount(txn, newKey, 1, hash); err != nil {
					return err
				}
			} else {
				return err
			}

			// 3. Release previous block's object if it changed
			if prevCanonical != "" && prevCanonical != result.Canonical {
				shouldDel, prevHash, err := decrementRefcount(txn, prevCanonical)
				if err != nil {
					return err
				}
				if shouldDel {
					prevHashKey := []byte(hashPrefix + hex.EncodeToString(prevHash[:]))
					if e := txn.Delete(prevHashKey); e != nil && !errors.Is(e, badger.ErrKeyNotFound) {
						return e
					}
					if e := txn.Delete([]byte(refPrefix + prevCanonical)); e != nil && !errors.Is(e, badger.ErrKeyNotFound) {
						return e
					}
					result.ToDelete = prevCanonical
				}
			}

			// 4. Update block mapping
			return txn.Set(blkKey, []byte(result.Canonical))
		})
	})
	return result, err
}

func (b *badgerIndex) ReadBlock(vol string, blkNum int64) (string, bool, error) {
	blkKey := blockBadgerKey(vol, blkNum)
	var canonical string
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(blkKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			canonical = string(v)
			return nil
		})
	})
	return canonical, canonical != "", err
}

func (b *badgerIndex) FreeBlock(vol string, blkNum int64) (string, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	blkKey := blockBadgerKey(vol, blkNum)
	var objectKey string
	var shouldDelete bool

	err := retry(maxRetries, func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			objectKey = ""
			shouldDelete = false

			item, err := txn.Get(blkKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := item.Value(func(v []byte) error {
				objectKey = string(v)
				return nil
			}); err != nil {
				return err
			}

			if err := txn.Delete(blkKey); err != nil {
				return err
			}

			toDelete, hash, err := decrementRefcount(txn, objectKey)
			if err != nil {
				return err
			}
			if toDelete {
				hashKey := []byte(hashPrefix + hex.EncodeToString(hash[:]))
				if e := txn.Delete(hashKey); e != nil && !errors.Is(e, badger.ErrKeyNotFound) {
					return e
				}
				if e := txn.Delete([]byte(refPrefix + objectKey)); e != nil && !errors.Is(e, badger.ErrKeyNotFound) {
					return e
				}
				shouldDelete = true
			}
			return nil
		})
	})
	return objectKey, shouldDelete, err
}

func (b *badgerIndex) DeleteVolume(vol string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	prefix := blockBadgerPrefix(vol)

	// Phase 1: collect all block entries for this volume (read-only scan)
	type blkEntry struct {
		blkKey    []byte
		canonical string
	}
	var entries []blkEntry

	if err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := append([]byte{}, item.Key()...)
			var canonical string
			if err := item.Value(func(v []byte) error {
				canonical = string(v)
				return nil
			}); err != nil {
				return err
			}
			entries = append(entries, blkEntry{blkKey: key, canonical: canonical})
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Phase 2: decrement refcounts; collect S3 objects to delete
	var toDelete []string
	for _, e := range entries {
		if err := retry(maxRetries, func() error {
			return b.db.Update(func(txn *badger.Txn) error {
				if err := txn.Delete(e.blkKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				shouldDel, hash, err := decrementRefcount(txn, e.canonical)
				if err != nil {
					return err
				}
				if shouldDel {
					hashKey := []byte(hashPrefix + hex.EncodeToString(hash[:]))
					txn.Delete(hashKey)                         //nolint:errcheck
					txn.Delete([]byte(refPrefix + e.canonical)) //nolint:errcheck
					toDelete = append(toDelete, e.canonical)
				}
				return nil
			})
		}); err != nil {
			return toDelete, err
		}
	}
	return toDelete, nil
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

// decrementRefcount decrements the refcount for objectKey. Returns (shouldDelete=true, hash)
// when refcount reaches zero. Caller is responsible for deleting the hash index and ref entries.
func decrementRefcount(txn *badger.Txn, objectKey string) (shouldDelete bool, hash [32]byte, err error) {
	refKey := []byte(refPrefix + objectKey)
	item, err := txn.Get(refKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, hash, nil // not tracked by dedup
	}
	if err != nil {
		return false, hash, err
	}
	var rc int32
	if err := item.Value(func(v []byte) error {
		if len(v) < 36 {
			return fmt.Errorf("dedup: corrupt ref entry for %q (len %d, want 36)", objectKey, len(v))
		}
		rc, hash = decodeRefVal(v)
		return nil
	}); err != nil {
		return false, hash, err
	}
	if rc <= 1 {
		return true, hash, nil // caller deletes ref + hash entries
	}
	return false, hash, setRefcount(txn, objectKey, rc-1, hash)
}
