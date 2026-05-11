package dedup

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	snapBlockPrefix   = "vd:s:"  // vd:s:{vol}:{snapID}:{blkNum:012d} → canonicalKey
	snapStatePrefix   = "vd:ss:" // vd:ss:{vol}:{snapID}              → byte(SnapshotState)
	snapMetaPrefix    = "vd:sm:" // vd:sm:{vol}:{snapID}              → binary SnapshotMeta
	rollbackStatePref = "vd:rb:" // vd:rb:{vol}:{snapID}              → byte (1=in-progress)
	clonePrefix       = "vd:cl:" // vd:cl:{dstVol}                    → byte (1=in-progress)
)

func snapBlockKey(vol, snapID string, blkNum int64) []byte {
	return []byte(fmt.Sprintf("%s%s:%s:%012d", snapBlockPrefix, vol, snapID, blkNum))
}

func snapBlockPrefixKey(vol, snapID string) []byte {
	return []byte(snapBlockPrefix + vol + ":" + snapID + ":")
}

func snapStateKey(vol, snapID string) []byte {
	return []byte(snapStatePrefix + vol + ":" + snapID)
}

func snapStatePrefixKey() []byte { return []byte(snapStatePrefix) }

func snapMetaKey(vol, snapID string) []byte {
	return []byte(snapMetaPrefix + vol + ":" + snapID)
}

func snapMetaPrefixKey(vol string) []byte {
	return []byte(snapMetaPrefix + vol + ":")
}

func rollbackStateKey(vol, snapID string) []byte {
	return []byte(rollbackStatePref + vol + ":" + snapID)
}

func rollbackStatePrefixKey() []byte { return []byte(rollbackStatePref) }

func cloneStateKey(dstVol string) []byte {
	return []byte(clonePrefix + dstVol)
}

func cloneStatePrefixKey() []byte { return []byte(clonePrefix) }

func errNotImpl(name string) error { return fmt.Errorf("dedup: %s not implemented", name) }

// snapState reads the SnapshotState for (vol, snapID) within the given txn.
// ok=false when no marker exists.
func (b *badgerIndex) snapState(txn *badger.Txn, vol, snapID string) (st SnapshotState, ok bool, err error) {
	item, err := txn.Get(snapStateKey(vol, snapID))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	err = item.Value(func(v []byte) error {
		if len(v) != 1 {
			return fmt.Errorf("dedup: corrupt snap state for %s/%s", vol, snapID)
		}
		st = SnapshotState(v[0])
		return nil
	})
	return st, true, err
}

func (b *badgerIndex) setSnapState(txn *badger.Txn, vol, snapID string, st SnapshotState) error {
	return txn.Set(snapStateKey(vol, snapID), []byte{byte(st)})
}

// encodeSnapMeta writes (CreatedAt UnixNano int64 BE, BlockCount int64 BE).
// Binary layout, no JSON (per feedback_no_internal_json.md).
func encodeSnapMeta(m SnapshotMeta) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], uint64(m.CreatedAt.UnixNano()))
	binary.BigEndian.PutUint64(buf[8:16], uint64(m.BlockCount))
	return buf
}

func decodeSnapMeta(snapID string, val []byte) (SnapshotMeta, error) {
	if len(val) != 16 {
		return SnapshotMeta{}, fmt.Errorf("dedup: corrupt snap meta for %s (len %d, want 16)", snapID, len(val))
	}
	ns := int64(binary.BigEndian.Uint64(val[:8]))
	bc := int64(binary.BigEndian.Uint64(val[8:16]))
	return SnapshotMeta{
		SnapID:     snapID,
		CreatedAt:  time.Unix(0, ns).UTC(),
		BlockCount: bc,
	}, nil
}

func (b *badgerIndex) setSnapMeta(txn *badger.Txn, vol string, m SnapshotMeta) error {
	return txn.Set(snapMetaKey(vol, m.SnapID), encodeSnapMeta(m))
}

// Stub implementations — real logic comes in subsequent tasks (B3-B7).

func (b *badgerIndex) SnapshotBegin(vol, snapID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return retry(maxRetries, func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			st, ok, err := b.snapState(txn, vol, snapID)
			if err != nil {
				return err
			}
			if ok {
				if st == SnapshotBegun || st == SnapshotCommitted {
					return nil // idempotent
				}
				return fmt.Errorf("dedup: snapshot %s/%s in unexpected state %d", vol, snapID, st)
			}
			return b.setSnapState(txn, vol, snapID, SnapshotBegun)
		})
	})
}

func (b *badgerIndex) SnapshotAppendChunk(vol, snapID string, entries []SnapshotBlockEntry) error {
	if len(entries) == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return retry(maxRetries, func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			st, ok, err := b.snapState(txn, vol, snapID)
			if err != nil {
				return err
			}
			if !ok || st != SnapshotBegun {
				return fmt.Errorf("dedup: snapshot %s/%s not in Begun state", vol, snapID)
			}
			for _, e := range entries {
				k := snapBlockKey(vol, snapID, e.BlkNum)
				if _, err := txn.Get(k); err == nil {
					// Already inserted (idempotent retry); skip without re-IncRef.
					continue
				} else if !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				if err := txn.Set(k, []byte(e.Canonical)); err != nil {
					return err
				}
				if err := incrRefcount(txn, e.Canonical); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

// SnapshotCommit transitions Begun → Committed and writes vd:sm: meta in
// the same Badger txn (atomic, no Badger/S3 split-brain per R3).
func (b *badgerIndex) SnapshotCommit(vol, snapID string, meta SnapshotMeta) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return retry(maxRetries, func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			st, ok, err := b.snapState(txn, vol, snapID)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("dedup: snapshot %s/%s not begun", vol, snapID)
			}
			if st == SnapshotCommitted {
				return nil // idempotent
			}
			if st != SnapshotBegun {
				return fmt.Errorf("dedup: snapshot %s/%s in state %d (expected Begun)", vol, snapID, st)
			}
			if err := b.setSnapState(txn, vol, snapID, SnapshotCommitted); err != nil {
				return err
			}
			if meta.SnapID == "" {
				meta.SnapID = snapID
			} else if meta.SnapID != snapID {
				return fmt.Errorf("dedup: SnapshotCommit snapID %q != meta.SnapID %q", snapID, meta.SnapID)
			}
			return b.setSnapMeta(txn, vol, meta)
		})
	})
}

// teardownSnapshot is the common path for Abort (Begun) and Delete (Committed).
// Walks vd:s:{vol}:{snapID}: entries chunked, DecRefs each canonical, returns S3 keys
// that hit refcount zero. Removes vd:ss: state entry on success; also removes
// vd:sm: meta entry if it exists (only set when Committed).
func (b *badgerIndex) teardownSnapshot(vol, snapID string, expectStates ...SnapshotState) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Verify state under a read txn.
	if err := b.db.View(func(txn *badger.Txn) error {
		st, ok, err := b.snapState(txn, vol, snapID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("dedup: snapshot %s/%s not found", vol, snapID)
		}
		for _, want := range expectStates {
			if st == want {
				return nil
			}
		}
		return fmt.Errorf("dedup: snapshot %s/%s state %d not in %v", vol, snapID, st, expectStates)
	}); err != nil {
		return nil, err
	}

	prefix := snapBlockPrefixKey(vol, snapID)
	var toDelete []string
	const chunk = 512

	for {
		batch := make([][]byte, 0, chunk)
		batchCanon := make([]string, 0, chunk)
		if err := b.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid() && len(batch) < chunk; it.Next() {
				item := it.Item()
				key := append([]byte{}, item.Key()...)
				var canon string
				if err := item.Value(func(v []byte) error { canon = string(v); return nil }); err != nil {
					return err
				}
				batch = append(batch, key)
				batchCanon = append(batchCanon, canon)
			}
			return nil
		}); err != nil {
			return toDelete, err
		}
		if len(batch) == 0 {
			break
		}

		if err := retry(maxRetries, func() error {
			return b.db.Update(func(txn *badger.Txn) error {
				for i, k := range batch {
					if err := txn.Delete(k); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
						return err
					}
					shouldDel, hash, err := decrementRefcount(txn, batchCanon[i])
					if err != nil {
						return err
					}
					if shouldDel {
						hashKey := []byte(hashPrefix + hex.EncodeToString(hash[:]))
						_ = txn.Delete(hashKey)
						_ = txn.Delete([]byte(refPrefix + batchCanon[i]))
						toDelete = append(toDelete, batchCanon[i])
					}
				}
				return nil
			})
		}); err != nil {
			return toDelete, err
		}
	}

	// Remove state + meta entries. Meta key may not exist (Abort case) — ignore NotFound.
	if err := b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(snapStateKey(vol, snapID)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if err := txn.Delete(snapMetaKey(vol, snapID)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	}); err != nil {
		return toDelete, err
	}
	return toDelete, nil
}

func (b *badgerIndex) SnapshotAbort(vol, snapID string) ([]string, error) {
	return b.teardownSnapshot(vol, snapID, SnapshotBegun)
}

func (b *badgerIndex) SnapshotIter(vol, snapID string, fn func(blkNum int64, canonical string) error) error {
	prefix := snapBlockPrefixKey(vol, snapID)
	return b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			suffix := key[len(prefix):]
			if len(suffix) < 12 {
				continue
			}
			blkNum, err := strconv.ParseInt(string(suffix[:12]), 10, 64)
			if err != nil {
				return err
			}
			var canon string
			if err := item.Value(func(v []byte) error { canon = string(v); return nil }); err != nil {
				return err
			}
			if err := fn(blkNum, canon); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *badgerIndex) SnapshotReadBlock(vol, snapID string, blkNum int64) (string, bool, error) {
	k := snapBlockKey(vol, snapID, blkNum)
	var canon string
	found := false
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return item.Value(func(v []byte) error { canon = string(v); return nil })
	})
	return canon, found, err
}

func (b *badgerIndex) SnapshotDelete(vol, snapID string) ([]string, error) {
	return b.teardownSnapshot(vol, snapID, SnapshotCommitted)
}

func (b *badgerIndex) SnapshotListInProgress() ([]struct{ Vol, SnapID string }, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []struct{ Vol, SnapID string }
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = snapStatePrefixKey()
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			var st SnapshotState
			if err := item.Value(func(v []byte) error {
				if len(v) != 1 {
					return fmt.Errorf("dedup: bad state len %d", len(v))
				}
				st = SnapshotState(v[0])
				return nil
			}); err != nil {
				return err
			}
			if st != SnapshotBegun {
				continue
			}
			// key = vd:ss:{vol}:{snapID}
			rest := key[len(snapStatePrefix):]
			parts := strings.SplitN(rest, ":", 2)
			if len(parts) != 2 {
				continue
			}
			out = append(out, struct{ Vol, SnapID string }{parts[0], parts[1]})
		}
		return nil
	})
	return out, err
}

func (b *badgerIndex) SnapshotListPendingRollbacks() ([]struct{ Vol, SnapID string }, error) {
	return nil, errNotImpl("SnapshotListPendingRollbacks")
}

func (b *badgerIndex) SnapshotListPendingClones() ([]string, error) {
	return nil, errNotImpl("SnapshotListPendingClones")
}

func (b *badgerIndex) SnapshotRollback(vol, snapID string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 1. Verify snap is committed.
	if err := b.db.View(func(txn *badger.Txn) error {
		st, ok, err := b.snapState(txn, vol, snapID)
		if err != nil {
			return err
		}
		if !ok || st != SnapshotCommitted {
			return fmt.Errorf("dedup: snapshot %s/%s not committed", vol, snapID)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// 2. Mark rollback in-progress.
	rbKey := rollbackStateKey(vol, snapID)
	if err := b.db.Update(func(txn *badger.Txn) error { return txn.Set(rbKey, []byte{1}) }); err != nil {
		return nil, err
	}

	var toDelete []string
	const chunk = 256

	// 3. Collect snap entries (snap is immutable while committed).
	type pair struct {
		blkNum int64
		canon  string
	}
	var snapEntries []pair
	snapByBlk := make(map[int64]string)
	if err := b.SnapshotIter(vol, snapID, func(blk int64, c string) error {
		snapEntries = append(snapEntries, pair{blk, c})
		snapByBlk[blk] = c
		return nil
	}); err != nil {
		return nil, err
	}

	// 4. Walk live (vd:b:{vol}:*) entries; collect blkNums that need delete (not in snap).
	livePrefix := blockBadgerPrefix(vol)
	var liveOnly []pair
	if err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = livePrefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			suffix := key[len(livePrefix):]
			if len(suffix) < 12 {
				continue
			}
			blk, err := strconv.ParseInt(string(suffix[:12]), 10, 64)
			if err != nil {
				return err
			}
			if _, inSnap := snapByBlk[blk]; inSnap {
				continue
			}
			var canon string
			if err := item.Value(func(v []byte) error { canon = string(v); return nil }); err != nil {
				return err
			}
			liveOnly = append(liveOnly, pair{blk, canon})
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// 5. Apply snap entries to live, chunked.
	for i := 0; i < len(snapEntries); i += chunk {
		end := i + chunk
		if end > len(snapEntries) {
			end = len(snapEntries)
		}
		batch := snapEntries[i:end]
		if err := retry(maxRetries, func() error {
			return b.db.Update(func(txn *badger.Txn) error {
				for _, e := range batch {
					blkKey := blockBadgerKey(vol, e.blkNum)
					var prevCanon string
					if item, err := txn.Get(blkKey); err == nil {
						_ = item.Value(func(v []byte) error { prevCanon = string(v); return nil })
					}
					if prevCanon == e.canon {
						continue
					}
					if err := incrRefcount(txn, e.canon); err != nil {
						return err
					}
					if prevCanon != "" {
						shouldDel, hash, err := decrementRefcount(txn, prevCanon)
						if err != nil {
							return err
						}
						if shouldDel {
							_ = txn.Delete([]byte(hashPrefix + hex.EncodeToString(hash[:])))
							_ = txn.Delete([]byte(refPrefix + prevCanon))
							toDelete = append(toDelete, prevCanon)
						}
					}
					if err := txn.Set(blkKey, []byte(e.canon)); err != nil {
						return err
					}
				}
				return nil
			})
		}); err != nil {
			return toDelete, err
		}
	}

	// 6. Remove live-only blocks, chunked.
	for i := 0; i < len(liveOnly); i += chunk {
		end := i + chunk
		if end > len(liveOnly) {
			end = len(liveOnly)
		}
		batch := liveOnly[i:end]
		if err := retry(maxRetries, func() error {
			return b.db.Update(func(txn *badger.Txn) error {
				for _, e := range batch {
					if err := txn.Delete(blockBadgerKey(vol, e.blkNum)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
						return err
					}
					shouldDel, hash, err := decrementRefcount(txn, e.canon)
					if err != nil {
						return err
					}
					if shouldDel {
						_ = txn.Delete([]byte(hashPrefix + hex.EncodeToString(hash[:])))
						_ = txn.Delete([]byte(refPrefix + e.canon))
						toDelete = append(toDelete, e.canon)
					}
				}
				return nil
			})
		}); err != nil {
			return toDelete, err
		}
	}

	// 7. Clear rollback marker.
	if err := b.db.Update(func(txn *badger.Txn) error { return txn.Delete(rbKey) }); err != nil {
		return toDelete, err
	}
	return toDelete, nil
}

func (b *badgerIndex) SnapshotClone(srcVol, dstVol string) error {
	return errNotImpl("SnapshotClone")
}

// IterLiveBlocks walks vd:b:{vol}:* under a single Badger MVCC view (R1).
// Used by badgerSnapshotStore.CreateSnapshot to enumerate live blocks
// without scanning vol.Size/vol.BlockSize linearly.
func (b *badgerIndex) IterLiveBlocks(vol string, fn func(blkNum int64, canonical string) error) error {
	prefix := blockBadgerPrefix(vol)
	return b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			suffix := key[len(prefix):]
			if len(suffix) < 12 {
				continue
			}
			blkNum, err := strconv.ParseInt(string(suffix[:12]), 10, 64)
			if err != nil {
				return err
			}
			var canon string
			if err := item.Value(func(v []byte) error { canon = string(v); return nil }); err != nil {
				return err
			}
			if err := fn(blkNum, canon); err != nil {
				return err
			}
		}
		return nil
	})
}

// SnapshotPutMeta writes vd:sm:{vol}:{meta.SnapID} = binary-encoded meta.
// Called independently of SnapshotCommit when meta needs to be updated
// (e.g., post-commit refinement). Returns error if meta.SnapID is empty.
func (b *badgerIndex) SnapshotPutMeta(vol string, meta SnapshotMeta) error {
	if meta.SnapID == "" {
		return fmt.Errorf("dedup: SnapshotPutMeta requires non-empty SnapID")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return retry(maxRetries, func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			return b.setSnapMeta(txn, vol, meta)
		})
	})
}

// SnapshotList returns all committed snapshots for vol, ordered by CreatedAt asc.
func (b *badgerIndex) SnapshotList(vol string) ([]SnapshotMeta, error) {
	prefix := snapMetaPrefixKey(vol)
	var out []SnapshotMeta
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			snapID := key[len(prefix):]
			var val []byte
			if err := item.Value(func(v []byte) error {
				val = append([]byte{}, v...)
				return nil
			}); err != nil {
				return err
			}
			m, err := decodeSnapMeta(snapID, val)
			if err != nil {
				return err
			}
			out = append(out, m)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}
