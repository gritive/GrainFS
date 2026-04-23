package cluster

// Phase 18 Cluster EC — FSM metadata layer for shard placement.
//
// Key layout: `placement:<bucket>/<key>` → binary-encoded PlacementRecord.
// Format: <uvarint k> <uvarint m> <uvarint count> <uvarint len> <bytes>...
// k and m are always written; 0,0 is invalid (no legacy format compatibility needed).

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// PlacementRecord holds the shard node list and the EC parameters used when
// the object was written. K=0, M=0 means legacy V0 record — callers should
// fall back to the global ecConfig for reconstruction parameters.
type PlacementRecord struct {
	Nodes []string
	K, M  int
}

// ECConfigOrFallback returns an ECConfig from stored K,M, or falls back to
// the provided default when K==0 (legacy V0 placement record).
func (r PlacementRecord) ECConfigOrFallback(def ECConfig) ECConfig {
	if r.K == 0 {
		return def
	}
	return ECConfig{DataShards: r.K, ParityShards: r.M}
}

// applyPutShardPlacement persists the shard placement record to BadgerDB.
func (f *FSM) applyPutShardPlacement(data []byte) error {
	c, err := decodePutShardPlacementCmd(data)
	if err != nil {
		return err
	}
	val := encodePlacementValue(PlacementRecord{Nodes: c.NodeIDs, K: c.K, M: c.M})
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(shardPlacementKey(c.Bucket, c.Key), val)
	})
}

// applyDeleteShardPlacement removes the shard placement record for an object.
func (f *FSM) applyDeleteShardPlacement(data []byte) error {
	c, err := decodeDeleteShardPlacementCmd(data)
	if err != nil {
		return err
	}
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(shardPlacementKey(c.Bucket, c.Key))
	})
}

// ObjectMetaRef is the tuple IterObjectMetas yields for each object.
type ObjectMetaRef struct {
	Bucket string
	Key    string
	Size   int64
	ETag   string
}

// IterObjectMetas iterates every logical object's metadata, invoking fn
// exactly once per (bucket, key). Used by the Phase 18 re-placement manager
// to find N× objects that need conversion to EC.
//
// Versioned keys (`obj:{bucket}/{key}/{versionID}`) cannot be safely parsed by
// splitting on '/' — S3 keys legitimately contain slashes. Iterate the
// `lat:{bucket}/{key}` pointer table instead: each entry yields the exact
// key and the latest versionID, which is the only version that matters for
// re-placement. Delete markers (tombstones) are skipped.
//
// Legacy unversioned `obj:{bucket}/{key}` records that lack a `lat:` pointer
// are caught by a fallback scan of the `obj:` space — for those the key has
// no embedded versionID so first-slash split is unambiguous. We skip any
// `obj:` key whose base has a `lat:` pointer (those come through the lat
// pass already).
//
// fn returning a non-nil error stops iteration.
func (f *FSM) IterObjectMetas(fn func(ObjectMetaRef) error) error {
	return f.db.View(func(txn *badger.Txn) error {
		seen := make(map[string]struct{}) // "bucket\x00key" → visited

		latPrefix := []byte("lat:")
		itLat := txn.NewIterator(badger.DefaultIteratorOptions)
		for itLat.Seek(latPrefix); itLat.ValidForPrefix(latPrefix); itLat.Next() {
			item := itLat.Item()
			rest := string(item.Key()[len(latPrefix):])
			slash := -1
			for i, c := range rest {
				if c == '/' {
					slash = i
					break
				}
			}
			if slash < 0 {
				continue
			}
			bucket := rest[:slash]
			key := rest[slash+1:]

			var versionID string
			if err := item.Value(func(v []byte) error {
				versionID = string(v)
				return nil
			}); err != nil || versionID == "" {
				continue
			}

			metaItem, err := txn.Get(objectMetaKeyV(bucket, key, versionID))
			if err != nil {
				continue
			}
			var ref ObjectMetaRef
			ref.Bucket = bucket
			ref.Key = key
			skip := false
			if verr := metaItem.Value(func(val []byte) error {
				m, derr := unmarshalObjectMeta(val)
				if derr != nil {
					return derr
				}
				if m.ETag == deleteMarkerETag {
					skip = true
					return nil
				}
				ref.Size = m.Size
				ref.ETag = m.ETag
				return nil
			}); verr != nil {
				itLat.Close()
				return verr
			}
			if skip {
				continue
			}
			seen[bucket+"\x00"+key] = struct{}{}
			if err := fn(ref); err != nil {
				itLat.Close()
				return err
			}
		}
		itLat.Close()

		// Fallback: legacy unversioned obj:{bucket}/{key} entries with no
		// lat: pointer. Split on first '/' — these predate versioning and
		// therefore carry no embedded versionID.
		objPrefix := []byte("obj:")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(objPrefix); it.ValidForPrefix(objPrefix); it.Next() {
			item := it.Item()
			trimmed := string(item.Key()[len(objPrefix):])
			slash := -1
			for i, c := range trimmed {
				if c == '/' {
					slash = i
					break
				}
			}
			if slash < 0 {
				continue
			}
			bucket := trimmed[:slash]
			key := trimmed[slash+1:]

			// Skip: handled via lat: pass already, OR this is a versioned
			// sub-entry whose base key was covered above.
			if _, ok := seen[bucket+"\x00"+key]; ok {
				continue
			}
			// Skip versioned sub-entries whose base is in seen. The suffix
			// after the last '/' is a versionID when the base has a lat pointer.
			// TODO(slice-3+): this heuristic over-skips when legacy data mixes a
			// versioned key "a" with an unversioned key "a/b" (prefix collision).
			// Fix by inserting versioned sub-keys into seen during the lat: pass.
			if lastSlash := lastIndexByte(trimmed, '/'); lastSlash > slash {
				base := trimmed[:lastSlash]
				baseKey := base[slash+1:]
				if _, ok := seen[bucket+"\x00"+baseKey]; ok {
					continue
				}
			}

			var ref ObjectMetaRef
			ref.Bucket = bucket
			ref.Key = key
			verr := item.Value(func(val []byte) error {
				m, derr := unmarshalObjectMeta(val)
				if derr != nil {
					return derr
				}
				ref.Size = m.Size
				ref.ETag = m.ETag
				return nil
			})
			if verr != nil {
				return verr
			}
			seen[bucket+"\x00"+key] = struct{}{}
			if err := fn(ref); err != nil {
				return err
			}
		}
		return nil
	})
}

// lastIndexByte mirrors strings.LastIndexByte without pulling the import.
func lastIndexByte(s string, b byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// IterShardPlacements iterates every shard placement record in the FSM, invoking
// fn with the bucket, key, and PlacementRecord for each. Iteration stops if fn
// returns a non-nil error, which is propagated. Used by ShardPlacementMonitor
// to scan for missing shards.
func (f *FSM) IterShardPlacements(fn func(bucket, key string, rec PlacementRecord) error) error {
	return f.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("placement:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			rawKey := string(item.Key())
			trimmed := rawKey[len(prefix):]
			slash := -1
			for i, c := range trimmed {
				if c == '/' {
					slash = i
					break
				}
			}
			if slash < 0 {
				continue
			}
			bucket := trimmed[:slash]
			key := trimmed[slash+1:]
			var rec PlacementRecord
			verr := item.Value(func(val []byte) error {
				decoded, derr := decodePlacementValue(val)
				if derr != nil {
					return derr
				}
				rec = decoded
				return nil
			})
			if verr != nil {
				return verr
			}
			if err := fn(bucket, key, rec); err != nil {
				return err
			}
		}
		return nil
	})
}

// LookupLatestVersion returns the most recent versionID for (bucket, key),
// as written by applyPutObjectMeta to the `lat:` pointer. Used by
// RepairShard to resolve the physical shard path when callers don't have a
// versionID of their own (ShardPlacementMonitor onMissing). Returns an
// error when the pointer is absent; callers treat that as "pre-versioned
// legacy EC" and fall back to the bare-key layout.
func (f *FSM) LookupLatestVersion(bucket, key string) (string, error) {
	var versionID string
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(latestKey(bucket, key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			versionID = string(val)
			return nil
		})
	})
	if err != nil {
		return "", err
	}
	return versionID, nil
}

// LookupShardPlacement returns the PlacementRecord for the given object.
// Returns ({}, nil) with empty Nodes when no placement record exists (N× replication objects).
// Returns ({}, err) on a real BadgerDB read error — callers must not silently
// fall back to N× replication on an error, as that risks data loss.
func (f *FSM) LookupShardPlacement(bucket, key string) (PlacementRecord, error) {
	var rec PlacementRecord
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(shardPlacementKey(bucket, key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			decoded, derr := decodePlacementValue(val)
			if derr != nil {
				return derr
			}
			rec = decoded
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return PlacementRecord{}, nil
		}
		return PlacementRecord{}, err
	}
	return rec, nil
}

// encodePlacementValue serializes a PlacementRecord.
// Format: <uvarint k> <uvarint m> <uvarint count> <uvarint len> <bytes>...
func encodePlacementValue(rec PlacementRecord) []byte {
	var buf bytes.Buffer
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(rec.K))
	buf.Write(tmp[:n])
	n = binary.PutUvarint(tmp[:], uint64(rec.M))
	buf.Write(tmp[:n])
	n = binary.PutUvarint(tmp[:], uint64(len(rec.Nodes)))
	buf.Write(tmp[:n])
	for _, s := range rec.Nodes {
		n = binary.PutUvarint(tmp[:], uint64(len(s)))
		buf.Write(tmp[:n])
		buf.WriteString(s)
	}
	return buf.Bytes()
}

func decodePlacementValue(data []byte) (PlacementRecord, error) {
	if len(data) == 0 {
		return PlacementRecord{}, nil
	}
	r := bytes.NewReader(data)
	k, err := binary.ReadUvarint(r)
	if err != nil {
		return PlacementRecord{}, err
	}
	m, err := binary.ReadUvarint(r)
	if err != nil {
		return PlacementRecord{}, err
	}
	count, err := binary.ReadUvarint(r)
	if err != nil {
		return PlacementRecord{}, err
	}
	nodes := make([]string, 0, count)
	for i := uint64(0); i < count; i++ {
		sl, err := binary.ReadUvarint(r)
		if err != nil {
			return PlacementRecord{}, err
		}
		if sl == 0 {
			nodes = append(nodes, "")
			continue
		}
		buf := make([]byte, sl)
		if _, err := r.Read(buf); err != nil {
			return PlacementRecord{}, err
		}
		nodes = append(nodes, string(buf))
	}
	return PlacementRecord{K: int(k), M: int(m), Nodes: nodes}, nil
}
