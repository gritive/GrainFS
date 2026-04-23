package cluster

// Phase 18 Cluster EC — Slice 1: FSM metadata layer for shard placement.
//
// This file adds Put/Delete/Lookup for per-object shard placement records to
// the Raft FSM. It does NOT integrate with PutObject/GetObject yet — that is
// Slice 2. Slice 1's only job is to get a durable, Raft-replicated placement
// map in place so later slices can rely on it.
//
// Key layout: `placement:<bucket>/<key>` → FlatBuffers-encoded node list.
// On-disk bytes are the same as the command payload (PutShardPlacementCmd
// body), which re-uses the existing FlatBuffers schema.

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// applyPutShardPlacement persists the shard placement record to BadgerDB.
func (f *FSM) applyPutShardPlacement(data []byte) error {
	c, err := decodePutShardPlacementCmd(data)
	if err != nil {
		return err
	}
	val := encodePlacementValue(c.NodeIDs)
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
// fn with the bucket, key, and ordered nodeIDs for each. Iteration stops if fn
// returns a non-nil error, which is propagated. Used by ShardPlacementMonitor
// to scan for missing shards.
func (f *FSM) IterShardPlacements(fn func(bucket, key string, nodes []string) error) error {
	return f.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("placement:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			rawKey := string(item.Key())
			// Strip "placement:" prefix and split bucket/key on first '/'.
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
			var nodes []string
			verr := item.Value(func(val []byte) error {
				decoded, derr := decodePlacementValue(val)
				if derr != nil {
					return derr
				}
				nodes = decoded
				return nil
			})
			if verr != nil {
				return verr
			}
			if err := fn(bucket, key, nodes); err != nil {
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

// LookupShardPlacement returns the list of nodeIDs holding shards for the
// given object, in shardIdx order.
// Returns (nil, nil) when no placement record exists (N× replication objects).
// Returns (nil, err) on a real BadgerDB read error — callers must not silently
// fall back to N× replication on an error, as that risks data loss.
func (f *FSM) LookupShardPlacement(bucket, key string) ([]string, error) {
	var nodes []string
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
			nodes = decoded
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return nodes, nil
}

// encodePlacementValue serializes a node list as a length-prefixed sequence
// of strings. Format: <uvarint count><uvarint len><bytes>...
// Chosen over FlatBuffers round-trip because the value never leaves the FSM;
// no forward/backward compat constraint, minimum bytes.
func encodePlacementValue(nodes []string) []byte {
	var buf bytes.Buffer
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(nodes)))
	buf.Write(tmp[:n])
	for _, s := range nodes {
		n = binary.PutUvarint(tmp[:], uint64(len(s)))
		buf.Write(tmp[:n])
		buf.WriteString(s)
	}
	return buf.Bytes()
}

func decodePlacementValue(data []byte) ([]string, error) {
	r := bytes.NewReader(data)
	count, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, count)
	for i := uint64(0); i < count; i++ {
		sl, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		if sl == 0 {
			out = append(out, "")
			continue
		}
		buf := make([]byte, sl)
		if _, err := r.Read(buf); err != nil {
			return nil, err
		}
		out = append(out, string(buf))
	}
	return out, nil
}
