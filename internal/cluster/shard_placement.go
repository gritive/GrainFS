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

// IterObjectMetas iterates every object metadata record, invoking fn for each.
// Used by the Phase 18 re-placement manager to find N× objects that need
// conversion to EC. fn returning a non-nil error stops iteration.
func (f *FSM) IterObjectMetas(fn func(ObjectMetaRef) error) error {
	return f.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("obj:")
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
			if err := fn(ref); err != nil {
				return err
			}
		}
		return nil
	})
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

// LookupShardPlacement returns the list of nodeIDs holding shards for the
// given object, in shardIdx order. Returns ok=false if no placement record
// exists (typical for N× replication objects pre-Phase-18).
func (f *FSM) LookupShardPlacement(bucket, key string) ([]string, bool) {
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
		return nil, false
	}
	return nodes, true
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
