package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// keyspaceLeakTotal counts keys that reached a scoped strip-site without the
// expected group prefix. A non-zero value means a prefix-scoping bug — the
// process panics on the offending op (see MustStrip); the counter is the
// metric trail.
var keyspaceLeakTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "grainfs_fsm_keyspace_leak_total",
	Help: "FSM-state keys observed without the expected group prefix at a scoped strip-site (indicates a prefix-scoping bug; the op also panics).",
})

// errStopScan, returned from a scanGroupPrefix callback, halts the scan cleanly
// (scanGroupPrefix returns nil). Use for early-break loops.
var errStopScan = errors.New("cluster: stop keyspace scan")

// stateKeyspace prefixes every FSM-state key with a per-group prefix so that
// many raft groups can share one BadgerDB. An empty prefix (newStateKeyspaceEmpty)
// is a behavior-preserving identity — used by single-group tests and during the
// transition before shared mode is wired.
//
// Prefix encoding: 4-byte big-endian uint32 len(groupID) || groupID. Identical
// to raft.OpenSharedLogStore's encoding (internal/raft/store.go); the fixed-width
// length makes parsing unambiguous and prevents cross-group collisions when
// len(groupID) >= 256 or when one group ID is a byte-prefix of another.
type stateKeyspace struct {
	prefix []byte // empty => identity
}

func newStateKeyspaceEmpty() *stateKeyspace { return &stateKeyspace{} }

func newStateKeyspace(groupID string) (*stateKeyspace, error) {
	if groupID == "" {
		return nil, fmt.Errorf("newStateKeyspace: empty groupID")
	}
	if uint64(len(groupID)) > 0xFFFFFFFF {
		return nil, fmt.Errorf("newStateKeyspace: groupID length %d exceeds uint32 max", len(groupID))
	}
	p := make([]byte, 4+len(groupID))
	binary.BigEndian.PutUint32(p[:4], uint32(len(groupID)))
	copy(p[4:], groupID)
	return &stateKeyspace{prefix: p}, nil
}

// isShared reports whether this keyspace carries a non-empty group prefix.
func (ks *stateKeyspace) isShared() bool { return len(ks.prefix) > 0 }

// Key returns prefix||raw. Use for point Get/Set/Delete.
func (ks *stateKeyspace) Key(raw []byte) []byte {
	if len(ks.prefix) == 0 {
		return raw
	}
	out := make([]byte, 0, len(ks.prefix)+len(raw))
	out = append(out, ks.prefix...)
	out = append(out, raw...)
	return out
}

// Prefix returns prefix||rawPrefix. Use for it.Seek / it.ValidForPrefix /
// badger.IteratorOptions.Prefix. Pass nil rawPrefix to get the bare group prefix
// (e.g. a whole-group scan in Snapshot).
func (ks *stateKeyspace) Prefix(rawPrefix []byte) []byte {
	if len(ks.prefix) == 0 {
		return rawPrefix
	}
	out := make([]byte, 0, len(ks.prefix)+len(rawPrefix))
	out = append(out, ks.prefix...)
	out = append(out, rawPrefix...)
	return out
}

// HasPrefix reports whether fullKey begins with this keyspace's group prefix.
// Always true for the empty keyspace. Used by Restore to reject already-prefixed keys.
func (ks *stateKeyspace) HasPrefix(fullKey []byte) bool {
	if len(ks.prefix) == 0 {
		return true
	}
	return bytes.HasPrefix(fullKey, ks.prefix)
}

// MustStrip removes the group prefix from fullKey. Call only inside an iteration
// whose scope (it.Seek + it.ValidForPrefix on ks.Prefix(...)) already guarantees
// the prefix is present. On a miss it increments grainfs_fsm_keyspace_leak_total
// and panics — a miss here is a prefix-scoping bug; failing loud (node crash,
// crash-loop, metric, log) beats silently serving cross-group data.
func (ks *stateKeyspace) MustStrip(fullKey []byte) []byte {
	if len(ks.prefix) == 0 {
		return fullKey
	}
	if !bytes.HasPrefix(fullKey, ks.prefix) {
		keyspaceLeakTotal.Inc()
		panic(fmt.Sprintf("stateKeyspace: key %q lacks expected group prefix (len=%d) — prefix-scoping bug", fullKey, len(ks.prefix)))
	}
	return fullKey[len(ks.prefix):]
}

// TryStrip removes the group prefix, returning (key, false) when fullKey lacks
// it. Use only at genuine boundaries (decoded snapshot payloads, offline recovery
// reads) where a missing prefix is possible-but-suspect.
func (ks *stateKeyspace) TryStrip(fullKey []byte) ([]byte, bool) {
	if len(ks.prefix) == 0 {
		return fullKey, true
	}
	if !bytes.HasPrefix(fullKey, ks.prefix) {
		return nil, false
	}
	return fullKey[len(ks.prefix):], true
}

// scanGroupPrefix iterates keys under prefix||rawPrefix, calling fn with the
// group-RELATIVE key (group prefix already stripped via MustStrip) and the item.
// fn returning errStopScan stops the scan cleanly (returns nil); any other error
// is propagated. The caller supplies the transaction.
func (ks *stateKeyspace) scanGroupPrefix(txn *badger.Txn, rawPrefix []byte,
	fn func(rawKey []byte, item *badger.Item) error) error {
	full := ks.Prefix(rawPrefix)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = full
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(full); it.ValidForPrefix(full); it.Next() {
		item := it.Item()
		if err := fn(ks.MustStrip(item.Key()), item); err != nil {
			if errors.Is(err, errStopScan) {
				return nil
			}
			return err
		}
	}
	return nil
}

// ---- semantic key-builders (the apply.go free functions become these) ----

func (ks *stateKeyspace) BucketKey(bucket string) []byte  { return ks.Key([]byte("bucket:" + bucket)) }
func (ks *stateKeyspace) BucketPolicyKey(b string) []byte { return ks.Key([]byte("policy:" + b)) }
func (ks *stateKeyspace) BucketVerKey(b string) []byte    { return ks.Key([]byte("bucketver:" + b)) }
func (ks *stateKeyspace) ObjectMetaKey(b, k string) []byte {
	return ks.Key([]byte("obj:" + b + "/" + k))
}
func (ks *stateKeyspace) ObjectMetaKeyV(b, k, ver string) []byte {
	return ks.Key([]byte("obj:" + b + "/" + k + "/" + ver))
}
func (ks *stateKeyspace) LatestKey(b, k string) []byte { return ks.Key([]byte("lat:" + b + "/" + k)) }
func (ks *stateKeyspace) MultipartKey(uploadID string) []byte {
	return ks.Key([]byte("mpu:" + uploadID))
}
func (ks *stateKeyspace) ShardPlacementKey(b, k string) []byte {
	return ks.Key([]byte("placement:" + b + "/" + k))
}
func (ks *stateKeyspace) PendingMigrationKey(b, k, ver string) []byte {
	return ks.Key([]byte("pending-migration:" + b + "/" + k + "/" + ver))
}
func (ks *stateKeyspace) QuarantineKey(bucket, key, versionID string) []byte {
	return ks.Key([]byte("quarantine:" + bucket + "\x00" + key + "\x00" + versionID))
}
