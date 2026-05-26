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
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
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

// ObjectMetaRef is the tuple IterObjectMetas yields for each object.
type ObjectMetaRef struct {
	Bucket    string
	Key       string
	VersionID string
	Size      int64
	ETag      string
	ECData    uint8
	ECParity  uint8
	NodeIDs   []string
	// PlacementGroupID is the data raft group that owns this object version.
	PlacementGroupID string
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
	return f.iterLatestObjectMetas(func(ref ObjectMetaRef, _ objectMeta) error {
		return fn(ref)
	})
}

// iterLatestObjectMetas runs both the lat: pass and the legacy obj: fallback,
// reads+decrypts+unmarshals each meta once, applies the delete-marker skip and
// seen-dedup, and invokes fn with the built ObjectMetaRef AND the full
// objectMeta. It is the shared core behind IterObjectMetas and
// IterECShardScanTargets; behavior must stay identical to the public contract
// documented on IterObjectMetas.
func (f *FSM) iterLatestObjectMetas(fn func(ref ObjectMetaRef, m objectMeta) error) error {
	return f.db.View(func(txn *badger.Txn) error {
		seen := make(map[string]struct{}) // "bucket\x00key" → visited

		rawLatPrefix := []byte("lat:")
		if serr := f.keys.scanGroupPrefix(txn, rawLatPrefix, func(raw []byte, item *badger.Item) error {
			rest := string(raw[len(rawLatPrefix):])
			slash := -1
			for i, c := range rest {
				if c == '/' {
					slash = i
					break
				}
			}
			if slash < 0 {
				return nil
			}
			bucket := rest[:slash]
			key := rest[slash+1:]

			var versionID string
			if err := item.Value(func(v []byte) error {
				versionID = string(v)
				return nil
			}); err != nil || versionID == "" {
				return nil
			}

			metaItem, err := txn.Get(f.keys.ObjectMetaKeyV(bucket, key, versionID))
			if err != nil {
				return nil
			}
			var ref ObjectMetaRef
			ref.Bucket = bucket
			ref.Key = key
			ref.VersionID = versionID
			skip := false
			val, verr := f.itemValueCopy(metaItem)
			if verr != nil {
				return verr
			}
			m, verr := unmarshalObjectMeta(val)
			if verr != nil {
				return verr
			}
			if m.ETag == deleteMarkerETag {
				skip = true
			}
			ref.Size = m.Size
			ref.ETag = m.ETag
			ref.ECData = m.ECData
			ref.ECParity = m.ECParity
			ref.NodeIDs = m.NodeIDs
			ref.PlacementGroupID = m.PlacementGroupID
			if skip {
				return nil
			}
			seen[bucket+"\x00"+key] = struct{}{}
			return fn(ref, m)
		}); serr != nil {
			return serr
		}

		// Fallback: legacy unversioned obj:{bucket}/{key} entries with no
		// lat: pointer. Split on first '/' — these predate versioning and
		// therefore carry no embedded versionID.
		rawObjPrefix := []byte("obj:")
		return f.keys.scanGroupPrefix(txn, rawObjPrefix, func(raw []byte, item *badger.Item) error {
			trimmed := string(raw[len(rawObjPrefix):])
			slash := -1
			for i, c := range trimmed {
				if c == '/' {
					slash = i
					break
				}
			}
			if slash < 0 {
				return nil
			}
			bucket := trimmed[:slash]
			key := trimmed[slash+1:]

			// Skip: handled via lat: pass already, OR this is a versioned
			// sub-entry whose base key was covered above.
			if _, ok := seen[bucket+"\x00"+key]; ok {
				return nil
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
					return nil
				}
			}

			var ref ObjectMetaRef
			ref.Bucket = bucket
			ref.Key = key
			val, verr := f.itemValueCopy(item)
			if verr != nil {
				return verr
			}
			m, verr := unmarshalObjectMeta(val)
			if verr != nil {
				return verr
			}
			ref.Size = m.Size
			ref.ETag = m.ETag
			ref.ECData = m.ECData
			ref.ECParity = m.ECParity
			ref.NodeIDs = m.NodeIDs
			ref.PlacementGroupID = m.PlacementGroupID
			seen[bucket+"\x00"+key] = struct{}{}
			return fn(ref, m)
		})
	})
}

// ECShardKind distinguishes the provenance of an EC shard scan target. It is
// set once at enumeration time and must never be re-derived from ShardKey
// downstream.
type ECShardKind int

const (
	// ECShardObjectVersion is a plain single-blob object-version shard whose
	// placement lives on the object meta's top-level EC fields.
	ECShardObjectVersion ECShardKind = iota
	// ECShardSegment is a chunked/segment shard (`<key>/segments/<blobID>`)
	// whose placement lives in objectMeta.Segments[].
	ECShardSegment
	// ECShardCoalesced is a coalesced shard (`<key>/coalesced/<id>`) whose
	// placement lives in objectMeta.Coalesced[].
	ECShardCoalesced
)

// ECShardScanTarget is one EC shard the placement monitor must verify. Object-
// version targets carry the raw object EC fields (ECData/ECParity/NodeIDs/
// PlacementGroupID) and leave ShardKey/Placement empty — the monitor resolves
// the placement via ResolvePlacement. Segment/coalesced targets carry a fully
// resolved ShardKey + Placement and leave the raw object fields zero.
type ECShardScanTarget struct {
	Kind             ECShardKind
	Bucket           string
	ObjectKey        string
	VersionID        string
	ShardKey         string
	ECData, ECParity uint8
	NodeIDs          []string
	PlacementGroupID string
	Placement        PlacementRecord
}

// IterECShardScanTargets emits one target per EC shard to verify, covering
// object-version, segment, and coalesced shards. Provenance (Kind) is set here
// and must never be re-derived from ShardKey downstream.
//
// Per object meta:
//   - An object-version target is emitted ONLY when the object has neither
//     segments nor coalesced shards (a real key/versionID object-version shard
//     exists on disk). For chunked/coalesced objects the top-level EC fields are
//     just a segment-0 mirror with no on-disk object-version shard, so no
//     object-version target is emitted. No EC validation happens here — the
//     monitor's ResolvePlacement owns that and skips non-EC objects.
//   - Each EC segment ref yields an ECShardSegment target; each EC coalesced
//     ref yields an ECShardCoalesced target. A ref with ECData==0 (owner-local)
//     is silently skipped; a ref whose NodeIDs length != ECData+ECParity is
//     malformed and is skipped with a warning + metric.
//
// fn returning a non-nil error stops iteration.
func (f *FSM) IterECShardScanTargets(fn func(ECShardScanTarget) error) error {
	return f.iterLatestObjectMetas(func(ref ObjectMetaRef, m objectMeta) error {
		if len(m.Segments) == 0 && len(m.Coalesced) == 0 {
			return fn(ECShardScanTarget{
				Kind:             ECShardObjectVersion,
				Bucket:           ref.Bucket,
				ObjectKey:        ref.Key,
				VersionID:        ref.VersionID,
				ECData:           m.ECData,
				ECParity:         m.ECParity,
				NodeIDs:          m.NodeIDs,
				PlacementGroupID: m.PlacementGroupID,
			})
		}

		for i := range m.Segments {
			seg := m.Segments[i]
			if seg.ECData == 0 {
				continue // owner-local segment: no EC shard to verify
			}
			if len(seg.NodeIDs) != int(seg.ECData)+int(seg.ECParity) {
				log.Warn().
					Str("component", "placement-monitor").
					Str("bucket", ref.Bucket).
					Str("key", ref.Key).
					Str("blob_id", seg.BlobID).
					Int("ec_data", int(seg.ECData)).
					Int("ec_parity", int(seg.ECParity)).
					Int("node_ids", len(seg.NodeIDs)).
					Msg("placement-monitor: skipping malformed segment EC ref (NodeIDs length mismatch)")
				metrics.PlacementMonitorInvalidECRef.WithLabelValues("segment").Inc()
				continue
			}
			if err := fn(ECShardScanTarget{
				Kind:      ECShardSegment,
				Bucket:    ref.Bucket,
				ObjectKey: ref.Key,
				VersionID: ref.VersionID,
				ShardKey:  ref.Key + "/segments/" + seg.BlobID,
				Placement: PlacementRecord{
					Nodes: seg.NodeIDs,
					K:     int(seg.ECData),
					M:     int(seg.ECParity),
				},
			}); err != nil {
				return err
			}
		}

		for i := range m.Coalesced {
			cs := m.Coalesced[i]
			if cs.ECData == 0 {
				continue // owner-local coalesced blob: no EC shard to verify
			}
			if len(cs.NodeIDs) != int(cs.ECData)+int(cs.ECParity) {
				log.Warn().
					Str("component", "placement-monitor").
					Str("bucket", ref.Bucket).
					Str("key", ref.Key).
					Str("coalesced_id", cs.CoalescedID).
					Int("ec_data", int(cs.ECData)).
					Int("ec_parity", int(cs.ECParity)).
					Int("node_ids", len(cs.NodeIDs)).
					Msg("placement-monitor: skipping malformed coalesced EC ref (NodeIDs length mismatch)")
				metrics.PlacementMonitorInvalidECRef.WithLabelValues("coalesced").Inc()
				continue
			}
			// Asymmetry: the coalesced ShardKey is authoritative (the
			// pre-populated CoalescedShardRef.ShardKey), whereas the segment
			// ShardKey above is derived (key+"/segments/"+blobID). Do not
			// "normalize" the two — coalesced refs carry their own key.
			if err := fn(ECShardScanTarget{
				Kind:      ECShardCoalesced,
				Bucket:    ref.Bucket,
				ObjectKey: ref.Key,
				VersionID: ref.VersionID,
				ShardKey:  cs.ShardKey,
				Placement: PlacementRecord{
					Nodes: cs.NodeIDs,
					K:     int(cs.ECData),
					M:     int(cs.ECParity),
				},
			}); err != nil {
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
		rawPrefix := []byte("placement:")
		return f.keys.scanGroupPrefix(txn, rawPrefix, func(raw []byte, item *badger.Item) error {
			trimmed := string(raw[len(rawPrefix):])
			slash := -1
			for i, c := range trimmed {
				if c == '/' {
					slash = i
					break
				}
			}
			if slash < 0 {
				return nil
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
			return fn(bucket, key, rec)
		})
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
		item, err := txn.Get(f.keys.LatestKey(bucket, key))
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
		item, err := txn.Get(f.keys.ShardPlacementKey(bucket, key))
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
//
//nolint:unused // package tests seed placement metadata directly.
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

// LookupObjectPlacement returns the EC placement stored in exact object
// metadata. versionID="" reads the unversioned bare key (kept in sync with the
// latest version by dual-write); a non-empty versionID reads the exact
// versioned metadata key. Returns an empty PlacementRecord when the object is
// missing, is not EC-managed, or has incomplete EC metadata (ECData==0 or no
// NodeIDs) — callers treat an empty record as "no actionable placement".
func (f *FSM) LookupObjectPlacement(bucket, key, versionID string) (PlacementRecord, error) {
	var rec PlacementRecord
	err := f.db.View(func(txn *badger.Txn) error {
		dbKey := f.keys.ObjectMetaKey(bucket, key)
		if versionID != "" {
			dbKey = f.keys.ObjectMetaKeyV(bucket, key, versionID)
		}
		item, err := txn.Get(dbKey)
		if err != nil {
			return err
		}
		val, err := f.itemValueCopy(item)
		if err != nil {
			return err
		}
		meta, derr := unmarshalObjectMeta(val)
		if derr != nil {
			return derr
		}
		if meta.ECData == 0 || len(meta.NodeIDs) == 0 {
			return nil
		}
		rec = PlacementRecord{
			K:     int(meta.ECData),
			M:     int(meta.ECParity),
			Nodes: append([]string(nil), meta.NodeIDs...),
		}
		return nil
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return PlacementRecord{}, nil
	}
	return rec, err
}

// LookupObjectECShards returns the EC shard parameters (k, m) stored in ObjectMeta
// for the given versioned object. Returns (0, 0, nil) when the object has no EC
// metadata (N× replication mode). Returns (0, 0, err) on a real BadgerDB read error.
func (f *FSM) LookupObjectECShards(bucket, key, versionID string) (k, m int, err error) {
	err = f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(f.keys.ObjectMetaKeyV(bucket, key, versionID))
		if err != nil {
			return err
		}
		val, err := f.itemValueCopy(item)
		if err != nil {
			return err
		}
		meta, derr := unmarshalObjectMeta(val)
		if derr != nil {
			return derr
		}
		k = int(meta.ECData)
		m = int(meta.ECParity)
		return nil
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, 0, nil // N× 모드: EC 메타 없음
	}
	return k, m, err
}
