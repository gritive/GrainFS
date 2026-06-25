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

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
)

// PlacementRecord holds the shard node list and the EC parameters used when
// the object was written. K=0, M=0 means legacy V0 record — callers should
// fall back to the global ecConfig for reconstruction parameters.
type PlacementRecord struct {
	Nodes       []string
	K, M        int
	StripeBytes int
}

// ECConfigOrFallback returns an ECConfig from stored K,M, or falls back to
// the provided default when K==0 (legacy V0 placement record).
func (r PlacementRecord) ECConfigOrFallback(def ECConfig) ECConfig {
	if r.K == 0 {
		return def
	}
	return ECConfig{DataShards: r.K, ParityShards: r.M}
}

// ObjectMetaRef identifies one object version for EC shard target construction
// (buildECShardTargets). Callers build it from a decoded quorum-meta blob.
type ObjectMetaRef struct {
	Bucket      string
	Key         string
	VersionID   string
	Size        int64
	ETag        string
	ECData      uint8
	ECParity    uint8
	StripeBytes uint32
	NodeIDs     []string
	// PlacementGroupID is the data raft group that owns this object version.
	PlacementGroupID string
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

// buildECShardTargets emits ECShardScanTarget entries for one object meta ref.
// Provenance (Kind) is set here and must never be re-derived from ShardKey
// downstream. Callers feed it a quorum-meta-blob-decoded objectMeta:
//   - An object-version target is emitted ONLY when the object has neither
//     segments nor coalesced shards (a real key/versionID object-version shard
//     exists on disk). For chunked/coalesced objects the top-level EC fields are
//     just a segment-0 mirror with no on-disk object-version shard, so no
//     object-version target is emitted. Object-version targets are not
//     EC-validated here — ResolvePlacement owns that and skips non-EC objects.
//   - Each segment/coalesced ref IS validated: a ref with ECData==0 (owner-local)
//     is silently skipped; a ref whose len(NodeIDs) != ECData+ECParity is
//     malformed and is skipped with a warning + metric.
//
// fn returning a non-nil error stops iteration.
func (f *FSM) buildECShardTargets(ref ObjectMetaRef, m objectMeta, fn func(ECShardScanTarget) error) error {
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

	// Segment/coalesced targets are emitted ONLY for versioned refs. The lat:
	// pass always sets VersionID; the legacy obj: fallback leaves it empty.
	// Modern chunked/coalesced objects are always versioned (the writer mints a
	// UUIDv7 version), so a VersionID=="" ref is either an ancient unversioned
	// object (no EC segments) or a delete-marker legacy-fallback misparse —
	// neither must yield a segment/coalesced target (a misparse would emit a
	// WRONG shard key like "key/v1/segments/<blob>" and trigger a false-missing
	// repair every scan). Object-version targets are still emitted above for
	// VersionID=="" legacy refs (that coverage is intended).
	if ref.VersionID == "" {
		return nil
	}

	for i := range m.Segments {
		seg := m.Segments[i]
		if !validateECRefPlacement(seg.ECData, seg.ECParity, seg.NodeIDs) {
			if seg.ECData != 0 {
				// malformed: ECData>0 but NodeIDs length mismatches
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
			}
			continue // owner-local (ECData==0) or malformed
		}
		if err := fn(ECShardScanTarget{
			Kind:      ECShardSegment,
			Bucket:    ref.Bucket,
			ObjectKey: ref.Key,
			VersionID: ref.VersionID,
			ShardKey:  ref.Key + "/segments/" + seg.BlobID,
			Placement: PlacementRecord{
				Nodes:       seg.NodeIDs,
				K:           int(seg.ECData),
				M:           int(seg.ECParity),
				StripeBytes: int(seg.StripeBytes),
			},
		}); err != nil {
			return err
		}
	}

	for i := range m.Coalesced {
		cs := m.Coalesced[i]
		if !validateECRefPlacement(cs.ECData, cs.ECParity, cs.NodeIDs) {
			if cs.ECData != 0 {
				// malformed: ECData>0 but NodeIDs length mismatches
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
			}
			continue // owner-local (ECData==0) or malformed
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
				Nodes:       cs.NodeIDs,
				K:           int(cs.ECData),
				M:           int(cs.ECParity),
				StripeBytes: int(cs.StripeBytes),
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

// validateECRefPlacement reports whether an EC ref describes a distributedly-
// striped blob with a well-formed node list. It returns false (and callers must
// skip) for two cases:
//   - ECData==0: owner-local blob, no EC shard to verify (silent skip).
//   - len(nodeIDs) != int(ecData)+int(ecParity): malformed ref (caller logs +
//     increments the metric before skipping).
func validateECRefPlacement(ecData, ecParity uint8, nodeIDs []string) bool {
	if ecData == 0 {
		return false
	}
	return len(nodeIDs) == int(ecData)+int(ecParity)
}

// IterShardPlacements iterates every shard placement record in the FSM, invoking
// fn with the bucket, key, and PlacementRecord for each. Iteration stops if fn
// returns a non-nil error, which is propagated. Used by ShardPlacementMonitor
// to scan for missing shards.
func (f *FSM) IterShardPlacements(fn func(bucket, key string, rec PlacementRecord) error) error {
	return f.db.View(func(txn MetadataTxn) error {
		rawPrefix := []byte("placement:")
		return f.keys.scanGroupPrefix(txn, rawPrefix, func(raw []byte, item MetaItem) error {
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

// LookupLatestVersion returns the most recent versionID for (bucket, key)
// from the legacy `lat:` FSM pointer. That pointer was written by the
// now-retired per-object meta raft command (data-plane raft-free Slice 2);
// the off-raft data plane no longer writes obj:/lat: records, so for
// blob-authoritative objects the pointer is absent. Used by RepairShard to
// resolve the physical shard path when callers don't have a versionID of
// their own (ShardPlacementMonitor onMissing). Returns an error when the
// pointer is absent; callers treat that as "pre-versioned legacy EC" and
// fall back to the bare-key layout.
func (f *FSM) LookupLatestVersion(bucket, key string) (string, error) {
	var versionID string
	err := f.db.View(func(txn MetadataTxn) error {
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
	err := f.db.View(func(txn MetadataTxn) error {
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
		if errors.Is(err, ErrMetaKeyNotFound) {
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
	err := f.db.View(func(txn MetadataTxn) error {
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
			K:           int(meta.ECData),
			M:           int(meta.ECParity),
			StripeBytes: int(meta.StripeBytes),
			Nodes:       append([]string(nil), meta.NodeIDs...),
		}
		return nil
	})
	if errors.Is(err, ErrMetaKeyNotFound) {
		return PlacementRecord{}, nil
	}
	return rec, err
}

// LookupObjectECShards returns the EC shard parameters (k, m) stored in ObjectMeta
// for the given versioned object. Returns (0, 0, nil) when the object has no EC
// metadata (N× replication mode). Returns (0, 0, err) on a real BadgerDB read error.
func (f *FSM) LookupObjectECShards(bucket, key, versionID string) (k, m int, err error) {
	err = f.db.View(func(txn MetadataTxn) error {
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
	if errors.Is(err, ErrMetaKeyNotFound) {
		return 0, 0, nil // N× 모드: EC 메타 없음
	}
	return k, m, err
}
