package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/storage"
)

// soleAuthReadOn reports whether the bucket's per-version quorum-meta blob tree
// is the SOLE AUTHORITY for reads (soleauth state == "on"). It FAILS CLOSED: on
// any error reading the soleauth state it returns (false, err) so callers must
// surface the error rather than silently treating the bucket as "off".
//
// Returns (true, nil) iff the state is soleAuthOn; (false, nil) for off/pending.
//
// Wired into the HEAD/GET latest reader (headObjectMeta) in S4c-c-read1 T1;
// the specific-version and GetObjectTags readers follow in T2/T3.
func (b *DistributedBackend) soleAuthReadOn(bucket string) (bool, error) {
	state, err := b.GetBucketSoleAuthority(bucket)
	if err != nil {
		return false, fmt.Errorf("read soleauth state for bucket %q: %w", bucket, err)
	}
	return state == soleAuthOn, nil
}

// fsmCarveoutObject reads the FSM BadgerDB object record the same way the
// availability-first FSM fallback in headObjectMeta does (lat:{bucket}/{key} →
// obj:{bucket}/{key}/{vid}, else bare obj:{bucket}/{key}) and returns the
// decoded object ONLY IF it belongs to a carve-out class that stays
// FSM-authoritative even under soleauth=on:
//
//  1. appendable      (meta.IsAppendable)
//  2. coalesced       (len(meta.Coalesced) > 0)
//  3. legacy unversioned "bare" record (obj:{bucket}/{key} with NO lat: pointer)
//
// Otherwise it returns (nil, PlacementMeta{}, false, nil) — a non-carve-out
// record (a plain versioned obj) must NOT resurrect a blob-deleted versioned
// object under sole authority, so the caller treats it as 404.
//
// An absent FSM record returns (nil, PlacementMeta{}, false, nil) (not an
// error). Store errors propagate.
//
// CRITICAL: bare-vs-versioned is resolved from WHICH KEY was actually read, not
// from the versionID argument (the decoded objectMeta carries no VersionID
// field):
//   - versionID != "": target is a vid-bearing versioned record
//     (obj:{bucket}/{key}/{vid}) → NEVER a bare-legacy carve-out.
//   - versionID == "": if a lat: pointer EXISTS the object is versioned (its
//     latest record is vid-bearing) → NOT bare-legacy; only the
//     appendable/coalesced test applies. If lat: is ABSENT but a bare
//     obj:{bucket}/{key} record EXISTS → legacy unversioned carve-out.
//
// Wired into the HEAD/GET latest reader (headObjectMeta) in S4c-c-read1 T1;
// the specific-version and GetObjectTags readers follow in T2/T3.
func (b *DistributedBackend) fsmCarveoutObject(bucket, key, versionID string) (*storage.Object, PlacementMeta, bool, error) {
	var (
		meta        objectMeta
		found       bool
		bareLegacy  bool
		resolvedVID = versionID
	)
	err := b.store.View(func(txn MetadataTxn) error {
		// Resolve the read key exactly like headObjectMeta's FSM fallback.
		metaKeyBytes := b.ks().ObjectMetaKey(bucket, key)
		latPresent := false
		if versionID == "" {
			if latItem, lerr := txn.Get(b.ks().LatestKey(bucket, key)); lerr == nil {
				latPresent = true
				_ = latItem.Value(func(v []byte) error {
					resolvedVID = string(v)
					return nil
				})
				if resolvedVID != "" {
					metaKeyBytes = b.ks().ObjectMetaKeyV(bucket, key, resolvedVID)
				}
			} else if lerr != ErrMetaKeyNotFound {
				return lerr
			}
		} else {
			metaKeyBytes = b.ks().ObjectMetaKeyV(bucket, key, versionID)
		}

		// A bare-legacy record is only possible for a latest read whose key had
		// no lat: pointer (so it falls back to the bare obj: key).
		bareLegacy = versionID == "" && !latPresent

		item, err := txn.Get(metaKeyBytes)
		if err == ErrMetaKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		val, err := b.itemValueCopy(item)
		if err != nil {
			return err
		}
		m, err := unmarshalObjectMeta(val)
		if err != nil {
			return err
		}
		meta = m
		found = true
		return nil
	})
	if err != nil {
		return nil, PlacementMeta{}, false, err
	}
	if !found {
		return nil, PlacementMeta{}, false, nil
	}

	if !isFsmCarveoutClass(meta, bareLegacy) {
		return nil, PlacementMeta{}, false, nil
	}

	// Bare-legacy records have no version identity; mirror headObjectMeta's
	// latest read which carries the (empty) resolved versionID.
	vid := resolvedVID
	if bareLegacy {
		vid = ""
	}
	obj, pm := objectAndPlacementFromObjectMeta(meta, vid)
	return obj, pm, true, nil
}

// isFsmCarveoutClass reports whether a decoded FSM objectMeta belongs to a
// carve-out class that stays FSM-authoritative even under soleauth=on:
//
//  1. appendable  (meta.IsAppendable)
//  2. coalesced   (len(meta.Coalesced) > 0)
//  3. legacy unversioned "bare" record (bareLegacy — an obj:{bucket}/{key}
//     record with NO lat: pointer, resolved by the caller from WHICH KEY was
//     actually read, NOT from any versionID argument)
//
// SHARED PREDICATE: fsmCarveoutObject (single-object read, S4c-c-read1) and
// DistributedBackend.ListObjectVersions's bucket-wide carve-out scan (S4c-c
// T2) BOTH classify records through this one rule so the two paths cannot
// drift. A plain vid-bearing versioned record (none of the three) is NOT a
// carve-out: it is blob-authoritative under sole authority and a stale one
// must never resurrect.
func isFsmCarveoutClass(meta objectMeta, bareLegacy bool) bool {
	return meta.IsAppendable || len(meta.Coalesced) > 0 || bareLegacy
}

// objectAndPlacementFromObjectMeta builds a storage.Object and PlacementMeta
// from a decoded FSM objectMeta, mirroring exactly the construction the
// availability-first FSM fallback in headObjectMeta performs (decodeMeta
// closure). Single-sourced here so the soleauth read carve-out path cannot
// drift from the FSM fallback's object/placement shape.
//
// Invoked by fsmCarveoutObject, which is wired into headObjectMeta in
// S4c-c-read1 T1.
func objectAndPlacementFromObjectMeta(m objectMeta, versionID string) (*storage.Object, PlacementMeta) {
	obj := &storage.Object{
		Key:              m.Key,
		Size:             m.Size,
		ContentType:      m.ContentType,
		ETag:             m.ETag,
		LastModified:     m.LastModified,
		VersionID:        versionID,
		ACL:              m.ACL,
		UserMetadata:     cloneStringMap(m.UserMetadata),
		SSEAlgorithm:     m.SSEAlgorithm,
		PlacementGroupID: m.PlacementGroupID,
		ECData:           m.ECData,
		ECParity:         m.ECParity,
		StripeBytes:      m.StripeBytes,
		NodeIDs:          cloneStringSlice(m.NodeIDs),
		Segments:         m.Segments,
		Parts:            m.Parts,
		Coalesced:        coalescedRefsToStorage(m.Coalesced),
		IsAppendable:     m.IsAppendable,
		Tags:             append([]storage.Tag(nil), m.Tags...),
	}
	placement := PlacementMeta{
		VersionID:        versionID,
		ECData:           m.ECData,
		ECParity:         m.ECParity,
		StripeBytes:      m.StripeBytes,
		NodeIDs:          m.NodeIDs,
		PlacementGroupID: m.PlacementGroupID,
	}
	return obj, placement
}
