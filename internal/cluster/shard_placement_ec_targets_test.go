package cluster

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage"
)

// seedLatestObjectMetaVersion writes one objectMeta version AND the lat:
// pointer, mirroring applyPutObjectMeta's write path so iterLatestObjectMetas'
// lat: pass (the primary enumeration path) is exercised with a correctly split
// key and a populated VersionID.
func seedLatestObjectMetaVersion(t *testing.T, b *DistributedBackend, bucket, key, versionID string, m objectMeta) {
	t.Helper()
	m.Key = key
	f := b.FSMRef()
	val, err := marshalObjectMeta(m)
	require.NoError(t, err)
	require.NoError(t, f.db.Update(func(txn *badger.Txn) error {
		if err := f.setValue(txn, f.keys.ObjectMetaKeyV(bucket, key, versionID), val); err != nil {
			return err
		}
		return txn.Set(f.keys.LatestKey(bucket, key), []byte(versionID))
	}))
}

func collectECTargets(t *testing.T, b *DistributedBackend) []ECShardScanTarget {
	t.Helper()
	var got []ECShardScanTarget
	require.NoError(t, b.FSMRef().IterECShardScanTargets(func(tgt ECShardScanTarget) error {
		got = append(got, tgt)
		return nil
	}))
	return got
}

func TestIterECShardScanTargets(t *testing.T) {
	ctx := context.Background()
	backend := NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(ctx, "b"))

	// (a) Chunked object: Segments[] mixing a valid EC seg, an owner-local
	// (ECData==0) seg, and a malformed (NodeIDs-length mismatch) EC seg; plus a
	// valid coalesced EC ref. Expect exactly the valid segment + coalesced
	// targets, and NO object-version target for this object.
	segNodes := []string{"n1", "n2", "n3"}
	coalNodes := []string{"n4", "n5", "n6"}
	seedLatestObjectMetaVersion(t, backend, "b", "chunked", "cv1", objectMeta{
		// Top-level EC fields are a segment-0 mirror — must NOT produce a target.
		ECData: 2, ECParity: 1, NodeIDs: segNodes,
		Segments: []storage.SegmentRef{
			{BlobID: "seg-ok", ECData: 2, ECParity: 1, NodeIDs: segNodes},          // valid
			{BlobID: "seg-local", ECData: 0, NodeIDs: []string{"nx"}},              // owner-local, skipped
			{BlobID: "seg-bad", ECData: 2, ECParity: 1, NodeIDs: []string{"only"}}, // malformed, skipped
		},
		Coalesced: []CoalescedShardRef{
			{CoalescedID: "c1", ShardKey: "chunked/coalesced/c1", ECData: 2, ECParity: 1, NodeIDs: coalNodes},
			{CoalescedID: "c2", ShardKey: "chunked/coalesced/c2", ECData: 0, NodeIDs: nil}, // owner-local, skipped
		},
	})

	// (b) Plain single-blob EC object: no Segments/Coalesced. Expect exactly one
	// ECShardObjectVersion target carrying the raw object EC fields.
	ovNodes := []string{"o1", "o2", "o3"}
	seedLatestObjectMetaVersion(t, backend, "b", "plain", "pv1", objectMeta{
		ECData: 2, ECParity: 1, NodeIDs: ovNodes, PlacementGroupID: "pg-1",
	})

	// (c) Legacy unversioned obj: object with NO lat: pointer — must still be
	// enumerated via the shared-core fallback pass. Kept as a plain object so
	// the legacy-shaped ref.Key does not corrupt any shard key.
	seedObjectMetaVersion(t, backend, "b", "legacy", "lv1", objectMeta{
		ECData: 2, ECParity: 1, NodeIDs: []string{"l1", "l2", "l3"},
	})

	// Capture the invalid-EC counter before/after delta. The malformed seg-bad
	// ref must bump the "segment" counter by exactly 1; the owner-local
	// (ECData==0) skips (seg-local / c2) must NOT touch it. The counter is a
	// process-global registered once at init, so use a delta rather than a
	// reset to stay robust against other subtests.
	segBefore := testutil.ToFloat64(metrics.PlacementMonitorInvalidECRef.WithLabelValues("segment"))
	coalBefore := testutil.ToFloat64(metrics.PlacementMonitorInvalidECRef.WithLabelValues("coalesced"))

	got := collectECTargets(t, backend)

	require.Equal(t, float64(1),
		testutil.ToFloat64(metrics.PlacementMonitorInvalidECRef.WithLabelValues("segment"))-segBefore,
		"malformed segment ref must increment the invalid-EC counter exactly once")
	require.Equal(t, float64(0),
		testutil.ToFloat64(metrics.PlacementMonitorInvalidECRef.WithLabelValues("coalesced"))-coalBefore,
		"owner-local (ECData==0) coalesced skip must not increment the invalid-EC counter")

	// Index emitted targets for assertion.
	var (
		segTargets   []ECShardScanTarget
		coalTargets  []ECShardScanTarget
		ovTargets    []ECShardScanTarget
		legacySeen   bool
		legacyVerSet bool
	)
	for _, tgt := range got {
		switch tgt.Kind {
		case ECShardSegment:
			segTargets = append(segTargets, tgt)
		case ECShardCoalesced:
			coalTargets = append(coalTargets, tgt)
		case ECShardObjectVersion:
			ovTargets = append(ovTargets, tgt)
			// Legacy fallback yields the raw versioned key as ObjectKey
			// ("legacy/lv1", first-slash split) and an empty VersionID —
			// confirm the shared-core fallback path is taken.
			if tgt.ObjectKey == "legacy/lv1" {
				legacySeen = true
				legacyVerSet = tgt.VersionID != ""
			}
		}
	}

	// (a) exactly one valid segment target.
	require.Len(t, segTargets, 1)
	require.Equal(t, ECShardScanTarget{
		Kind:      ECShardSegment,
		Bucket:    "b",
		ObjectKey: "chunked",
		VersionID: "cv1",
		ShardKey:  "chunked/segments/seg-ok",
		Placement: PlacementRecord{Nodes: segNodes, K: 2, M: 1},
	}, segTargets[0])

	// (a) exactly one valid coalesced target, ShardKey taken from the ref.
	require.Len(t, coalTargets, 1)
	require.Equal(t, ECShardScanTarget{
		Kind:      ECShardCoalesced,
		Bucket:    "b",
		ObjectKey: "chunked",
		VersionID: "cv1",
		ShardKey:  "chunked/coalesced/c1",
		Placement: PlacementRecord{Nodes: coalNodes, K: 2, M: 1},
	}, coalTargets[0])

	// (a) NO object-version target for the chunked object.
	for _, tgt := range ovTargets {
		require.NotEqual(t, "chunked", tgt.ObjectKey, "chunked object must not emit an object-version target")
	}

	// (b) exactly one object-version target for the plain object.
	var plainOV *ECShardScanTarget
	for i := range ovTargets {
		if ovTargets[i].ObjectKey == "plain" {
			plainOV = &ovTargets[i]
		}
	}
	require.NotNil(t, plainOV, "plain EC object must emit an object-version target")
	require.Equal(t, ECShardScanTarget{
		Kind:             ECShardObjectVersion,
		Bucket:           "b",
		ObjectKey:        "plain",
		VersionID:        "pv1",
		ECData:           2,
		ECParity:         1,
		NodeIDs:          ovNodes,
		PlacementGroupID: "pg-1",
	}, *plainOV)
	// And the plain object must not emit any segment/coalesced targets.
	for _, tgt := range segTargets {
		require.NotEqual(t, "plain", tgt.ObjectKey)
	}

	// (c) legacy unversioned object is still enumerated via the fallback pass.
	require.True(t, legacySeen, "legacy unversioned object must be enumerated via the shared-core fallback")
	require.False(t, legacyVerSet, "legacy fallback ref carries an empty VersionID")
}

// TestIterECShardScanTargetsAllVersions verifies that IterECShardScanTargetsAllVersions
// covers non-latest versions AND legacy unversioned objects, unlike
// IterECShardScanTargets which only covers the latest version per key.
//
// Setup:
//   - "b/versioned": v0 (old, no lat: pointer) + v1-latest (lat: → v1). Plain
//     single-blob EC objects. IterECShardScanTargets yields only v1.
//     IterECShardScanTargetsAllVersions must yield BOTH v0 AND v1.
//   - "b/legacy": unversioned object (obj:b/legacy, no lat: pointer). Both
//     enumerators must yield it.
func TestIterECShardScanTargetsAllVersions(t *testing.T) {
	ctx := context.Background()
	backend := NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(ctx, "b"))

	v0Nodes := []string{"a1", "a2", "a3"}
	v1Nodes := []string{"b1", "b2", "b3"}
	legacyNodes := []string{"c1", "c2", "c3"}

	// v0: non-latest, obj:b/versioned/v0 only (no lat: pointer).
	seedObjectMetaVersion(t, backend, "b", "versioned", "v0", objectMeta{
		ECData: 2, ECParity: 1, NodeIDs: v0Nodes,
	})
	// v1: latest, obj:b/versioned/v1 + lat:b/versioned → v1.
	seedLatestObjectMetaVersion(t, backend, "b", "versioned", "v1", objectMeta{
		ECData: 2, ECParity: 1, NodeIDs: v1Nodes,
	})
	// Legacy unversioned: real legacy format uses ObjectMetaKey (no trailing slash),
	// not ObjectMetaKeyV with empty version. Seed directly to match the real write path.
	{
		f := backend.FSMRef()
		m := objectMeta{ECData: 2, ECParity: 1, NodeIDs: legacyNodes, Key: "legacy"}
		val, err := marshalObjectMeta(m)
		require.NoError(t, err)
		require.NoError(t, f.db.Update(func(txn *badger.Txn) error {
			return f.setValue(txn, f.keys.ObjectMetaKey("b", "legacy"), val)
		}))
	}

	collectAllVersions := func(t *testing.T, b *DistributedBackend) []ECShardScanTarget {
		t.Helper()
		var out []ECShardScanTarget
		require.NoError(t, b.FSMRef().IterECShardScanTargetsAllVersions(func(tgt ECShardScanTarget) error {
			out = append(out, tgt)
			return nil
		}))
		return out
	}

	latestOnly := collectECTargets(t, backend) // IterECShardScanTargets
	allVers := collectAllVersions(t, backend)  // IterECShardScanTargetsAllVersions

	// Helper to find a target by VersionID.
	findByVID := func(targets []ECShardScanTarget, vid string) *ECShardScanTarget {
		for i := range targets {
			if targets[i].VersionID == vid {
				return &targets[i]
			}
		}
		return nil
	}

	// IterECShardScanTargets must yield v1 + legacy, but NOT v0.
	require.NotNil(t, findByVID(latestOnly, "v1"), "latest-only enumerator must yield v1")
	require.Nil(t, findByVID(latestOnly, "v0"), "latest-only enumerator must NOT yield non-latest v0")

	// IterECShardScanTargetsAllVersions must yield v0, v1, AND legacy.
	tgtV0 := findByVID(allVers, "v0")
	require.NotNil(t, tgtV0, "all-version enumerator must yield non-latest v0")
	require.Equal(t, "b", tgtV0.Bucket)
	require.Equal(t, "versioned", tgtV0.ObjectKey)
	require.Equal(t, v0Nodes, tgtV0.NodeIDs)

	tgtV1 := findByVID(allVers, "v1")
	require.NotNil(t, tgtV1, "all-version enumerator must yield latest v1")
	require.Equal(t, v1Nodes, tgtV1.NodeIDs)

	// Legacy unversioned object: VersionID is "".
	tgtLegacy := findByVID(allVers, "")
	require.NotNil(t, tgtLegacy, "all-version enumerator must yield legacy unversioned object")
	require.Equal(t, legacyNodes, tgtLegacy.NodeIDs)

	// CollectECRewrapTargets glue: verify Kind switch, ecObjectShardKey derivation,
	// and NodeIDs plumbing end-to-end over the same seeded backend.
	rewrapTargets, err := backend.CollectECRewrapTargets()
	require.NoError(t, err)
	findRewrap := func(shardKey string) *ECRewrapTarget {
		for i := range rewrapTargets {
			if rewrapTargets[i].ShardKey == shardKey {
				return &rewrapTargets[i]
			}
		}
		return nil
	}
	// v0: ecObjectShardKey("versioned","v0") = "versioned/v0"
	rtV0 := findRewrap(ecObjectShardKey("versioned", "v0"))
	require.NotNil(t, rtV0, "CollectECRewrapTargets must include non-latest v0")
	require.Equal(t, "b", rtV0.Bucket)
	require.Equal(t, v0Nodes, rtV0.NodeIDs)

	// v1: ecObjectShardKey("versioned","v1") = "versioned/v1"
	rtV1 := findRewrap(ecObjectShardKey("versioned", "v1"))
	require.NotNil(t, rtV1, "CollectECRewrapTargets must include latest v1")
	require.Equal(t, v1Nodes, rtV1.NodeIDs)

	// legacy: ecObjectShardKey("legacy","") = "legacy"
	rtLegacy := findRewrap(ecObjectShardKey("legacy", ""))
	require.NotNil(t, rtLegacy, "CollectECRewrapTargets must include legacy unversioned object")
	require.Equal(t, legacyNodes, rtLegacy.NodeIDs)
}

// TestIterECShardScanTargets_DeletedChunkedObject reproduces Finding 2: when the
// latest pointer is a delete marker, the lat: pass returns before adding the base
// key to `seen`, so the legacy obj: fallback enumerates the OLDER versioned meta
// (obj:bucket/k/v1) with a first-slash split — yielding ref.Key="k/v1",
// ref.VersionID="". Without the VersionID!="" guard this misparse would emit a
// WRONG segment target "k/v1/segments/<blob>" and trigger a false-missing repair
// every scan. Assert NO segment target is emitted for the deleted object.
func TestIterECShardScanTargets_DeletedChunkedObject(t *testing.T) {
	ctx := context.Background()
	backend := NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(ctx, "b"))

	segNodes := []string{"n1", "n2", "n3"}
	// Older chunked version k/v1 with an EC segment, NO lat: pointer of its own.
	seedObjectMetaVersion(t, backend, "b", "k", "v1", objectMeta{
		ECData: 2, ECParity: 1, NodeIDs: segNodes,
		Segments: []storage.SegmentRef{
			{BlobID: "blob-1", ECData: 2, ECParity: 1, NodeIDs: segNodes},
		},
	})
	// Latest version is a delete marker; lat: -> del-v2.
	seedLatestObjectMetaVersion(t, backend, "b", "k", "del-v2", objectMeta{
		ETag: deleteMarkerETag,
	})

	got := collectECTargets(t, backend)

	for _, tgt := range got {
		if tgt.Kind == ECShardSegment {
			require.NotEqual(t, "k/v1/segments/blob-1", tgt.ShardKey,
				"deleted chunked object must not emit a misparsed segment target")
		}
		// More broadly: no segment/coalesced target may carry the misparsed
		// legacy-fallback key shape for this object.
		if tgt.Kind == ECShardSegment || tgt.Kind == ECShardCoalesced {
			require.NotEqual(t, "k/v1", tgt.ObjectKey,
				"misparsed legacy-fallback ref must not yield a segment/coalesced target")
		}
	}
}
