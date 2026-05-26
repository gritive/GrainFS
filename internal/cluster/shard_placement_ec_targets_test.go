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
