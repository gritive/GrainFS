package cluster

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage"
)

// seedLatestObjectMetaVersion writes one objectMeta version AND the lat:
// pointer directly into the FSM. It is retained for tests that exercise the
// FSM-reading EC-repair placement resolver (ResolveShardKeyPlacement).
func seedLatestObjectMetaVersion(t *testing.T, b *DistributedBackend, bucket, key, versionID string, m objectMeta) {
	t.Helper()
	m.Key = key
	f := b.FSMRef()
	val, err := marshalObjectMeta(m)
	require.NoError(t, err)
	require.NoError(t, f.db.Update(func(txn MetadataTxn) error {
		if err := f.setValue(txn, f.keys.ObjectMetaKeyV(bucket, key, versionID), val); err != nil {
			return err
		}
		return txn.Set(f.keys.LatestKey(bucket, key), []byte(versionID))
	}))
}

// buildTargets drives buildECShardTargets over one object meta ref and returns
// the emitted scan targets. buildECShardTargets is the EC-shard target
// constructor consumed by both live enumerators (the placement monitor's
// IterQuorumMetaECShardTargets and the DEK rewrap blob enum), so testing it
// directly covers the construction + EC-validation logic without the FSM scan.
func buildTargets(t *testing.T, b *DistributedBackend, ref ObjectMetaRef, m objectMeta) []ECShardScanTarget {
	t.Helper()
	var got []ECShardScanTarget
	require.NoError(t, b.FSMRef().buildECShardTargets(ref, m, func(tgt ECShardScanTarget) error {
		got = append(got, tgt)
		return nil
	}))
	return got
}

func TestBuildECShardTargets(t *testing.T) {
	ctx := context.Background()
	backend := NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(ctx, "b"))

	// (a) Chunked object: Segments[] mixing a valid EC seg, an owner-local
	// (ECData==0) seg, and a malformed (NodeIDs-length mismatch) EC seg; plus a
	// valid coalesced EC ref + an owner-local coalesced ref. Expect exactly the
	// valid segment + coalesced targets, and NO object-version target.
	segNodes := []string{"n1", "n2", "n3"}
	coalNodes := []string{"n4", "n5", "n6"}

	// The malformed seg-bad ref must bump the "segment" invalid-EC counter by
	// exactly 1; the owner-local (ECData==0) skips must NOT touch it. The counter
	// is a process-global registered once at init, so use a delta.
	segBefore := testutil.ToFloat64(metrics.PlacementMonitorInvalidECRef.WithLabelValues("segment"))
	coalBefore := testutil.ToFloat64(metrics.PlacementMonitorInvalidECRef.WithLabelValues("coalesced"))

	chunkedTargets := buildTargets(t, backend, ObjectMetaRef{Bucket: "b", Key: "chunked", VersionID: "cv1"}, objectMeta{
		Key: "chunked",
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

	require.Equal(t, float64(1),
		testutil.ToFloat64(metrics.PlacementMonitorInvalidECRef.WithLabelValues("segment"))-segBefore,
		"malformed segment ref must increment the invalid-EC counter exactly once")
	require.Equal(t, float64(0),
		testutil.ToFloat64(metrics.PlacementMonitorInvalidECRef.WithLabelValues("coalesced"))-coalBefore,
		"owner-local (ECData==0) coalesced skip must not increment the invalid-EC counter")

	var (
		segTargets  []ECShardScanTarget
		coalTargets []ECShardScanTarget
		ovTargets   []ECShardScanTarget
	)
	for _, tgt := range chunkedTargets {
		switch tgt.Kind {
		case ECShardSegment:
			segTargets = append(segTargets, tgt)
		case ECShardCoalesced:
			coalTargets = append(coalTargets, tgt)
		case ECShardObjectVersion:
			ovTargets = append(ovTargets, tgt)
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
	require.Empty(t, ovTargets, "chunked object must not emit an object-version target")

	// (b) Plain single-blob EC object: no Segments/Coalesced. Expect exactly one
	// ECShardObjectVersion target carrying the raw object EC fields.
	ovNodes := []string{"o1", "o2", "o3"}
	plainTargets := buildTargets(t, backend, ObjectMetaRef{Bucket: "b", Key: "plain", VersionID: "pv1"}, objectMeta{
		Key: "plain", ECData: 2, ECParity: 1, NodeIDs: ovNodes, PlacementGroupID: "pg-1",
	})
	require.Len(t, plainTargets, 1)
	require.Equal(t, ECShardScanTarget{
		Kind:             ECShardObjectVersion,
		Bucket:           "b",
		ObjectKey:        "plain",
		VersionID:        "pv1",
		ECData:           2,
		ECParity:         1,
		NodeIDs:          ovNodes,
		PlacementGroupID: "pg-1",
	}, plainTargets[0])

	// (c) A version-less ref for a plain (no segments/coalesced) legacy object
	// still emits its object-version target, with an empty VersionID.
	legacyTargets := buildTargets(t, backend, ObjectMetaRef{Bucket: "b", Key: "legacy", VersionID: ""}, objectMeta{
		Key: "legacy", ECData: 2, ECParity: 1, NodeIDs: []string{"l1", "l2", "l3"},
	})
	require.Len(t, legacyTargets, 1)
	require.Equal(t, ECShardObjectVersion, legacyTargets[0].Kind)
	require.Equal(t, "", legacyTargets[0].VersionID)

	// (d) A version-less ref WITH segments emits NO targets: object-version is
	// suppressed (it has segments) and segment/coalesced targets are guarded on
	// VersionID != "" to avoid a version-less misparse shard key.
	guarded := buildTargets(t, backend, ObjectMetaRef{Bucket: "b", Key: "guarded", VersionID: ""}, objectMeta{
		Key: "guarded", ECData: 2, ECParity: 1, NodeIDs: []string{"g1", "g2", "g3"},
		Segments: []storage.SegmentRef{
			{BlobID: "seg-x", ECData: 2, ECParity: 1, NodeIDs: []string{"g1", "g2", "g3"}},
		},
	})
	require.Empty(t, guarded, "version-less ref with segments must emit no targets")
}

// TestBuildECShardTargets_CarryStripeBytes asserts the constructor propagates
// StripeBytes (not just K/M) onto the PlacementRecord that flows into
// RepairShardAtShardKey -> reconstructShardAtKey. Without it, repair would
// regenerate a contiguous shard for a striped segment and corrupt it on heal.
func TestBuildECShardTargets_CarryStripeBytes(t *testing.T) {
	ctx := context.Background()
	backend := NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(ctx, "b"))

	const segStripe, coalStripe = 1 << 20, 1 << 19
	segNodes := []string{"n1", "n2", "n3"}
	coalNodes := []string{"n4", "n5", "n6"}

	targets := buildTargets(t, backend, ObjectMetaRef{Bucket: "b", Key: "striped", VersionID: "sv1"}, objectMeta{
		Key: "striped",
		Segments: []storage.SegmentRef{
			{BlobID: "seg-s", ECData: 2, ECParity: 1, NodeIDs: segNodes, StripeBytes: segStripe},
		},
		Coalesced: []CoalescedShardRef{
			{CoalescedID: "c1", ShardKey: "striped/coalesced/c1", ECData: 2, ECParity: 1, NodeIDs: coalNodes, StripeBytes: coalStripe},
		},
	})

	var segTgt, coalTgt *ECShardScanTarget
	for i := range targets {
		switch targets[i].Kind {
		case ECShardSegment:
			segTgt = &targets[i]
		case ECShardCoalesced:
			coalTgt = &targets[i]
		}
	}
	require.NotNil(t, segTgt)
	require.NotNil(t, coalTgt)
	require.Equal(t, segStripe, segTgt.Placement.StripeBytes, "segment target must carry StripeBytes")
	require.Equal(t, coalStripe, coalTgt.Placement.StripeBytes, "coalesced target must carry StripeBytes")

	// ResolveShardKeyPlacement -> RepairShardAtShardKey must also carry
	// StripeBytes. This path still reads the FSM obj: record (EC-repair
	// placement resolver), so seed it accordingly.
	seedLatestObjectMetaVersion(t, backend, "b", "striped", "sv1", objectMeta{
		Segments: []storage.SegmentRef{
			{BlobID: "seg-s", ECData: 2, ECParity: 1, NodeIDs: segNodes, StripeBytes: segStripe},
		},
		Coalesced: []CoalescedShardRef{
			{CoalescedID: "c1", ShardKey: "striped/coalesced/c1", ECData: 2, ECParity: 1, NodeIDs: coalNodes, StripeBytes: coalStripe},
		},
	})
	scan := NewShardKeyPlacementScanCache()
	segRec, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "striped/segments/seg-s", scan)
	require.NoError(t, err)
	require.Empty(t, skip)
	require.Equal(t, segStripe, segRec.StripeBytes, "segment placement must carry StripeBytes")

	coalRec, skip, err := backend.ResolveShardKeyPlacement(ctx, "b", "striped/coalesced/c1", scan)
	require.NoError(t, err)
	require.Empty(t, skip)
	require.Equal(t, coalStripe, coalRec.StripeBytes, "coalesced placement must carry StripeBytes")
}
