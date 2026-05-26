package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/storage"
)

// segmentTarget builds an ECShardSegment scan target whose ShardKey matches the
// reconstruction IterECShardScanTargets performs (objectKey+"/segments/"+blobID).
func segmentTarget(bucket, objectKey, versionID, blobID string) ECShardScanTarget {
	return ECShardScanTarget{
		Kind:      ECShardSegment,
		Bucket:    bucket,
		ObjectKey: objectKey,
		VersionID: versionID,
		ShardKey:  objectKey + "/segments/" + blobID,
	}
}

// TestQuarantineCorruptShardLocalAtShardKey_VersionExists verifies that when the
// exact (bucket, objectKey, versionID) meta exists AND still references the target's
// shard, QuarantineObject is applied and incident facts are recorded.
func TestQuarantineCorruptShardLocalAtShardKey_VersionExists(t *testing.T) {
	ctx := context.Background()
	b := NewSingletonBackendForTest(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))

	// Seed a versioned object-meta whose Segments[] references blob "seg-1".
	seedObjectMetaVersion(t, b, "b", "obj", "v1", objectMeta{
		Segments: []storage.SegmentRef{
			{BlobID: "seg-1", ECData: 4, ECParity: 2, NodeIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6"}},
		},
	})

	rec := &recordingIncidentRecorder{}
	b.SetIncidentRecorder(rec)

	tgt := segmentTarget("b", "obj", "v1", "seg-1")
	err := b.QuarantineCorruptShardLocalAtShardKey(tgt, 0, "CRC mismatch at shardKey")
	require.NoError(t, err)

	// Incident facts must be recorded.
	require.NotEmpty(t, rec.facts)
	require.Equal(t, incident.FactObserved, rec.facts[0].Type)
	require.Equal(t, incident.CauseCorruptShard, rec.facts[0].Cause)
	require.Equal(t, incident.ScopeObject, rec.facts[0].Scope.Kind)
	require.Equal(t, "b", rec.facts[0].Scope.Bucket)
	require.Equal(t, "obj", rec.facts[0].Scope.Key)
	require.Equal(t, "v1", rec.facts[0].Scope.VersionID)
	require.Equal(t, 0, rec.facts[0].Scope.ShardID)
	// shardKey must appear in the Message for traceability.
	require.Contains(t, rec.facts[0].Message, "obj/segments/seg-1")
	require.Equal(t, incident.FactIsolated, rec.facts[len(rec.facts)-1].Type)

	// Reducer should confirm isolated state.
	state, err := incident.NewReducer().Reduce(append([]incident.Fact(nil), rec.facts...))
	require.NoError(t, err)
	require.Equal(t, incident.StateIsolated, state.State)
	require.Equal(t, incident.ActionIsolateObject, state.Action)

	// The object must be quarantined.
	quarantined, _, err := b.isObjectQuarantined("b", "obj", "v1")
	require.NoError(t, err)
	require.True(t, quarantined)
}

// TestQuarantineCorruptShardLocalAtShardKey_VersionGone verifies that when the
// object-version meta is absent (deleted/replaced between scan and callback),
// QuarantineObject is NOT called and no facts are recorded.
func TestQuarantineCorruptShardLocalAtShardKey_VersionGone(t *testing.T) {
	ctx := context.Background()
	b := NewSingletonBackendForTest(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))

	// Do NOT seed any meta for (b, obj, v1) — it is deliberately absent.
	rec := &recordingIncidentRecorder{}
	b.SetIncidentRecorder(rec)

	tgt := segmentTarget("b", "obj", "v1", "seg-1")
	err := b.QuarantineCorruptShardLocalAtShardKey(tgt, 0, "CRC mismatch at shardKey")
	require.NoError(t, err)

	// No facts should have been recorded.
	require.Empty(t, rec.facts)

	// The object must NOT be in quarantine state.
	quarantined, _, err := b.isObjectQuarantined("b", "obj", "v1")
	require.NoError(t, err)
	require.False(t, quarantined)
}

// TestQuarantineCorruptShardLocalAtShardKey_StaleTarget verifies that when the
// object-version meta EXISTS but no longer references the target's shard (the
// segment was legitimately coalesced away between scan and callback), the object
// is NOT quarantined — only logged. This defends against quarantining a clean,
// live object.
func TestQuarantineCorruptShardLocalAtShardKey_StaleTarget(t *testing.T) {
	ctx := context.Background()
	b := NewSingletonBackendForTest(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))

	// Seed a versioned meta whose Segments[] does NOT contain "seg-gone"
	// (coalesce consumed it while preserving the version).
	seedObjectMetaVersion(t, b, "b", "obj", "v1", objectMeta{
		Segments: []storage.SegmentRef{
			{BlobID: "seg-other", ECData: 4, ECParity: 2, NodeIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6"}},
		},
	})

	rec := &recordingIncidentRecorder{}
	b.SetIncidentRecorder(rec)

	tgt := segmentTarget("b", "obj", "v1", "seg-gone")
	err := b.QuarantineCorruptShardLocalAtShardKey(tgt, 0, "CRC mismatch at shardKey")
	require.NoError(t, err)

	// The de-referenced shard must NOT trigger quarantine or incident facts.
	require.Empty(t, rec.facts)
	quarantined, _, err := b.isObjectQuarantined("b", "obj", "v1")
	require.NoError(t, err)
	require.False(t, quarantined)
}

// TestShardTargetStillReferenced covers segment present/absent, coalesced
// present/absent, version-gone, and object-version-exists.
func TestShardTargetStillReferenced(t *testing.T) {
	ctx := context.Background()
	b := NewSingletonBackendForTest(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))

	seedObjectMetaVersion(t, b, "b", "obj", "v1", objectMeta{
		ECData: 4, ECParity: 2, NodeIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6"},
		Segments: []storage.SegmentRef{
			{BlobID: "seg-1", ECData: 4, ECParity: 2, NodeIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6"}},
		},
		Coalesced: []CoalescedShardRef{
			{CoalescedID: "c1", ShardKey: "obj/coalesced/c1", ECData: 4, ECParity: 2, NodeIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6"}},
		},
	})

	// Segment referenced → true.
	require.True(t, b.ShardTargetStillReferenced(ctx, segmentTarget("b", "obj", "v1", "seg-1")))
	// Segment blobID absent → false.
	require.False(t, b.ShardTargetStillReferenced(ctx, segmentTarget("b", "obj", "v1", "seg-absent")))

	// Coalesced ShardKey present → true.
	require.True(t, b.ShardTargetStillReferenced(ctx, ECShardScanTarget{
		Kind: ECShardCoalesced, Bucket: "b", ObjectKey: "obj", VersionID: "v1", ShardKey: "obj/coalesced/c1",
	}))
	// Coalesced ShardKey absent → false.
	require.False(t, b.ShardTargetStillReferenced(ctx, ECShardScanTarget{
		Kind: ECShardCoalesced, Bucket: "b", ObjectKey: "obj", VersionID: "v1", ShardKey: "obj/coalesced/absent",
	}))

	// Object-version target where the version exists → true.
	require.True(t, b.ShardTargetStillReferenced(ctx, ECShardScanTarget{
		Kind: ECShardObjectVersion, Bucket: "b", ObjectKey: "obj", VersionID: "v1",
	}))

	// Version gone → false (any kind).
	require.False(t, b.ShardTargetStillReferenced(ctx, segmentTarget("b", "obj", "v-missing", "seg-1")))
	require.False(t, b.ShardTargetStillReferenced(ctx, ECShardScanTarget{
		Kind: ECShardObjectVersion, Bucket: "b", ObjectKey: "obj", VersionID: "v-missing",
	}))
}
