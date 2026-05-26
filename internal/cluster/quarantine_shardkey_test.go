package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/storage"
)

// TestQuarantineCorruptShardLocalAtShardKey_VersionExists verifies that when the
// exact (bucket, objectKey, versionID) meta exists, QuarantineObject is applied and
// incident facts are recorded (mirroring QuarantineCorruptShardLocal assertions).
func TestQuarantineCorruptShardLocalAtShardKey_VersionExists(t *testing.T) {
	ctx := context.Background()
	b := NewSingletonBackendForTest(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))

	// Seed a versioned object-meta so the existence check passes.
	seedObjectMetaVersion(t, b, "b", "obj", "v1", objectMeta{
		Segments: []storage.SegmentRef{
			{BlobID: "seg-1", ECData: 4, ECParity: 2, NodeIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6"}},
		},
	})

	rec := &recordingIncidentRecorder{}
	b.SetIncidentRecorder(rec)

	err := b.QuarantineCorruptShardLocalAtShardKey("b", "obj", "v1", "seg-1/shard-0", 0, "CRC mismatch at shardKey")
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
	require.Contains(t, rec.facts[0].Message, "seg-1/shard-0")
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

	err := b.QuarantineCorruptShardLocalAtShardKey("b", "obj", "v1", "seg-1/shard-0", 0, "CRC mismatch at shardKey")
	require.NoError(t, err)

	// No facts should have been recorded.
	require.Empty(t, rec.facts)

	// The object must NOT be in quarantine state.
	quarantined, _, err := b.isObjectQuarantined("b", "obj", "v1")
	require.NoError(t, err)
	require.False(t, quarantined)
}
