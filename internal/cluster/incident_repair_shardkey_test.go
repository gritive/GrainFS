// incident_repair_shardkey_test.go: RepairShardLocalWithIncident dispatches to
// the shard-key repair path (RepairShardAtShardKey + localRepairTargetReadableAtShardKey)
// when req.ShardKey is set, for segment/coalesced shards that have no
// object-version placement record.

package cluster

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/stretchr/testify/require"
)

func TestRepairShardLocalWithIncident_ShardKeyPath(t *testing.T) {
	backend := setupECBackend(t)
	svc := backend.shardSvc

	require.NoError(t, backend.CreateBucket(t.Context(), "b"))

	const shardKey = "obj/segments/seg-blob-incident-0001"
	cfg := ECConfig{DataShards: 1, ParityShards: 1}
	content := bytes.Repeat([]byte("incident-shardkey-repair-"), 256)
	freshShards, err := ECSplit(cfg, content)
	require.NoError(t, err)
	require.Len(t, freshShards, 2)
	for i, s := range freshShards {
		require.NoError(t, svc.WriteLocalShard("b", shardKey, i, s))
	}

	// Drop shard 0 so it must be reconstructed from surviving shard 1.
	require.NoError(t, os.Remove(svc.getShardPath("b", shardKey, 0)))

	rec := PlacementRecord{Nodes: []string{"self", "self"}, K: 1, M: 1}
	recorder := &recordingIncidentRecorder{}
	err = backend.RepairShardLocalWithIncident(t.Context(), IncidentRepairRequest{
		Bucket:    "b",
		Key:       "obj",
		ShardKey:  shardKey,
		Placement: rec,
		ShardIdx:  0,
		Recorder:  recorder,
	})
	require.NoError(t, err)

	// Repair succeeded and the rebuilt shard matches the canonical split bytes.
	rebuilt, err := svc.ReadLocalShard("b", shardKey, 0)
	require.NoError(t, err)
	require.Equal(t, freshShards[0], rebuilt)

	// FactVerified recorded as the terminal fact.
	require.NotEmpty(t, recorder.facts)
	require.Equal(t, incident.FactVerified, recorder.facts[len(recorder.facts)-1].Type)

	// Traceability: the Diagnosed fact carries the shardKey in its message.
	sawShardKeyMsg := false
	for _, f := range recorder.facts {
		if f.Type == incident.FactDiagnosed && strings.Contains(f.Message, "shardKey="+shardKey) {
			sawShardKeyMsg = true
		}
	}
	require.True(t, sawShardKeyMsg, "Diagnosed fact must include shardKey for traceability")
}

func TestRepairShardLocalWithIncident_ShardKeyPath_FailsButReadable(t *testing.T) {
	backend := setupECBackend(t)
	svc := backend.shardSvc

	require.NoError(t, backend.CreateBucket(t.Context(), "b"))

	const shardKey = "obj/segments/seg-blob-incident-fails-readable"
	cfg := ECConfig{DataShards: 1, ParityShards: 1}
	content := bytes.Repeat([]byte("incident-fails-but-readable-"), 256)
	freshShards, err := ECSplit(cfg, content)
	require.NoError(t, err)
	require.Len(t, freshShards, 2)
	for i, s := range freshShards {
		require.NoError(t, svc.WriteLocalShard("b", shardKey, i, s))
	}

	// Drop shard 1 (the only OTHER shard). Repairing shard 0 skips shard 0 and
	// finds 0 survivors < DataShards=1, so RepairShardAtShardKey fails. But shard
	// 0 is still present on disk, so localRepairTargetReadableAtShardKey returns
	// true → the incident terminates in FactVerified, not FactActionFailed.
	require.NoError(t, os.Remove(svc.getShardPath("b", shardKey, 1)))

	rec := PlacementRecord{Nodes: []string{"self", "self"}, K: 1, M: 1}
	recorder := &recordingIncidentRecorder{}
	err = backend.RepairShardLocalWithIncident(t.Context(), IncidentRepairRequest{
		Bucket:    "b",
		Key:       "obj",
		ShardKey:  shardKey,
		Placement: rec,
		ShardIdx:  0,
		Recorder:  recorder,
	})
	require.NoError(t, err)
	require.NotEmpty(t, recorder.facts)
	require.Equal(t, incident.FactVerified, recorder.facts[len(recorder.facts)-1].Type)
}

func TestRepairShardLocalWithIncident_ShardKeyRequiresPlacement(t *testing.T) {
	backend := setupECBackend(t)
	recorder := &recordingIncidentRecorder{}
	err := backend.RepairShardLocalWithIncident(t.Context(), IncidentRepairRequest{
		Bucket:   "b",
		Key:      "obj",
		ShardKey: "obj/segments/seg-blob-no-placement",
		ShardIdx: 0,
		Recorder: recorder,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-empty Placement.Nodes")
	// Fast-fail happens before any facts are recorded.
	require.Empty(t, recorder.facts)
}

func TestLocalRepairTargetReadableAtShardKey(t *testing.T) {
	backend := setupECBackend(t)
	svc := backend.shardSvc
	require.NoError(t, backend.CreateBucket(t.Context(), "b"))

	const shardKey = "obj/segments/seg-blob-readable-0001"
	require.NoError(t, svc.WriteLocalShard("b", shardKey, 0, []byte("locally-present")))

	// Local owner with the shard present → readable.
	owned := PlacementRecord{Nodes: []string{"self", "self"}, K: 1, M: 1}
	require.True(t, backend.localRepairTargetReadableAtShardKey(t.Context(), "b", shardKey, owned, 0))

	// Not the local owner for shard 0 → not readable, regardless of disk state.
	notOwner := PlacementRecord{Nodes: []string{"other", "self"}, K: 1, M: 1}
	require.False(t, backend.localRepairTargetReadableAtShardKey(t.Context(), "b", shardKey, notOwner, 0))

	// Out-of-range shard index → not readable.
	require.False(t, backend.localRepairTargetReadableAtShardKey(t.Context(), "b", shardKey, owned, 5))

	// Nil shard service → not readable.
	backend.shardSvc = nil
	require.False(t, backend.localRepairTargetReadableAtShardKey(t.Context(), "b", shardKey, owned, 0))
}
