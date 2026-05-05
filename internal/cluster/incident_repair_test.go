package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/incident"
)

type recordingIncidentRecorder struct{ facts []incident.Fact }

func (r *recordingIncidentRecorder) Record(_ context.Context, facts []incident.Fact) error {
	r.facts = append(r.facts, facts...)
	return nil
}

func TestIncidentRepair_RecordsFailureWhenShardServiceMissing(t *testing.T) {
	b := newTestDistributedBackend(t)
	writePlacement(t, b, "b", "k/v1", []string{"test-node", "other-a"})
	rec := &recordingIncidentRecorder{}

	err := b.RepairShardLocalWithIncident(context.Background(), IncidentRepairRequest{
		Bucket: "b", Key: "k", VersionID: "v1", ShardIdx: 0, Recorder: rec, Now: time.Unix(100, 0).UTC(),
	})
	require.Error(t, err)
	require.NotEmpty(t, rec.facts)
	require.Equal(t, incident.FactObserved, rec.facts[0].Type)
	require.Equal(t, incident.FactActionFailed, rec.facts[len(rec.facts)-1].Type)
}

func TestIncidentRepair_ContextCanceledRecordsBlocked(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := &recordingIncidentRecorder{}

	err := b.RepairShardLocalWithIncident(ctx, IncidentRepairRequest{
		Bucket: "b", Key: "k", VersionID: "v1", ShardIdx: 0, Recorder: rec, Now: time.Unix(100, 0).UTC(),
	})
	require.True(t, errors.Is(err, context.Canceled))
	require.NotEmpty(t, rec.facts)
	require.Equal(t, "context_canceled", rec.facts[len(rec.facts)-1].ErrorCode)
}

func TestIncidentRepair_RecordsVerifiedWhenConcurrentRepairAlreadyRestoredShard(t *testing.T) {
	b := newTestDistributedBackend(t)
	dir := t.TempDir()
	svc := NewShardService(dir, nil)
	b.SetShardService(svc, []string{"test-node", "other-a", "other-b", "other-c", "other-d", "other-e"})
	writePlacement(t, b, "b", "k/v1", []string{"test-node", "other-a", "other-b", "other-c", "other-d", "other-e"})
	require.NoError(t, svc.WriteLocalShard("b", "k/v1", 0, []byte("already-repaired")))

	rec := &recordingIncidentRecorder{}
	err := b.RepairShardLocalWithIncident(context.Background(), IncidentRepairRequest{
		Bucket: "b", Key: "k", VersionID: "v1", ShardIdx: 0, Recorder: rec, Now: time.Unix(100, 0).UTC(),
	})
	require.NoError(t, err)
	require.NotEmpty(t, rec.facts)
	require.Equal(t, incident.FactVerified, rec.facts[len(rec.facts)-1].Type)
	require.NotEqual(t, incident.FactActionFailed, rec.facts[len(rec.facts)-1].Type)
}

func TestIncidentRepair_ReceiptSignedRecordedOnlyAfterPersistCallback(t *testing.T) {
	b := newTestDistributedBackend(t)
	rec := &recordingIncidentRecorder{}
	req := IncidentRepairRequest{
		Bucket: "b", Key: "k", VersionID: "v1", ShardIdx: 0, Recorder: rec, CorrelationID: "cid-repair", Now: time.Unix(100, 0).UTC(),
	}

	require.NoError(t, b.RecordRepairReceiptSigned(context.Background(), req, "rcpt-cid-repair"))
	require.NotEmpty(t, rec.facts)
	require.Equal(t, incident.FactObserved, rec.facts[0].Type)
	require.Equal(t, incident.FactReceiptSigned, rec.facts[len(rec.facts)-1].Type)
	require.Equal(t, "rcpt-cid-repair", rec.facts[len(rec.facts)-1].ReceiptID)
}
