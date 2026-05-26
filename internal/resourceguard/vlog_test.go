package resourceguard

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/resourcewatch"
)

func TestRecordVlogDecision_WarnCreatesDiagnosedIncident(t *testing.T) {
	ctx := context.Background()
	store := newTestIncidentStore()
	recorder := incident.NewRecorder(store, incident.NewReducer())
	decision := &resourcewatch.Decision{
		Level:     resourcewatch.LevelWarn,
		Threshold: "warn",
		Ratio:     0.41,
		Message:   "vlog ratio 0.41/0.40",
		Snapshot: resourcewatch.Sample{
			Open:        4 << 30,
			Limit:       10 << 30,
			Categories:  map[resourcewatch.Category]int{resourcewatch.DBCategoryGroupRaft: 3 << 30, resourcewatch.DBCategoryMeta: 1 << 30},
			CollectedAt: time.Unix(100, 0).UTC(),
		},
	}
	require.NoError(t, recordVlogDecision(ctx, recorder, "node-1", decision))
	got, ok, err := store.Get(ctx, vlogIncidentID("node-1"))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, incident.StateDiagnosed, got.State)
	assert.Equal(t, incident.SeverityWarning, got.Severity)
	assert.Equal(t, incident.ActionResourceWarning, got.Action)
	assert.Contains(t, got.Decision, "top:")
	assert.Contains(t, got.Decision, "group-raft")
	assert.Equal(t, "node-1", got.Scope.NodeID)
}

func TestRecordVlogDecision_CriticalBlocksIncident(t *testing.T) {
	ctx := context.Background()
	store := newTestIncidentStore()
	recorder := incident.NewRecorder(store, incident.NewReducer())
	decision := &resourcewatch.Decision{
		Level:     resourcewatch.LevelCritical,
		Threshold: "critical",
		Ratio:     0.71,
		Message:   "vlog ratio 0.71/0.70",
		Snapshot:  resourcewatch.Sample{Open: 7 << 30, Limit: 10 << 30, CollectedAt: time.Unix(100, 0).UTC()},
	}
	require.NoError(t, recordVlogDecision(ctx, recorder, "node-1", decision))
	got, ok, err := store.Get(ctx, vlogIncidentID("node-1"))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, incident.StateBlocked, got.State)
	assert.Equal(t, incident.SeverityCritical, got.Severity)
}

func TestRecordVlogDecision_RecoveryFixesIncident(t *testing.T) {
	ctx := context.Background()
	store := newTestIncidentStore()
	recorder := incident.NewRecorder(store, incident.NewReducer())
	decision := &resourcewatch.Decision{
		Level:    resourcewatch.LevelOK,
		Message:  "vlog recovered below warn ratio",
		Snapshot: resourcewatch.Sample{Open: 1 << 28, Limit: 10 << 30, CollectedAt: time.Unix(100, 0).UTC()},
	}
	require.NoError(t, recordVlogDecision(ctx, recorder, "node-1", decision))
	got, ok, err := store.Get(ctx, vlogIncidentID("node-1"))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, incident.StateFixed, got.State)
	assert.Equal(t, incident.SeverityInfo, got.Severity)
}

func TestRecordBadgerGCFailedIncident_FiresOnce(t *testing.T) {
	ctx := context.Background()
	store := newTestIncidentStore()
	recorder := incident.NewRecorder(store, incident.NewReducer())
	recordBadgerGCFailedIncident(ctx, recorder, "node-1", resourcewatch.DBCategoryMeta, errAssertion)
	got, ok, err := store.Get(ctx, badgerGCIncidentID("node-1", resourcewatch.DBCategoryMeta))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, incident.CauseBadgerGCFailed, got.Cause)
	assert.Equal(t, incident.StateBlocked, got.State)
	assert.Contains(t, got.NextAction, "vlog reclaim")
}

func TestRecordSmokeUnderPopulatedIncident_FiresWithLiveList(t *testing.T) {
	ctx := context.Background()
	store := newTestIncidentStore()
	recorder := incident.NewRecorder(store, incident.NewReducer())
	live := []string{"/data/orphan-a", "/data/orphan-b"}
	recordSmokeUnderPopulatedIncident(ctx, recorder, "node-1", live)
	got, ok, err := store.Get(ctx, smokeIncidentID("node-1"))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, incident.CauseRegistryUnderPopulated, got.Cause)
	assert.Equal(t, incident.StateDiagnosed, got.State)
	assert.Contains(t, got.Decision, "/data/orphan-a")
}

func TestVlogDecisionMessageWithBreakdown_Top3Sorted(t *testing.T) {
	d := &resourcewatch.Decision{
		Message: "vlog ratio 0.41/0.40",
		Snapshot: resourcewatch.Sample{
			Categories: map[resourcewatch.Category]int{
				resourcewatch.DBCategoryGroupRaft: 5,
				resourcewatch.DBCategoryMeta:      4,
				resourcewatch.DBCategoryIncident:  3,
				resourcewatch.DBCategoryReceipts:  2,
				resourcewatch.DBCategoryStorage:   1,
			},
		},
	}
	out := vlogDecisionMessageWithBreakdown(d)
	assert.Contains(t, out, "top:")
	assert.Contains(t, out, "group-raft=5")
	assert.Contains(t, out, "meta=4")
	assert.Contains(t, out, "incident=3")
	assert.NotContains(t, out, "receipts")
	assert.NotContains(t, out, "storage")
}
