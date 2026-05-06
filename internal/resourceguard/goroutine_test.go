package resourceguard

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/resourcewatch"
)

func TestRecordGoroutineDecision_WarnCreatesDiagnosedIncident(t *testing.T) {
	ctx := context.Background()
	store := newTestIncidentStore()
	recorder := incident.NewRecorder(store, incident.NewReducer())
	decision := &resourcewatch.Decision{
		Level:     resourcewatch.LevelWarn,
		Threshold: "warn",
		Ratio:     0.30,
		Message:   "goroutines 6000/20000 crossed warn threshold",
		Snapshot:  resourcewatch.Sample{Open: 6000, Limit: 20000, CollectedAt: time.Unix(100, 0).UTC()},
	}

	require.NoError(t, recordGoroutineDecision(ctx, recorder, "node-1", decision))
	got, ok, err := store.Get(ctx, goroutineIncidentID("node-1"))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, incident.StateDiagnosed, got.State)
	assert.Equal(t, incident.SeverityWarning, got.Severity)
	assert.Equal(t, incident.ActionResourceWarning, got.Action)
	assert.Contains(t, got.Decision, "warn threshold")
	assert.Equal(t, "node-1", got.Scope.NodeID)
}

func TestRecordGoroutineDecision_CriticalBlocksIncident(t *testing.T) {
	ctx := context.Background()
	store := newTestIncidentStore()
	recorder := incident.NewRecorder(store, incident.NewReducer())
	decision := &resourcewatch.Decision{
		Level:     resourcewatch.LevelCritical,
		Threshold: "critical",
		Ratio:     1.0,
		Message:   "goroutines 22000/20000 crossed critical threshold",
		Snapshot:  resourcewatch.Sample{Open: 22000, Limit: 20000, CollectedAt: time.Unix(100, 0).UTC()},
	}

	require.NoError(t, recordGoroutineDecision(ctx, recorder, "node-1", decision))
	got, ok, err := store.Get(ctx, goroutineIncidentID("node-1"))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, incident.StateBlocked, got.State)
	assert.Equal(t, incident.SeverityCritical, got.Severity)
	assert.Contains(t, got.NextAction, "pprof")
}

func TestRecordGoroutineDecision_RecoveryFixesIncident(t *testing.T) {
	ctx := context.Background()
	store := newTestIncidentStore()
	recorder := incident.NewRecorder(store, incident.NewReducer())
	decision := &resourcewatch.Decision{
		Level:    resourcewatch.LevelOK,
		Message:  "goroutines recovered below warning threshold",
		Snapshot: resourcewatch.Sample{Open: 200, Limit: 20000, CollectedAt: time.Unix(100, 0).UTC()},
	}

	require.NoError(t, recordGoroutineDecision(ctx, recorder, "node-1", decision))
	got, ok, err := store.Get(ctx, goroutineIncidentID("node-1"))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, incident.StateFixed, got.State)
	assert.Equal(t, incident.SeverityInfo, got.Severity)
	assert.Equal(t, incident.ProofNotRequired, got.Proof.Status)
}

func TestRecordGoroutineMetrics_WritesAllGauges(t *testing.T) {
	nodeID := "goroutine-metric-node"
	recordGoroutineMetrics(nodeID, resourcewatch.Sample{
		Open:        500,
		Limit:       20000,
		CollectedAt: time.Unix(100, 0).UTC(),
	}, nil)
	assert.Equal(t, float64(500), testutil.ToFloat64(metrics.GoroutineCount.WithLabelValues(nodeID)))
	assert.Equal(t, float64(20000), testutil.ToFloat64(metrics.GoroutineLimit.WithLabelValues(nodeID)))
	assert.InDelta(t, 0.025, testutil.ToFloat64(metrics.GoroutineUsedRatio.WithLabelValues(nodeID)), 0.0001)
	assert.Equal(t, float64(-1), testutil.ToFloat64(metrics.GoroutineETASeconds.WithLabelValues(nodeID, "warn")))
	assert.Equal(t, float64(-1), testutil.ToFloat64(metrics.GoroutineETASeconds.WithLabelValues(nodeID, "critical")))
}

func TestRecordGoroutineMetrics_PopulatesETAOnDecision(t *testing.T) {
	nodeID := "goroutine-eta-node"
	decision := &resourcewatch.Decision{
		Level:     resourcewatch.LevelWarn,
		Threshold: "warn",
		ETA:       10 * time.Minute,
	}
	recordGoroutineMetrics(nodeID, resourcewatch.Sample{
		Open:        5500,
		Limit:       20000,
		CollectedAt: time.Unix(100, 0).UTC(),
	}, decision)
	assert.Equal(t, float64(600), testutil.ToFloat64(metrics.GoroutineETASeconds.WithLabelValues(nodeID, "warn")))
}
