package main

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/resourcewatch"
)

type testIncidentStore struct {
	states map[string]incident.IncidentState
}

func newTestIncidentStore() *testIncidentStore {
	return &testIncidentStore{states: map[string]incident.IncidentState{}}
}

func (s *testIncidentStore) Put(_ context.Context, state incident.IncidentState) error {
	s.states[state.ID] = state
	return nil
}

func (s *testIncidentStore) Get(_ context.Context, id string) (incident.IncidentState, bool, error) {
	state, ok := s.states[id]
	return state, ok, nil
}

func (s *testIncidentStore) List(_ context.Context, _ int) ([]incident.IncidentState, error) {
	out := make([]incident.IncidentState, 0, len(s.states))
	for _, state := range s.states {
		out = append(out, state)
	}
	return out, nil
}

func TestFDWatchFlagDefault(t *testing.T) {
	flag := serveCmd.Flags().Lookup("fd-watch-enabled")
	require.NotNil(t, flag)
	assert.Equal(t, "true", flag.DefValue)
}

func TestFDWatchEnabledHelper(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().Bool("fd-watch-enabled", true, "")
	assert.True(t, fdWatchEnabled(cmd))
	require.NoError(t, cmd.Flags().Set("fd-watch-enabled", "false"))
	assert.False(t, fdWatchEnabled(cmd))
}

func TestRecordFDDecision_WarnCreatesDiagnosedIncident(t *testing.T) {
	ctx := context.Background()
	store := newTestIncidentStore()
	recorder := incident.NewRecorder(store, incident.NewReducer())
	decision := &resourcewatch.Decision{
		Level:     resourcewatch.LevelWarn,
		Threshold: "warn",
		Ratio:     0.85,
		Message:   "FD usage 85.0% crossed warn threshold; top categories: socket=10",
		Snapshot:  resourcewatch.Sample{Open: 850, Limit: 1000, CollectedAt: time.Unix(100, 0).UTC()},
	}

	require.NoError(t, recordFDDecision(ctx, recorder, "node-1", decision))
	got, ok, err := store.Get(ctx, fdIncidentID("node-1"))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, incident.StateDiagnosed, got.State)
	assert.Equal(t, incident.SeverityWarning, got.Severity)
	assert.Equal(t, incident.ActionResourceWarning, got.Action)
	assert.Contains(t, got.Decision, "85.0%")
	assert.Equal(t, "node-1", got.Scope.NodeID)
}

func TestRecordFDDecision_CriticalBlocksIncident(t *testing.T) {
	ctx := context.Background()
	store := newTestIncidentStore()
	recorder := incident.NewRecorder(store, incident.NewReducer())
	decision := &resourcewatch.Decision{
		Level:     resourcewatch.LevelCritical,
		Threshold: "critical",
		Ratio:     0.93,
		Message:   "FD usage 93.0% crossed critical threshold",
		Snapshot:  resourcewatch.Sample{Open: 930, Limit: 1000, CollectedAt: time.Unix(100, 0).UTC()},
	}

	require.NoError(t, recordFDDecision(ctx, recorder, "node-1", decision))
	got, ok, err := store.Get(ctx, fdIncidentID("node-1"))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, incident.StateBlocked, got.State)
	assert.Equal(t, incident.SeverityCritical, got.Severity)
	assert.Contains(t, got.NextAction, "LimitNOFILE")
}

func TestRecordFDDecision_RecoveryFixesIncident(t *testing.T) {
	ctx := context.Background()
	store := newTestIncidentStore()
	recorder := incident.NewRecorder(store, incident.NewReducer())
	decision := &resourcewatch.Decision{
		Level:    resourcewatch.LevelOK,
		Message:  "FD usage recovered below warning threshold",
		Snapshot: resourcewatch.Sample{Open: 500, Limit: 1000, CollectedAt: time.Unix(100, 0).UTC()},
	}

	require.NoError(t, recordFDDecision(ctx, recorder, "node-1", decision))
	got, ok, err := store.Get(ctx, fdIncidentID("node-1"))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, incident.StateFixed, got.State)
	assert.Equal(t, incident.SeverityInfo, got.Severity)
	assert.Equal(t, incident.ProofNotRequired, got.Proof.Status)
}

func TestRecordFDMetrics_ClearsMissingCategories(t *testing.T) {
	nodeID := "metric-clear-node"
	recordFDMetrics(nodeID, resourcewatch.Sample{
		Open:       10,
		Limit:      100,
		Categories: map[resourcewatch.Category]int{resourcewatch.FDCategorySocket: 7},
	}, nil)
	assert.Equal(t, float64(7), testutil.ToFloat64(metrics.FDOpenByCategory.WithLabelValues(nodeID, string(resourcewatch.FDCategorySocket))))

	recordFDMetrics(nodeID, resourcewatch.Sample{
		Open:       3,
		Limit:      100,
		Categories: map[resourcewatch.Category]int{resourcewatch.FDCategoryBadger: 2},
	}, nil)

	assert.Equal(t, float64(0), testutil.ToFloat64(metrics.FDOpenByCategory.WithLabelValues(nodeID, string(resourcewatch.FDCategorySocket))))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.FDOpenByCategory.WithLabelValues(nodeID, string(resourcewatch.FDCategoryBadger))))
}

func TestGoroutineWatchFlagDefault(t *testing.T) {
	flag := serveCmd.Flags().Lookup("goroutine-watch-enabled")
	require.NotNil(t, flag)
	assert.Equal(t, "true", flag.DefValue)
}

func TestGoroutineWatchEnabledHelper(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().Bool("goroutine-watch-enabled", true, "")
	assert.True(t, goroutineWatchEnabled(cmd))
	require.NoError(t, cmd.Flags().Set("goroutine-watch-enabled", "false"))
	assert.False(t, goroutineWatchEnabled(cmd))
}

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
