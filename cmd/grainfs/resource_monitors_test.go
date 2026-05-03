package main

import (
	"context"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/incident"
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
		Level:     resourcewatch.FDLevelWarn,
		Threshold: "warn",
		Ratio:     0.85,
		Message:   "FD usage 85.0% crossed warn threshold; top categories: socket=10",
		Snapshot:  resourcewatch.FDSnapshot{Open: 850, Limit: 1000, CollectedAt: time.Unix(100, 0).UTC()},
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
		Level:     resourcewatch.FDLevelCritical,
		Threshold: "critical",
		Ratio:     0.93,
		Message:   "FD usage 93.0% crossed critical threshold",
		Snapshot:  resourcewatch.FDSnapshot{Open: 930, Limit: 1000, CollectedAt: time.Unix(100, 0).UTC()},
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
		Level:    resourcewatch.FDLevelOK,
		Message:  "FD usage recovered below warning threshold",
		Snapshot: resourcewatch.FDSnapshot{Open: 500, Limit: 1000, CollectedAt: time.Unix(100, 0).UTC()},
	}

	require.NoError(t, recordFDDecision(ctx, recorder, "node-1", decision))
	got, ok, err := store.Get(ctx, fdIncidentID("node-1"))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, incident.StateFixed, got.State)
	assert.Equal(t, incident.SeverityInfo, got.Severity)
	assert.Equal(t, incident.ProofNotRequired, got.Proof.Status)
}
