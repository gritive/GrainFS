package resourceguard

import (
	"context"

	"github.com/gritive/GrainFS/internal/incident"
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

type assertionError string

func (e assertionError) Error() string { return string(e) }

var errAssertion = assertionError("RunValueLogGC failed")
