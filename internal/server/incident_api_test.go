package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/incident"
)

type incidentStoreStub struct {
	list []incident.IncidentState
	byID map[string]incident.IncidentState
}

func (s *incidentStoreStub) Put(context.Context, incident.IncidentState) error { return nil }
func (s *incidentStoreStub) Get(_ context.Context, id string) (incident.IncidentState, bool, error) {
	v, ok := s.byID[id]
	return v, ok, nil
}
func (s *incidentStoreStub) List(context.Context, int) ([]incident.IncidentState, error) {
	return s.list, nil
}

func TestIncidentAPI_List(t *testing.T) {
	st := &incidentStoreStub{list: []incident.IncidentState{{ID: "cid", State: incident.StateFixed, UpdatedAt: time.Unix(100, 0).UTC()}}}
	base := setupTestServerWithOptions(t, WithIncidentStore(st))

	resp, err := http.Get(base + "/api/incidents?limit=10")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got []incident.IncidentState
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Len(t, got, 1)
	assert.Equal(t, "cid", got[0].ID)
}

func TestIncidentAPI_GetMissing(t *testing.T) {
	st := &incidentStoreStub{byID: map[string]incident.IncidentState{}}
	base := setupTestServerWithOptions(t, WithIncidentStore(st))

	resp, err := http.Get(base + "/api/incidents/missing")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
