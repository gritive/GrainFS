package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
	"github.com/stretchr/testify/require"
)

type listOnlyIncidentStore struct {
	states []incident.IncidentState
	err    error
}

func (s listOnlyIncidentStore) Put(context.Context, incident.IncidentState) error { return nil }
func (s listOnlyIncidentStore) Get(context.Context, string) (incident.IncidentState, bool, error) {
	return incident.IncidentState{}, false, nil
}
func (s listOnlyIncidentStore) List(context.Context, int) ([]incident.IncidentState, error) {
	return s.states, s.err
}

func newDeps(t *testing.T) *admin.Deps {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	tok, err := dashboard.Open(filepath.Join(dir, "dashboard.token"))
	require.NoError(t, err)
	return &admin.Deps{
		Manager:   volume.NewManager(backend),
		Token:     tok,
		PublicURL: "https://node1:9000",
		NodeID:    "test-node",
	}
}

func TestListVolumes_Empty(t *testing.T) {
	d := newDeps(t)
	resp, err := admin.ListVolumes(context.Background(), d)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(resp.Volumes) != 0 {
		t.Fatalf("len = %d, want 0", len(resp.Volumes))
	}
}

func TestCreateVolume_HappyPath(t *testing.T) {
	d := newDeps(t)
	v, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)
	if v.Name != "v1" || v.Size != 1<<20 {
		t.Fatalf("unexpected info %+v", v)
	}
}

func TestListVolumes_HealthOKWhenIncidentStoreHasNoMatchingIncidents(t *testing.T) {
	d := newDeps(t)
	d.Incident = listOnlyIncidentStore{}
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)

	resp, err := admin.ListVolumes(context.Background(), d)
	require.NoError(t, err)
	require.Len(t, resp.Volumes, 1)
	require.Equal(t, "ok", resp.Volumes[0].Health)
	require.Empty(t, resp.Volumes[0].HealthReasons)
}

func TestListVolumes_HealthCriticalForUnfixedVolumeIncident(t *testing.T) {
	d := newDeps(t)
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)
	d.Incident = listOnlyIncidentStore{states: []incident.IncidentState{{
		ID:        "inc-1",
		State:     incident.StateBlocked,
		Severity:  incident.SeverityCritical,
		Cause:     incident.CauseCorruptBlob,
		Scope:     incident.Scope{Bucket: volume.VolumeBucketName, Key: volume.BlockKeyPrefix("v1") + "0000000000000000"},
		UpdatedAt: time.Now(),
	}}}

	resp, err := admin.ListVolumes(context.Background(), d)
	require.NoError(t, err)
	require.Len(t, resp.Volumes, 1)
	require.Equal(t, "critical", resp.Volumes[0].Health)
	require.Equal(t, []string{"recent_incident"}, resp.Volumes[0].HealthReasons)
}

func TestGetVolume_HealthMatchesListForUnfixedVolumeIncident(t *testing.T) {
	d := newDeps(t)
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)
	d.Incident = listOnlyIncidentStore{states: []incident.IncidentState{{
		ID:        "inc-1",
		State:     incident.StateBlocked,
		Severity:  incident.SeverityCritical,
		Cause:     incident.CauseCorruptBlob,
		Scope:     incident.Scope{Bucket: volume.VolumeBucketName, Key: volume.BlockKeyPrefix("v1") + "0000000000000000"},
		UpdatedAt: time.Now(),
	}}}

	resp, err := admin.GetVolume(context.Background(), d, "v1")
	require.NoError(t, err)
	require.Equal(t, "critical", resp.Health)
	require.Equal(t, []string{"recent_incident"}, resp.HealthReasons)
}

func TestStatVolume_HealthCriticalForUnfixedVolumeIncident(t *testing.T) {
	d := newDeps(t)
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)
	d.Incident = listOnlyIncidentStore{states: []incident.IncidentState{{
		ID:        "inc-1",
		State:     incident.StateBlocked,
		Severity:  incident.SeverityCritical,
		Cause:     incident.CauseCorruptBlob,
		Scope:     incident.Scope{Bucket: volume.VolumeBucketName, Key: volume.BlockKeyPrefix("v1") + "0000000000000000"},
		UpdatedAt: time.Now(),
	}}}

	resp, err := admin.StatVolume(context.Background(), d, "v1")
	require.NoError(t, err)
	require.Equal(t, "critical", resp.Volume.Health)
	require.Equal(t, []string{"recent_incident"}, resp.Volume.HealthReasons)
	require.Len(t, resp.RecentIncidents, 1)
	require.Equal(t, "inc-1", resp.RecentIncidents[0]["id"])

	buf, err := json.Marshal(resp)
	require.NoError(t, err)
	require.Contains(t, string(buf), `"recent_incidents"`)
	require.Contains(t, string(buf), `"observed_at"`)
	require.NotContains(t, string(buf), `"ObservedAt"`)
}

func TestListVolumes_HealthUnknownWhenIncidentLookupFails(t *testing.T) {
	d := newDeps(t)
	d.Incident = listOnlyIncidentStore{err: errors.New("incident db unavailable")}
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)

	resp, err := admin.ListVolumes(context.Background(), d)
	require.NoError(t, err)
	require.Len(t, resp.Volumes, 1)
	require.Equal(t, "unknown", resp.Volumes[0].Health)
	require.Equal(t, []string{"incident_lookup_failed"}, resp.Volumes[0].HealthReasons)
}

func TestCreateVolume_DuplicateConflict(t *testing.T) {
	d := newDeps(t)
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)
	_, err = admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	var ae *admin.Error
	if !errors.As(err, &ae) || ae.Code != "conflict" {
		t.Fatalf("err = %v, want conflict", err)
	}
}

func TestCreateVolume_InvalidSize(t *testing.T) {
	d := newDeps(t)
	for _, sz := range []int64{0, -1} {
		_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: sz})
		var ae *admin.Error
		if !errors.As(err, &ae) || ae.Code != "invalid" {
			t.Fatalf("size=%d err = %v, want invalid", sz, err)
		}
	}
}

func TestGetVolume_NotFound(t *testing.T) {
	d := newDeps(t)
	_, err := admin.GetVolume(context.Background(), d, "ghost")
	var ae *admin.Error
	if !errors.As(err, &ae) || ae.Code != "not_found" {
		t.Fatalf("err = %v, want not_found", err)
	}
}

func TestDeleteVolume_HappyPath(t *testing.T) {
	d := newDeps(t)
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)
	_, err = d.Manager.WriteAt("v1", []byte("x"), 0)
	require.NoError(t, err)

	resp, err := admin.DeleteVolume(context.Background(), d, "v1")
	require.NoError(t, err)
	if !resp.Deleted {
		t.Fatal("Deleted = false")
	}
	if _, err := d.Manager.Get("v1"); err == nil {
		t.Fatal("volume still present after delete")
	}
}

func TestResizeVolume_GrowOK(t *testing.T) {
	d := newDeps(t)
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)
	resp, err := admin.ResizeVolume(context.Background(), d, "v1", admin.ResizeReq{Size: 1 << 21})
	require.NoError(t, err)
	if !resp.Changed {
		t.Fatal("Changed = false on grow")
	}
}

func TestResizeVolume_ShrinkUnsupported(t *testing.T) {
	d := newDeps(t)
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 21})
	require.NoError(t, err)
	_, err = admin.ResizeVolume(context.Background(), d, "v1", admin.ResizeReq{Size: 1 << 20})
	var ae *admin.Error
	if !errors.As(err, &ae) || ae.Code != "unsupported" {
		t.Fatalf("err = %v, want unsupported", err)
	}
}

func TestResizeVolume_EqualNoOp(t *testing.T) {
	d := newDeps(t)
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)
	resp, err := admin.ResizeVolume(context.Background(), d, "v1", admin.ResizeReq{Size: 1 << 20})
	require.NoError(t, err)
	if resp.Changed {
		t.Fatal("Changed = true on equal size")
	}
}

func TestStatVolume_NoIncidentStore(t *testing.T) {
	d := newDeps(t)
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)
	resp, err := admin.StatVolume(context.Background(), d, "v1")
	require.NoError(t, err)
	if resp.Volume.Name != "v1" {
		t.Fatalf("Volume.Name = %q", resp.Volume.Name)
	}
	if len(resp.RecentIncidents) != 0 {
		t.Fatalf("expected no incidents (nil store), got %d", len(resp.RecentIncidents))
	}
}

func TestGetDashboardToken(t *testing.T) {
	d := newDeps(t)
	resp, err := admin.GetDashboardToken(context.Background(), d)
	require.NoError(t, err)
	if resp.URL == "" || resp.Token == "" {
		t.Fatal("empty URL/Token")
	}
	if !resp.PublicURLSet {
		t.Fatal("PublicURLSet should be true (we set PublicURL)")
	}
}

func TestRotateDashboardToken_ChangesValue(t *testing.T) {
	d := newDeps(t)
	r1, err := admin.GetDashboardToken(context.Background(), d)
	require.NoError(t, err)
	r2, err := admin.RotateDashboardToken(context.Background(), d)
	require.NoError(t, err)
	if r1.Token == r2.Token {
		t.Fatal("rotate did not change token")
	}
}
