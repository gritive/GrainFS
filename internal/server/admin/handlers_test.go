package admin_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
	"github.com/stretchr/testify/require"
)

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

func TestDeleteVolume_RefusesWithSnapshots(t *testing.T) {
	d := newDeps(t)
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)
	_, err = d.Manager.WriteAt("v1", []byte("x"), 0)
	require.NoError(t, err)
	_, err = d.Manager.CreateSnapshot("v1")
	require.NoError(t, err)

	_, err = admin.DeleteVolume(context.Background(), d, "v1", false)
	var ae *admin.Error
	if !errors.As(err, &ae) || ae.Code != "conflict" {
		t.Fatalf("err = %v, want conflict", err)
	}
	det, ok := ae.Details.(map[string]any)
	if !ok {
		t.Fatalf("details type = %T, want map", ae.Details)
	}
	if det["snapshot_count"].(int) != 1 {
		t.Fatalf("snapshot_count = %v, want 1", det["snapshot_count"])
	}
	if det["cascade_command"] != "grainfs volume delete v1 --force" {
		t.Fatalf("cascade_command = %v", det["cascade_command"])
	}
}

func TestDeleteVolume_ForceCascades(t *testing.T) {
	d := newDeps(t)
	_, err := admin.CreateVolume(context.Background(), d, admin.CreateVolumeReq{Name: "v1", Size: 1 << 20})
	require.NoError(t, err)
	_, err = d.Manager.WriteAt("v1", []byte("x"), 0)
	require.NoError(t, err)
	_, err = d.Manager.CreateSnapshot("v1")
	require.NoError(t, err)

	resp, err := admin.DeleteVolume(context.Background(), d, "v1", true)
	require.NoError(t, err)
	if !resp.Deleted {
		t.Fatal("Deleted = false")
	}
	if _, err := d.Manager.Get("v1"); err == nil {
		t.Fatal("volume still present after force delete")
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
