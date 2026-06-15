package admin_test

import (
	"context"
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
