package volume

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManager_DeleteWithSnapshots_NoSnapshots(t *testing.T) {
	m := setupManager(t)
	if _, err := m.Create("v1", 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteWithSnapshots("v1"); err != nil {
		t.Fatalf("delete err = %v", err)
	}
	if _, err := m.Get("v1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get err = %v, want ErrNotFound", err)
	}
}

func TestManager_DeleteWithSnapshots_CascadesAllSnapshots(t *testing.T) {
	m := setupManager(t)
	if _, err := m.Create("v1", 1<<20); err != nil {
		t.Fatal(err)
	}
	// Write something so blocks exist for snapshots to capture.
	if _, err := m.WriteAt("v1", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := m.CreateSnapshot("v1"); err != nil {
			t.Fatal(err)
		}
	}
	snaps, err := m.ListSnapshots("v1")
	require.NoError(t, err)
	if len(snaps) != 3 {
		t.Fatalf("snapshots before = %d, want 3", len(snaps))
	}
	if err := m.DeleteWithSnapshots("v1"); err != nil {
		t.Fatalf("delete err = %v", err)
	}
	if _, err := m.Get("v1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get err = %v, want ErrNotFound", err)
	}
	if _, err := m.ListSnapshots("v1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ListSnapshots after delete err = %v, want ErrNotFound", err)
	}
}

func TestManager_DeleteWithSnapshots_NotFound(t *testing.T) {
	m := setupManager(t)
	err := m.DeleteWithSnapshots("ghost")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
