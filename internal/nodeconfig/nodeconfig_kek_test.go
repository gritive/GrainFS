package nodeconfig

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestKEKDir_DefaultsToDataDirKeys(t *testing.T) {
	t.Setenv("GRAINFS_KEK_DIR", "")
	nc := New("/some/data")
	if got, want := nc.KEKDir(), filepath.Join("/some/data", "keys"); got != want {
		t.Fatalf("KEKDir = %q, want %q", got, want)
	}
}

func TestKEKDir_HonorsEnvOverride(t *testing.T) {
	t.Setenv("GRAINFS_KEK_DIR", "/tmp/override-kek")
	nc := New("/some/data")
	if got, want := nc.KEKDir(), "/tmp/override-kek"; got != want {
		t.Fatalf("KEKDir = %q, want %q", got, want)
	}
}

func TestLoadOrInitClusterID_FreshGenerates16Bytes(t *testing.T) {
	dir := t.TempDir()
	nc := New(dir)
	id, err := nc.LoadOrInitClusterID()
	if err != nil {
		t.Fatalf("LoadOrInitClusterID: %v", err)
	}
	if len(id) != 16 {
		t.Errorf("cluster_id len = %d, want 16", len(id))
	}
	info, err := os.Stat(filepath.Join(dir, ClusterIDFile))
	if err != nil {
		t.Fatalf("stat cluster.id: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("cluster.id perm = %#o, want 0o600", perm)
	}
	// UUID v7: version nibble (high 4 bits of byte 6) must be 0x7.
	if (id[6] >> 4) != 0x7 {
		t.Errorf("cluster.id is not UUID v7 (byte[6]>>4 = %#x, want 0x7)", id[6]>>4)
	}
}

func TestLoadOrInitClusterID_ReloadStable(t *testing.T) {
	dir := t.TempDir()
	nc := New(dir)
	id1, err := nc.LoadOrInitClusterID()
	if err != nil {
		t.Fatalf("first load: %v", err)
	}
	id2, err := nc.LoadOrInitClusterID()
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if !bytes.Equal(id1, id2) {
		t.Errorf("cluster.id changed across reload: %x vs %x", id1, id2)
	}
}

func TestLoadOrInitClusterID_RejectsWrongSize(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ClusterIDFile), []byte{1, 2, 3}, 0o600); err != nil {
		t.Fatalf("write bad cluster.id: %v", err)
	}
	nc := New(dir)
	if _, err := nc.LoadOrInitClusterID(); err == nil {
		t.Errorf("LoadOrInitClusterID accepted a 3-byte cluster.id")
	}
}

func TestNodeConfig_LoadClusterID_RefuseMissing(t *testing.T) {
	dir := t.TempDir()
	nc := New(dir)
	_, err := nc.LoadClusterID()
	if err == nil {
		t.Fatal("LoadClusterID accepted missing file")
	}
	if !errors.Is(err, ErrClusterIDMissing) {
		t.Errorf("expected ErrClusterIDMissing, got %v", err)
	}
}

func TestNodeConfig_LoadClusterID_ReadExisting(t *testing.T) {
	dir := t.TempDir()
	nc := New(dir)
	// Init first, then strict-load
	id, err := nc.LoadOrInitClusterID()
	if err != nil {
		t.Fatalf("LoadOrInitClusterID: %v", err)
	}
	got, err := nc.LoadClusterID()
	if err != nil {
		t.Fatalf("LoadClusterID after init: %v", err)
	}
	if !bytes.Equal(id, got) {
		t.Errorf("LoadClusterID returned different bytes than init")
	}
}
