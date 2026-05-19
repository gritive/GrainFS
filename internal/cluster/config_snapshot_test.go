package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/raft"
)

// snapshotMetaFSMToBytes calls fsm.Snapshot() and returns the bytes.
func snapshotMetaFSMToBytes(t *testing.T, fsm *MetaFSM) ([]byte, error) {
	t.Helper()
	return fsm.Snapshot()
}

// restoreMetaFSMFromBytes calls fsm.Restore() with the given bytes.
func restoreMetaFSMFromBytes(t *testing.T, fsm *MetaFSM, data []byte) error {
	t.Helper()
	return fsm.Restore(raft.SnapshotMeta{}, data)
}

// buildLegacySnapshotBytes builds a snapshot without a GCFG trailer by taking a
// snapshot of a fresh MetaFSM with no config values set (cfgStore.Snapshot()
// returns an empty map → "skip append on empty map" rule fires → no GCFG bytes).
func buildLegacySnapshotBytes(t *testing.T) []byte {
	t.Helper()
	store := config.NewStore()
	config.RegisterClusterKeys(store, config.ReloadHooks{})
	// Do NOT Set any values — empty map means no GCFG trailer is appended.
	fsm := newTestMetaFSMWithConfigStore(t, store)
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("buildLegacySnapshotBytes: Snapshot: %v", err)
	}
	return snap
}

// TestSnapshot_RestoreConfigValues verifies that config values survive a
// Snapshot → Restore round-trip via the GCFG trailer.
func TestSnapshot_RestoreConfigValues(t *testing.T) {
	store := config.NewStore()
	config.RegisterClusterKeys(store, config.ReloadHooks{})
	if err := store.Set(context.Background(), "audit.deny-only", "true"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	fsm1 := newTestMetaFSMWithConfigStore(t, store)
	buf, err := snapshotMetaFSMToBytes(t, fsm1)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	store2 := config.NewStore()
	config.RegisterClusterKeys(store2, config.ReloadHooks{})
	fsm2 := newTestMetaFSMWithConfigStore(t, store2)
	if err := restoreMetaFSMFromBytes(t, fsm2, buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if v, _ := store2.GetBool("audit.deny-only"); !v {
		t.Fatalf("after restore, value = %v, want true", v)
	}
}

// TestSnapshot_LegacyWithoutConfigTrailer verifies that snapshots without a GCFG
// trailer (pre-Task-10) still restore cleanly and leave the config store empty.
func TestSnapshot_LegacyWithoutConfigTrailer(t *testing.T) {
	legacyBytes := buildLegacySnapshotBytes(t)

	store := config.NewStore()
	config.RegisterClusterKeys(store, config.ReloadHooks{})
	fsm := newTestMetaFSMWithConfigStore(t, store)
	if err := restoreMetaFSMFromBytes(t, fsm, legacyBytes); err != nil {
		t.Fatalf("legacy restore: %v", err)
	}
	// store should be empty — GetBool returns default (false).
	if v, _ := store.GetBool("audit.deny-only"); v {
		t.Fatalf("legacy restore should not populate config; got %v", v)
	}
}
