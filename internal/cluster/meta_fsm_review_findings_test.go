package cluster

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/raft"
)

// TestMetaFSM_Restore_RollbackOnJKEYCorruption verifies F6: if the JKEY
// trailer is corrupt, Restore returns an error AND f.icebergNamespaces is
// unchanged from before the call (no partial commit).
func TestMetaFSM_Restore_RollbackOnJKEYCorruption(t *testing.T) {
	// Build a source FSM with a JWT key so the snapshot carries a JKEY trailer.
	src, _ := newTestFSMWithDEK(t)
	applyDEKRotate(t, src)
	applyJWTRotate(t, src)

	// Also seed one Iceberg namespace via Snapshot/Restore of a separate FSM so
	// we can detect if icebergNamespaces is mutated on the target.
	// We insert directly via the apply path instead.
	if err := src.applyCmd(buildIcebergCreateNamespaceCmd(t, "warehouse1", []string{"ns1"})); err != nil {
		t.Fatalf("seed namespace: %v", err)
	}

	snap, err := src.Snapshot()
	require.NoError(t, err)

	// Corrupt the JKEY payload bytes while keeping the trailer length/magic intact.
	// Layout: [...JKEY payload...][u32 jkeyLen][u32 jkeyMagic].
	// We scramble the payload so trailer-peeling succeeds (length and magic are
	// valid) but decodeJWTKeyStore fails on garbage content.
	//
	// This is the key difference vs. a length-corruption test:
	//  - OLD broken Restore: commits f.icebergNamespaces at line 3056, then
	//    fails at decodeJWTKeyStore — partial state.
	//  - NEW fixed Restore: decodes all trailers first, decode fails → returns
	//    error, f.icebergNamespaces is never committed.
	if len(snap) < jkeySnapshotTrailerLen {
		t.Fatalf("snapshot too small to have JKEY trailer: %d bytes", len(snap))
	}
	// Verify JKEY magic is present.
	gotMagic := binary.LittleEndian.Uint32(snap[len(snap)-4:])
	require.Equal(t, uint32(jkeySnapshotTrailerMagic), gotMagic, "expected JKEY trailer magic")

	corrupted := make([]byte, len(snap))
	copy(corrupted, snap)
	// Scramble the JKEY payload bytes (leave length+magic intact).
	trailerEnd := len(corrupted) - jkeySnapshotTrailerLen
	jkeyLen := binary.LittleEndian.Uint32(corrupted[trailerEnd : trailerEnd+4])
	payloadStart := trailerEnd - int(jkeyLen)
	require.GreaterOrEqual(t, payloadStart, 0, "jkeyLen must be within snapshot bounds")
	for i := payloadStart; i < trailerEnd; i++ {
		corrupted[i] = 0xFF // garbage — FB decode will fail
	}

	// Target FSM: pre-populate icebergNamespaces with a sentinel value.
	dst, _ := newTestFSMWithDEK(t)
	sentinel := map[string]map[string]IcebergNamespaceEntry{
		"sentinel-warehouse": {"sentinel-ns": {Warehouse: "sentinel-warehouse", Namespace: []string{"sentinel-ns"}}},
	}
	dst.mu.Lock()
	dst.icebergNamespaces = sentinel
	dst.mu.Unlock()

	err = dst.Restore(raft.SnapshotMeta{}, corrupted)
	require.Error(t, err, "Restore with corrupt JKEY must return an error")

	// icebergNamespaces must still be the sentinel — not the src snapshot value.
	dst.mu.RLock()
	got := dst.icebergNamespaces
	dst.mu.RUnlock()
	_, hasSentinel := got["sentinel-warehouse"]
	assert.True(t, hasSentinel, "icebergNamespaces must be unchanged (sentinel) after failed Restore")
	_, hasSrcNS := got["warehouse1"]
	assert.False(t, hasSrcNS, "icebergNamespaces must NOT contain src namespace after failed Restore")
}

// TestMetaFSM_Restore_JKEYWithoutDEKKeeper_Errors verifies F9: a snapshot
// with a JKEY trailer cannot be restored into an FSM that has no DEK keeper
// wired — Restore must return an error.
func TestMetaFSM_Restore_JKEYWithoutDEKKeeper_Errors(t *testing.T) {
	// Build a snapshot that includes a JKEY trailer.
	src, _ := newTestFSMWithDEK(t)
	applyDEKRotate(t, src)
	applyJWTRotate(t, src)

	snap, err := src.Snapshot()
	require.NoError(t, err)

	// Verify JKEY trailer is present.
	require.GreaterOrEqual(t, len(snap), jkeySnapshotTrailerLen)
	gotMagic := binary.LittleEndian.Uint32(snap[len(snap)-4:])
	require.Equal(t, uint32(jkeySnapshotTrailerMagic), gotMagic)

	// Fresh FSM with NO DEK keeper wired.
	dst := NewMetaFSM()

	err = dst.Restore(raft.SnapshotMeta{}, snap)
	require.Error(t, err, "Restore with JKEY but no DEK keeper must error")
	assert.Contains(t, err.Error(), "DEK keeper not wired")
}

// TestMetaFSM_Restore_RejectsUnsupportedIcebergSchemaVersion verifies F10:
// a snapshot with an unknown iceberg_schema_version (e.g. 1) is rejected.
func TestMetaFSM_Restore_RejectsUnsupportedIcebergSchemaVersion(t *testing.T) {
	// Build a valid snapshot from a fresh FSM.
	src := NewMetaFSM()
	snap, err := src.Snapshot()
	require.NoError(t, err)

	// The snapshot starts with a FlatBuffers root table.  We need to mutate
	// the iceberg_schema_version field to an unsupported value (1).
	// FlatBuffers mutable accessor MutateIcebergSchemaVersion operates on the
	// root object in-place.
	snapMut := clusterpb.GetRootAsMetaStateSnapshot(snap, 0)
	ok := snapMut.MutateIcebergSchemaVersion(1)
	require.True(t, ok, "MutateIcebergSchemaVersion must succeed (field is a scalar)")

	dst := NewMetaFSM()
	err = dst.Restore(raft.SnapshotMeta{}, snap)
	require.Error(t, err, "Restore with iceberg_schema_version=1 must error")
	assert.Contains(t, err.Error(), "unsupported iceberg_schema_version=1")
}

// buildIcebergCreateNamespaceCmd builds a MetaCmd for creating an Iceberg namespace.
func buildIcebergCreateNamespaceCmd(t *testing.T, warehouse string, namespace []string) []byte {
	t.Helper()
	payload, err := encodeMetaIcebergCreateNamespaceCmd(IcebergCreateNamespaceCmd{
		RequestID: "test-req",
		Warehouse: warehouse,
		Namespace: namespace,
	})
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeIcebergCreateNamespace, payload)
	require.NoError(t, err)
	return cmd
}
