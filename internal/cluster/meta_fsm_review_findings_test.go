package cluster

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestMetaFSM_Restore_RollbackOnJKEYCorruption verifies F6: if the JKEY
// trailer is corrupt, Restore returns an error and no partial state is committed.
func TestMetaFSM_Restore_RollbackOnJKEYCorruption(t *testing.T) {
	// Build a source FSM with a JWT key so the snapshot carries a JKEY trailer.
	src, _ := newTestFSMWithDEK(t)
	applyDEKRotate(t, src)
	applyJWTRotate(t, src)

	sealed, err := src.Snapshot()
	require.NoError(t, err)
	// Phase D-snap: corrupt the plaintext body, then re-seal so Restore decrypts
	// to the corrupted inner snapshot and fails at the JKEY decode (not the
	// envelope layer).
	snap, err := src.openSnapshotEnvelope(sealed)
	require.NoError(t, err)

	// Corrupt the JKEY payload bytes while keeping the trailer length/magic intact.
	// Layout: [...JKEY payload...][u32 jkeyLen][u32 jkeyMagic].
	// We scramble the payload so trailer-peeling succeeds (length and magic are
	// valid) but decodeJWTKeyStore fails on garbage content.
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

	// Re-seal the corrupted plaintext so Restore decrypts to it.
	sealedCorrupted, err := src.sealSnapshotEnvelope(corrupted)
	require.NoError(t, err)

	dst, _ := newTestFSMWithDEK(t)
	err = dst.Restore(raft.SnapshotMeta{}, sealedCorrupted)
	require.Error(t, err, "Restore with corrupt JKEY must return an error")
}

// TestMetaFSM_Restore_JKEYWithoutDEKKeeper_Errors verifies F9: a snapshot
// with a JKEY trailer cannot be restored into an FSM that has no DEK keeper
// wired — Restore must return an error.
func TestMetaFSM_Restore_JKEYWithoutDEKKeeper_Errors(t *testing.T) {
	// Build a snapshot that includes a JKEY trailer.
	src, _ := newTestFSMWithDEK(t)
	applyDEKRotate(t, src)
	applyJWTRotate(t, src)

	sealed, err := src.Snapshot()
	require.NoError(t, err)

	// Verify JKEY trailer is present in the decrypted plaintext.
	plain, err := src.openSnapshotEnvelope(sealed)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(plain), jkeySnapshotTrailerLen)
	gotMagic := binary.LittleEndian.Uint32(plain[len(plain)-4:])
	require.Equal(t, uint32(jkeySnapshotTrailerMagic), gotMagic)

	// Fresh FSM with NO DEK keeper wired, but a KEK store so the envelope opens
	// before Restore reaches the JKEY-without-keeper check. clusterID matches
	// src (dekTestClusterID == wireTestKEK's byte(i+1)).
	dst := NewMetaFSM()
	wireTestKEK(t, dst)

	err = dst.Restore(raft.SnapshotMeta{}, sealed)
	require.Error(t, err, "Restore with JKEY but no DEK keeper must error")
	assert.Contains(t, err.Error(), "DEK keeper not wired")
}
