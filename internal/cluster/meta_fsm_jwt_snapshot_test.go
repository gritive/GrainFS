package cluster

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/raft"
)

// newTestFSMWithDEK creates a MetaFSM with a fresh DEKKeeper wired.
func newTestFSMWithDEK(t *testing.T) (*MetaFSM, *encrypt.DEKKeeper) {
	t.Helper()
	kek := make([]byte, 32)
	_, err := rand.Read(kek)
	require.NoError(t, err)
	keeper, err := encrypt.NewDEKKeeper(kek, dekTestClusterID())
	require.NoError(t, err)
	fsm := NewMetaFSM()
	fsm.SetDEKKeeper(keeper)
	return fsm, keeper
}

// applyDEKRotate applies a DEKRotate command to the FSM.
func applyDEKRotate(t *testing.T, fsm *MetaFSM) {
	t.Helper()
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeDEKRotate, nil)
	require.NoError(t, fsm.applyCmd(cmd))
}

// buildJWTRotatePayload builds a deterministic JWTSigningKeyRotate payload using
// the keeper from fsm. All random material (secret, wrapped, kid) is generated
// here (proposer side) so the FSM apply path receives only deterministic bytes.
func buildJWTRotatePayload(t *testing.T, keeper *encrypt.DEKKeeper) []byte {
	t.Helper()
	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	require.NoError(t, err)
	wrapped, gen, err := keeper.Seal(secret)
	require.NoError(t, err)
	kid, err := iamjwt.NewKid()
	require.NoError(t, err)
	return EncodeMetaJWTSigningKeyRotateCmd(kid, wrapped, gen, time.Now().Unix())
}

// applyJWTRotate applies a JWTSigningKeyRotate command to the FSM.
func applyJWTRotate(t *testing.T, fsm *MetaFSM) {
	t.Helper()
	payload := buildJWTRotatePayload(t, fsm.dekKeeper)
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeJWTSigningKeyRotate, payload)
	require.NoError(t, fsm.applyCmd(cmd))
}

// applyJWTPrune applies a JWTSigningKeyPrune command to the FSM.
func applyJWTPrune(t *testing.T, fsm *MetaFSM) error {
	t.Helper()
	payload := EncodeMetaJWTSigningKeyPruneCmd(time.Now().Unix())
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeJWTSigningKeyPrune, payload)
	return fsm.applyCmd(cmd)
}

// TestMetaFSM_JKEY_Roundtrip populates JWT keys via two JWTSigningKeyRotate
// commands (so we have both current and previous), snapshots, restores on a
// new FSM, and verifies the key store matches.
func TestMetaFSM_JKEY_Roundtrip(t *testing.T) {
	src, _ := newTestFSMWithDEK(t)
	// First: DEKRotate to have a non-zero DEK gen available.
	applyDEKRotate(t, src)
	// First JWT rotate creates current.
	applyJWTRotate(t, src)
	// Second JWT rotate demotes first to previous, creates new current.
	applyJWTRotate(t, src)

	srcCurrent, srcPrevious := src.jwtKeyStore.Snapshot()
	require.NotNil(t, srcCurrent, "expected current key after two rotates")
	require.NotNil(t, srcPrevious, "expected previous key after two rotates")
	require.NotEqual(t, srcCurrent.Kid, srcPrevious.Kid)

	snap, err := src.Snapshot()
	require.NoError(t, err)

	// Restore into a new FSM with the same DEK keeper.
	dst := NewMetaFSM()
	dst.SetDEKKeeper(src.dekKeeper)
	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, snap))

	dstCurrent, dstPrevious := dst.jwtKeyStore.Snapshot()
	require.NotNil(t, dstCurrent)
	require.NotNil(t, dstPrevious)
	assert.Equal(t, srcCurrent.Kid, dstCurrent.Kid)
	assert.Equal(t, srcCurrent.WrappedSecret, dstCurrent.WrappedSecret)
	assert.Equal(t, srcCurrent.DekGen, dstCurrent.DekGen)
	assert.Equal(t, srcPrevious.Kid, dstPrevious.Kid)
	assert.Equal(t, srcPrevious.WrappedSecret, dstPrevious.WrappedSecret)
	assert.Equal(t, srcPrevious.DekGen, dstPrevious.DekGen)
	assert.Equal(t, srcPrevious.DemotedAt, dstPrevious.DemotedAt)
}

// TestMetaFSM_LegacySnapshot_NoJKEY verifies that a snapshot from a fresh FSM
// with no JWT keys restores cleanly and leaves the key store empty.
func TestMetaFSM_LegacySnapshot_NoJKEY(t *testing.T) {
	src := NewMetaFSM()
	snap, err := src.Snapshot()
	require.NoError(t, err)

	dst := NewMetaFSM()
	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, snap))

	gotCurrent, gotPrevious := dst.jwtKeyStore.Snapshot()
	assert.Nil(t, gotCurrent, "expected no current key in legacy snapshot")
	assert.Nil(t, gotPrevious, "expected no previous key in legacy snapshot")
}

// TestMetaFSM_JWTRotate_AdvancesGeneration applies DEKRotate then two
// JWTSigningKeyRotate commands and verifies both current and previous exist
// with distinct kid values.
func TestMetaFSM_JWTRotate_AdvancesGeneration(t *testing.T) {
	fsm, _ := newTestFSMWithDEK(t)
	applyDEKRotate(t, fsm)
	applyJWTRotate(t, fsm)
	applyJWTRotate(t, fsm)

	current, previous := fsm.jwtKeyStore.Snapshot()
	require.NotNil(t, current, "current key must exist after two rotates")
	require.NotNil(t, previous, "previous key must exist after two rotates")
	assert.NotEqual(t, current.Kid, previous.Kid, "current and previous must have distinct kids")
}

// TestMetaFSM_JWTPrune_RefusesWhenLive applies DEKRotate + two JWTSigningKeyRotate
// (creating current + previous), then immediately applies JWTSigningKeyPrune. Since
// DemotedAt is time.Now(), the prune must be refused (within MaxJWTTokenTTL window).
func TestMetaFSM_JWTPrune_RefusesWhenLive(t *testing.T) {
	fsm, _ := newTestFSMWithDEK(t)
	applyDEKRotate(t, fsm)
	applyJWTRotate(t, fsm)
	applyJWTRotate(t, fsm)

	// Verify previous exists before prune attempt.
	_, prev := fsm.jwtKeyStore.Snapshot()
	require.NotNil(t, prev, "previous key must exist before prune test")

	err := applyJWTPrune(t, fsm)
	require.ErrorIs(t, err, iamjwt.ErrPrunePrev, "prune must be refused when previous was just demoted")
}

// TestMetaFSM_JWTPrune_SucceedsAfterTTL verifies that prune succeeds when the
// previous key's DemotedAt is older than MaxJWTTokenTTL.
func TestMetaFSM_JWTPrune_SucceedsAfterTTL(t *testing.T) {
	fsm, _ := newTestFSMWithDEK(t)
	applyDEKRotate(t, fsm)
	applyJWTRotate(t, fsm)
	applyJWTRotate(t, fsm)

	// Backdate the previous key's DemotedAt so PrunePrevSafe returns true.
	fsm.jwtKeyStore.mu.Lock()
	if fsm.jwtKeyStore.previous != nil {
		fsm.jwtKeyStore.previous.DemotedAt = time.Now().Add(-2 * time.Hour).Unix()
	}
	fsm.jwtKeyStore.mu.Unlock()

	err := applyJWTPrune(t, fsm)
	require.NoError(t, err, "prune must succeed after TTL window")

	_, prev := fsm.jwtKeyStore.Snapshot()
	assert.Nil(t, prev, "previous key must be nil after successful prune")
}

// TestMetaFSM_Restore_RollbackOnJKEYLoadSeedFailure is an F14 regression test.
// When a JKEY trailer is present in a snapshot but the restoring FSM's DEK keeper
// uses a different KEK (so dekKeeper.Open fails), Restore must:
//  1. Return a non-nil error.
//  2. Leave f.jwtKeyStore untouched (both slots nil on a fresh FSM).
//
// The bug (pre-F14) was that f.jwtKeyStore.ReplaceAll was called BEFORE
// LoadFromSeeds, so a failure half-way through the unwrap loop would leave
// f.jwtKeyStore updated but f.jwtKeys empty → split state.
func TestMetaFSM_Restore_RollbackOnJKEYLoadSeedFailure(t *testing.T) {
	// Build a source FSM with KEK-A and rotate a JWT key.
	src, _ := newTestFSMWithDEK(t)
	applyDEKRotate(t, src)
	applyJWTRotate(t, src)

	snap, err := src.Snapshot()
	require.NoError(t, err)

	// Build a destination FSM with a DIFFERENT KEK — Open will fail.
	wrongKEK := make([]byte, 32)
	_, err = rand.Read(wrongKEK)
	require.NoError(t, err)
	wrongKeeper, err := encrypt.NewDEKKeeper(wrongKEK, dekTestClusterID())
	require.NoError(t, err)

	dst := NewMetaFSM()
	dst.SetDEKKeeper(wrongKeeper)

	// Restore must fail — the wrapped secret was sealed under KEK-A.
	err = dst.Restore(raft.SnapshotMeta{}, snap)
	require.Error(t, err, "Restore must fail when DEK keeper cannot unwrap JKEY secrets")

	// jwtKeyStore must remain in its zero state (both slots nil).
	gotCurrent, gotPrevious := dst.jwtKeyStore.Snapshot()
	assert.Nil(t, gotCurrent, "jwtKeyStore.current must be nil after failed Restore (F14)")
	assert.Nil(t, gotPrevious, "jwtKeyStore.previous must be nil after failed Restore (F14)")
}
