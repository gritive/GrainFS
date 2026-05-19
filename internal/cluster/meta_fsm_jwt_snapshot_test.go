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
	keeper, err := encrypt.NewDEKKeeper(kek)
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

// applyJWTRotate applies a JWTSigningKeyRotate command to the FSM.
func applyJWTRotate(t *testing.T, fsm *MetaFSM) {
	t.Helper()
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeJWTSigningKeyRotate, nil)
	require.NoError(t, fsm.applyCmd(cmd))
}

// applyJWTPrune applies a JWTSigningKeyPrune command to the FSM.
func applyJWTPrune(t *testing.T, fsm *MetaFSM) error {
	t.Helper()
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeJWTSigningKeyPrune, nil)
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
