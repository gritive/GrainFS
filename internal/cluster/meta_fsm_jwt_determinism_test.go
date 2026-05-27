package cluster

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
)

// TestMetaFSM_JWTRotate_DeterministicAcrossNodes verifies that applying the
// same MetaCmdTypeJWTSigningKeyRotate payload on two FSMs sharing a KEK yields
// identical jwtKeyStore + jwtKeys state. Regression test for F1 (split-brain):
// the old code ran rand.Read + Seal inside applyCmd which produced different
// bytes on every node; this test FAILS with the old code and PASSES after fix.
func TestMetaFSM_JWTRotate_DeterministicAcrossNodes(t *testing.T) {
	// Both nodes share the same KEK (per-cluster secret, identical across nodes).
	kek := make([]byte, 32)
	_, err := rand.Read(kek)
	require.NoError(t, err)

	// Node A gets a fresh DEKKeeper.
	keeperA, err := encrypt.NewDEKKeeper(kek, dekTestClusterID())
	require.NoError(t, err)

	// Node B's DEKKeeper is loaded from A's wrapped DEK state — this mimics
	// production where all nodes receive the same DEK via Raft log replay or
	// snapshot restore (the KEK is per-cluster but the wrapped DEK is stored
	// in the Raft log and shared).
	versions, _ := keeperA.VersionsAndActive()
	keeperB, err := encrypt.LoadFromFSM(kek, dekTestClusterID(), versions, 0)
	require.NoError(t, err)

	fsmA := NewMetaFSM()
	fsmA.SetDEKKeeper(keeperA)
	fsmB := NewMetaFSM()
	fsmB.SetDEKKeeper(keeperB)

	// Simulate proposer-side work: generate the payload once, before proposal.
	secret := make([]byte, 32)
	_, err = rand.Read(secret)
	require.NoError(t, err)

	// Seal with keeperA — keeperB has the same wrapped DEK so it can Open the same bytes.
	wrapped, gen, err := keeperA.Seal(secret)
	require.NoError(t, err)

	const fixedKid = "k_deterministic_test"
	const demotedAtUnix = int64(1_700_000_000) // fixed for determinism

	payload := EncodeMetaJWTSigningKeyRotateCmd(fixedKid, wrapped, gen, demotedAtUnix)
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeJWTSigningKeyRotate, payload)

	// Apply the SAME command on both FSMs (simulates Raft log delivery).
	require.NoError(t, fsmA.applyCmd(cmd))
	require.NoError(t, fsmB.applyCmd(cmd))

	// Both FSMs must have the same kid in current.
	curA, _ := fsmA.jwtKeyStore.Snapshot()
	curB, _ := fsmB.jwtKeyStore.Snapshot()
	require.NotNil(t, curA, "fsmA must have a current key")
	require.NotNil(t, curB, "fsmB must have a current key")
	assert.Equal(t, curA.Kid, curB.Kid, "kid must match across nodes")
	assert.Equal(t, curA.WrappedSecret, curB.WrappedSecret, "wrapped_secret must match across nodes")
	assert.Equal(t, curA.DekGen, curB.DekGen, "dek_gen must match across nodes")

	// A token minted on A must verify on B (same unwrapped secret after Open).
	tok, err := fsmA.JWTKeySet().Mint(iamjwt.Claims{Sub: "test", Warehouse: "default"})
	require.NoError(t, err)
	_, err = fsmB.JWTKeySet().Verify(tok)
	require.NoError(t, err, "token minted on A must verify on B — split-brain regression check")
}

// TestJWTMetaCmdCodec_RotateRoundtrip verifies encode/decode round-trip for
// MetaJWTSigningKeyRotateCmd.
func TestJWTMetaCmdCodec_RotateRoundtrip(t *testing.T) {
	const kid = "k_roundtrip"
	wrapped := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02}
	const dekGen = uint32(3)
	const demotedAtUnix = int64(1_700_000_000)

	encoded := EncodeMetaJWTSigningKeyRotateCmd(kid, wrapped, dekGen, demotedAtUnix)
	require.NotEmpty(t, encoded)

	gotKid, gotWrapped, gotGen, gotDemotedAt, err := decodeMetaJWTSigningKeyRotateCmd(encoded)
	require.NoError(t, err)
	assert.Equal(t, kid, gotKid)
	assert.Equal(t, wrapped, gotWrapped)
	assert.Equal(t, dekGen, gotGen)
	assert.Equal(t, demotedAtUnix, gotDemotedAt)
}

// TestJWTMetaCmdCodec_PruneRoundtrip verifies encode/decode round-trip for
// MetaJWTSigningKeyPruneCmd.
func TestJWTMetaCmdCodec_PruneRoundtrip(t *testing.T) {
	const pruneAtUnix = int64(1_700_100_000)

	encoded := EncodeMetaJWTSigningKeyPruneCmd(pruneAtUnix)
	require.NotEmpty(t, encoded)

	gotPruneAt, err := decodeMetaJWTSigningKeyPruneCmd(encoded)
	require.NoError(t, err)
	assert.Equal(t, pruneAtUnix, gotPruneAt)
}

// TestJWTMetaCmdCodec_EmptyPayloadErrors verifies that empty/nil payloads
// return clear errors rather than panicking.
func TestJWTMetaCmdCodec_EmptyPayloadErrors(t *testing.T) {
	_, _, _, _, err := decodeMetaJWTSigningKeyRotateCmd(nil)
	require.Error(t, err, "empty rotate payload must error")

	_, _, _, _, err = decodeMetaJWTSigningKeyRotateCmd([]byte{})
	require.Error(t, err, "empty rotate payload must error")

	_, err = decodeMetaJWTSigningKeyPruneCmd(nil)
	require.Error(t, err, "empty prune payload must error")
}
