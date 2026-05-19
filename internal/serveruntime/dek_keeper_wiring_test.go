package serveruntime

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// TestDEKKeeperWiring verifies that the API contract exercised by the
// production boot path (C2) holds:
//
//   - LoadOrGenerateKEK generates a fresh KEK when the file is absent.
//   - NewDEKKeeper succeeds with that KEK.
//   - SetDEKKeeper injects the keeper into a MetaFSM without error.
//
// This does not spin up meta-raft; the test covers the wiring surface so a
// regression in any of the three calls surfaces here without a full boot
// fixture.
func TestDEKKeeperWiring(t *testing.T) {
	dir := t.TempDir()
	src := "file://" + filepath.Join(dir, "kek.key")

	kek, err := encrypt.LoadOrGenerateKEK(src)
	require.NoError(t, err, "LoadOrGenerateKEK should succeed on first call (generates key)")
	require.Len(t, kek, encrypt.KEKSize, "KEK must be %d bytes", encrypt.KEKSize)

	keeper, err := encrypt.NewDEKKeeper(kek)
	require.NoError(t, err, "NewDEKKeeper must succeed with a valid KEK")
	require.NotNil(t, keeper)

	fsm := cluster.NewMetaFSM()
	// SetDEKKeeper must not panic; a second call with the same keeper is also safe.
	fsm.SetDEKKeeper(keeper)

	gen, wrapped := keeper.Active()
	assert.Equal(t, uint32(0), gen, "initial active generation must be 0")
	assert.NotEmpty(t, wrapped, "wrapped DEK must not be empty")
}

// TestDEKKeeperWiring_LoadIdempotent verifies that a second LoadOrGenerateKEK
// call on an existing file returns the same 32-byte key (no overwrite).
func TestDEKKeeperWiring_LoadIdempotent(t *testing.T) {
	dir := t.TempDir()
	src := "file://" + filepath.Join(dir, "kek.key")

	kek1, err := encrypt.LoadOrGenerateKEK(src)
	require.NoError(t, err)

	kek2, err := encrypt.LoadOrGenerateKEK(src)
	require.NoError(t, err)

	assert.Equal(t, kek1, kek2, "second load must return identical KEK bytes")
}
