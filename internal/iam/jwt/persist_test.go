package jwt

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/require"
)

// jwtTestClusterID returns a deterministic 16-byte clusterID for DEKKeeper
// construction (DEK wraps are AAD-bound to clusterID).
func jwtTestClusterID() []byte {
	id := make([]byte, 16)
	for i := range id {
		id[i] = byte(i + 1)
	}
	return id
}

func TestKeySet_LoadFromSeeds_Roundtrip(t *testing.T) {
	kek := make([]byte, 32)
	_, err := rand.Read(kek)
	require.NoError(t, err)
	keeper, err := encrypt.NewDEKKeeper(kek, jwtTestClusterID())
	require.NoError(t, err)

	secret := make([]byte, 32)
	_, err = rand.Read(secret)
	require.NoError(t, err)
	wrapped, gen, err := keeper.Seal(secret)
	require.NoError(t, err)

	seeds := []KeySeed{{Kid: "k_test", WrappedSecret: wrapped, DekGen: gen, Role: "current"}}
	ks := NewKeySet()
	require.NoError(t, ks.LoadFromSeeds(seeds, keeper))

	tok, err := ks.Mint(Claims{Sub: "sa-1", Warehouse: "w"})
	require.NoError(t, err)
	_, err = ks.Verify(tok)
	require.NoError(t, err)
}

func TestKeySet_DemoteAndInstall(t *testing.T) {
	kek := make([]byte, 32)
	_, err := rand.Read(kek)
	require.NoError(t, err)
	keeper, err := encrypt.NewDEKKeeper(kek, jwtTestClusterID())
	require.NoError(t, err)

	// Install first key
	secret1 := make([]byte, 32)
	_, err = rand.Read(secret1)
	require.NoError(t, err)
	wrapped1, gen1, err := keeper.Seal(secret1)
	require.NoError(t, err)

	seed1 := KeySeed{Kid: "k_first", WrappedSecret: wrapped1, DekGen: gen1, Role: "current"}
	ks := NewKeySet()
	require.NoError(t, ks.InstallCurrent(seed1, keeper))

	// Mint a token with the first key
	tok1, err := ks.Mint(Claims{Sub: "sa-1", Warehouse: "w", TTL: time.Hour})
	require.NoError(t, err)

	// Demote first key to previous
	now := time.Now()
	ks.DemoteCurrentToPrevious(now)

	// Install second key
	secret2 := make([]byte, 32)
	_, err = rand.Read(secret2)
	require.NoError(t, err)
	wrapped2, gen2, err := keeper.Seal(secret2)
	require.NoError(t, err)

	seed2 := KeySeed{Kid: "k_second", WrappedSecret: wrapped2, DekGen: gen2, Role: "current"}
	require.NoError(t, ks.InstallCurrent(seed2, keeper))

	// Token minted before the demote must still verify (previous key)
	_, err = ks.Verify(tok1)
	require.NoError(t, err, "token minted by demoted key must still verify during dual-window")

	// Token minted with second key must also verify
	tok2, err := ks.Mint(Claims{Sub: "sa-2", Warehouse: "w", TTL: time.Hour})
	require.NoError(t, err)
	_, err = ks.Verify(tok2)
	require.NoError(t, err)
}
