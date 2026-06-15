package cluster

import (
	"bytes"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// testDEKKeeper returns a fresh DEK keeper + clusterID for tests, matching the
// production at-rest shape (WithShardDEKKeeper / DEKKeeperAdapter) that replaces
// the legacy static WithEncryptor path. Values match the existing dekShardSvc
// helper so DEK-mode tests share one on-disk shape.
//
// NOTE: NewDEKKeeper generates a RANDOM DEK, so two keepers built from the same
// KEK do NOT interoperate. Callers that need a shard sealer MUST thread the SAME
// returned keeper instance through WithShardDEKKeeper.
func testDEKKeeper(tb clusterTestTB) (*encrypt.DEKKeeper, []byte) {
	tb.Helper()
	clusterID := bytes.Repeat([]byte{0x42}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x91}, encrypt.KEKSize), clusterID)
	require.NoError(tb, err)
	return keeper, clusterID
}
