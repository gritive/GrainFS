package cluster

import (
	"bytes"
	"path/filepath"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/datawal"
)

// testDEKKeeper returns a fresh DEK keeper + clusterID for tests, matching the
// production at-rest shape (WithShardDEKKeeper / DEKKeeperAdapter) that replaces
// the legacy static WithEncryptor path. Values match the existing dekShardSvc
// helper so DEK-mode tests share one on-disk shape.
//
// NOTE: NewDEKKeeper generates a RANDOM DEK, so two keepers built from the same
// KEK do NOT interoperate. Callers that need a shard sealer + data WAL (and any
// cross-service interop) MUST thread the SAME returned keeper instance through
// both WithShardDEKKeeper and withTestWALDEK / mustTestDataWALDEK.
func testDEKKeeper(tb clusterTestTB) (*encrypt.DEKKeeper, []byte) {
	tb.Helper()
	clusterID := bytes.Repeat([]byte{0x42}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x91}, encrypt.KEKSize), clusterID)
	require.NoError(tb, err)
	return keeper, clusterID
}

// mustTestDataWALDEK opens a DEK-keeper-backed data WAL at an explicit dir, the
// DEK-path replacement for mustTestDataWAL(tb, dir, enc). Use this (not the
// TempDir-based withTestWALDEK) for recovery tests that reopen the service at
// the same dir. The recovery sealer matches because the SAME keeper is used.
func mustTestDataWALDEK(tb clusterTestTB, dir string, keeper *encrypt.DEKKeeper, clusterID []byte) DataWALAppender {
	tb.Helper()
	w, err := datawal.Open(filepath.Join(dir, "datawal"),
		storage.NewDEKKeeperAdapter(keeper, clusterID), datawal.NamespaceShard)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = w.Close() })
	return w
}
