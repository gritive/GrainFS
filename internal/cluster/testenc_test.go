package cluster

import (
	"bytes"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// testEncryptor returns a deterministic 32-byte-key encryptor for tests. It
// uses the same key as the existing encrypted ShardService tests so on-disk
// shard layout matches what those tests already exercise.
func testEncryptor(tb clusterTestTB) *encrypt.Encryptor {
	tb.Helper()
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte("k"), 32))
	require.NoError(tb, err)
	return enc
}
