package cluster

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSpoolPutObjectWritesPlaintext proves that regular PUT spool files are
// written as plaintext even under a DEK-backed backend. Encryption happens
// only at the final durable shard layer (eccodec.EncodeEncryptedShard).
func TestSpoolPutObjectWritesPlaintext(t *testing.T) {
	b := newTestDistributedBackendDEK(t) // production shape: WithShardDEKKeeper, encryptor nil
	plain := bytes.Repeat([]byte("X"), 3<<20)
	sp, err := b.spoolPutObject(context.Background(), "bkt", bytes.NewReader(plain), true)
	require.NoError(t, err)
	defer sp.Cleanup()
	require.False(t, sp.encrypted, "put spool must be plaintext: encryption is only at final shard")
	raw, err := os.ReadFile(sp.Path)
	require.NoError(t, err)
	require.True(t, bytes.Contains(raw, plain[:4096]), "spool temp file must contain plaintext object data")
}
