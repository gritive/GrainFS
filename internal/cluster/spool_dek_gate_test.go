package cluster

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSpoolPutObjectEncryptsUnderDEK proves the production-shaped backend
// (WithShardDEKKeeper, static encryptor nil) routes the PUT spool through the
// DEK seam and writes ciphertext on disk — the gap #631 opened by retiring the
// static key.
func TestSpoolPutObjectEncryptsUnderDEK(t *testing.T) {
	b := newTestDistributedBackendDEK(t) // production shape: WithShardDEKKeeper, encryptor nil
	plain := bytes.Repeat([]byte("X"), 3<<20)
	sp, err := b.spoolPutObject(context.Background(), "bkt", bytes.NewReader(plain))
	require.NoError(t, err)
	defer sp.Cleanup()
	require.True(t, sp.encrypted, "spool must be encrypted under a DEK-backed backend")
	raw, err := os.ReadFile(sp.Path)
	require.NoError(t, err)
	require.False(t, bytes.Contains(raw, plain[:4096]), "spool temp file must be ciphertext")
}
