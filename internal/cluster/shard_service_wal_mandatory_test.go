package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/datawal"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// mustTestDataWAL opens a data WAL for tests so a ShardService satisfies the
// "WAL is mandatory" invariant on the shard write path. enc must match the
// ShardService's encryptor (nil for plaintext) to avoid segment mode mismatch.
func mustTestDataWAL(tb clusterTestTB, dir string, enc *encrypt.Encryptor) DataWALAppender {
	tb.Helper()
	w, err := datawal.Open(filepath.Join(dir, "datawal"), enc)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = w.Close() })
	return w
}

// withTestWAL returns a ShardServiceOption wiring a plaintext data WAL rooted
// at an independent temp dir, satisfying the mandatory-WAL invariant for tests
// whose ShardService has no encryptor.
func withTestWAL(tb clusterTestTB) ShardServiceOption {
	tb.Helper()
	return WithDataWAL(mustTestDataWAL(tb, tb.TempDir(), nil))
}

// withTestWALEnc is withTestWAL for an encrypted ShardService: the WAL must use
// the same encryptor to avoid datawal segment mode mismatch.
func withTestWALEnc(tb clusterTestTB, enc *encrypt.Encryptor) ShardServiceOption {
	tb.Helper()
	return WithDataWAL(mustTestDataWAL(tb, tb.TempDir(), enc))
}

// TestAppendShardDataWAL_RequiresWAL asserts WAL is mandatory: the shard write
// path must reject a write when no WAL is wired, instead of silently signalling
// a direct-fsync fallback (the old requireFsync=true behaviour).
func TestAppendShardDataWAL_RequiresWAL(t *testing.T) {
	svc := NewShardService(t.TempDir(), nil) // no WithDataWAL → dataWAL == nil
	_, err := svc.appendShardDataWAL(context.Background(), "bucket", "key", 0, []byte("payload"))
	require.Error(t, err, "shard write without a WAL must be rejected")
	require.Contains(t, err.Error(), "WAL")
}

// TestAppendShardDataWAL_ReplayRequiresFsync guards the surviving requireFsync
// path: during WAL replay the WAL cannot be re-appended, so the caller must
// fsync the shard file directly.
func TestAppendShardDataWAL_ReplayRequiresFsync(t *testing.T) {
	dir := t.TempDir()
	svc := NewShardService(dir, transport.MustNewQUICTransport("test-cluster-psk"), WithDataWAL(mustTestDataWAL(t, dir, nil)))
	svc.replayingDataWAL.Store(true)

	requireFsync, err := svc.appendShardDataWAL(context.Background(), "b", "k", 0, []byte("d"))
	require.NoError(t, err)
	require.True(t, requireFsync, "replay must require direct shard fsync")
}
