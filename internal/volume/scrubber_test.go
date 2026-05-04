package volume

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// localBackendOpener returns a LocalOpener that reads from a LocalBackend's
// on-disk layout: {root}/data/{bucket}/{key}. Used for tests.
func localBackendOpener(root string) LocalOpener {
	return func(bucket, key string) (io.ReadCloser, error) {
		return os.Open(filepath.Join(root, "data", bucket, key))
	}
}

func setupManagerWithRoot(t *testing.T) (*Manager, string) {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	return NewManager(backend), dir
}

func TestVolumeBlockSource_FullSupersetOfLive(t *testing.T) {
	mgr, _ := setupManagerWithRoot(t)
	const blockSize = DefaultBlockSize
	require.NotZero(t, blockSize)

	_, err := mgr.Create("vol1", 8*int64(blockSize))
	require.NoError(t, err)

	// Write to two distinct blocks (block 0 and block 1).
	_, err = mgr.WriteAt("vol1", []byte("AAAA"), 0)
	require.NoError(t, err)
	_, err = mgr.WriteAt("vol1", []byte("BBBB"), int64(blockSize))
	require.NoError(t, err)

	// Snapshot, then overwrite block 0 to force a CoW physical key.
	_, err = mgr.CreateSnapshot("vol1")
	require.NoError(t, err)
	_, err = mgr.WriteAt("vol1", []byte("CCCC"), 0)
	require.NoError(t, err)

	src := NewBlockSource(mgr)
	require.Equal(t, "volume", src.Name())

	collect := func(scope scrubber.ScrubScope) []string {
		ch, err := src.Iter(context.Background(), scope, BlockKeyPrefix("vol1"))
		require.NoError(t, err)
		var keys []string
		for b := range ch {
			keys = append(keys, b.Key)
		}
		return keys
	}

	full := collect(scrubber.ScopeFull)
	live := collect(scrubber.ScopeLive)

	// After snapshot+CoW: full sees the original block-0 (snapshot only),
	// the new CoW block-0, and block-1 → 3 blocks. live sees only the live
	// physicals → 2 blocks.
	require.Greater(t, len(full), len(live), "full should include CoW + snapshot blocks: full=%v live=%v", full, live)
	for _, k := range live {
		require.Contains(t, full, k, "every live key must appear in full: live=%v full=%v", live, full)
	}
}

type fakeReplicaRepairer struct {
	calls []string
}

func (f *fakeReplicaRepairer) RepairReplica(ctx context.Context, bucket, key string) error {
	f.calls = append(f.calls, bucket+"/"+key)
	return nil
}

func TestVolumeBlockVerifier_HealthyCorruptMissing(t *testing.T) {
	mgr, root := setupManagerWithRoot(t)
	_, err := mgr.Create("vol1", int64(DefaultBlockSize)*4)
	require.NoError(t, err)
	_, err = mgr.WriteAt("vol1", []byte("DDDD"), 0)
	require.NoError(t, err)

	src := NewBlockSource(mgr)
	ch, err := src.Iter(context.Background(), scrubber.ScopeFull, BlockKeyPrefix("vol1"))
	require.NoError(t, err)
	var blocks []scrubber.Block
	for b := range ch {
		blocks = append(blocks, b)
	}
	require.NotEmpty(t, blocks)
	target := blocks[0]

	rep := &fakeReplicaRepairer{}
	v := NewBlockVerifier(localBackendOpener(root), rep)

	// Healthy: the just-written block matches its ETag.
	st, err := v.Verify(context.Background(), target)
	require.NoError(t, err)
	require.True(t, st.Healthy, "expected healthy, got %+v", st)

	// Corrupt: truncate local file to 1 byte. ETag mismatch.
	localPath := filepath.Join(root, "data", target.Bucket, target.Key)
	require.NoError(t, os.Truncate(localPath, 1))
	st, err = v.Verify(context.Background(), target)
	require.NoError(t, err)
	require.True(t, st.Corrupt, "expected corrupt, got %+v", st)
	require.False(t, st.Healthy)

	// Missing: remove local file.
	require.NoError(t, os.Remove(localPath))
	st, err = v.Verify(context.Background(), target)
	require.NoError(t, err)
	require.True(t, st.Missing, "expected missing, got %+v", st)

	// Repair delegates to RepairReplica.
	require.NoError(t, v.Repair(context.Background(), target))
	require.Equal(t, []string{target.Bucket + "/" + target.Key}, rep.calls)
}

// sanity: the ETag stored by Manager is the raw MD5 of the written block bytes
// (the physical object equals one volume block, not just the user payload —
// blocks are block-size-aligned with zero padding for unwritten ranges).
func TestVolumeBlock_ETagIsBlockMD5(t *testing.T) {
	mgr, root := setupManagerWithRoot(t)
	_, err := mgr.Create("vol1", int64(DefaultBlockSize)*2)
	require.NoError(t, err)
	_, err = mgr.WriteAt("vol1", []byte("hello"), 0)
	require.NoError(t, err)

	src := NewBlockSource(mgr)
	ch, _ := src.Iter(context.Background(), scrubber.ScopeFull, BlockKeyPrefix("vol1"))
	var blocks []scrubber.Block
	for b := range ch {
		blocks = append(blocks, b)
	}
	require.NotEmpty(t, blocks)
	b := blocks[0]

	data, err := os.ReadFile(filepath.Join(root, "data", b.Bucket, b.Key))
	require.NoError(t, err)
	h := md5.Sum(data)
	require.Equal(t, b.ExpectedETag, hex.EncodeToString(h[:]),
		"expected ETag = MD5(local file). If this fails, BlockVerifier.Verify needs a different oracle.")
}
