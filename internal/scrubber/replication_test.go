package scrubber

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func setupBackend(t *testing.T) (storage.Backend, string) {
	t.Helper()
	dir := t.TempDir()
	b, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	return b, dir
}

func putObject(t *testing.T, b storage.Backend, bucket, key string, data []byte) *storage.Object {
	t.Helper()
	_ = b.CreateBucket(context.Background(), bucket) // idempotent: ignore "already exists"
	obj, err := b.PutObject(context.Background(), bucket, key, strings.NewReader(string(data)), "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, obj.ETag, "PutObject must store an ETag (oracle)")
	return obj
}

func localOpener(root string) LocalOpener {
	return func(bucket, key string) (io.ReadCloser, error) {
		return os.Open(filepath.Join(root, "data", bucket, key))
	}
}

func TestReplicationObjectSource_WalksBucketPrefix(t *testing.T) {
	b, _ := setupBackend(t)
	bucket := "__grainfs_volumes"
	putObject(t, b, bucket, "__vol/v1/blk_000000000000", []byte("AAAA"))
	putObject(t, b, bucket, "__vol/v1/blk_000000000001", []byte("BBBB"))
	putObject(t, b, bucket, "__vol/v2/blk_000000000000", []byte("CCCC"))

	src := NewReplicationObjectSource("replication", bucket, "__vol/", b)
	require.Equal(t, "replication", src.Name())

	collect := func(prefix string) []string {
		ch, err := src.Iter(context.Background(), ScopeFull, prefix)
		require.NoError(t, err)
		var keys []string
		for blk := range ch {
			keys = append(keys, blk.Key)
		}
		return keys
	}

	all := collect("")
	require.Len(t, all, 3, "default prefix walks the whole source domain")

	v1 := collect("__vol/v1/")
	require.Len(t, v1, 2, "narrowed prefix walks only the requested subject")
}

func TestReplicationVerifier_HealthyCorruptMissing(t *testing.T) {
	b, root := setupBackend(t)
	bucket := "__grainfs_volumes"
	key := "__vol/v1/blk_000000000000"
	putObject(t, b, bucket, key, []byte("DDDD"))

	src := NewReplicationObjectSource("replication", bucket, "__vol/", b)
	ch, err := src.Iter(context.Background(), ScopeFull, "")
	require.NoError(t, err)
	var blocks []Block
	for blk := range ch {
		blocks = append(blocks, blk)
	}
	require.Len(t, blocks, 1)
	target := blocks[0]

	rep := &fakeRepairer{}
	v := NewReplicationVerifier(localOpener(root), rep)

	st, err := v.Verify(context.Background(), target)
	require.NoError(t, err)
	require.True(t, st.Healthy, "expected healthy, got %+v", st)

	localPath := filepath.Join(root, "data", bucket, key)
	require.NoError(t, os.Truncate(localPath, 1))
	st, err = v.Verify(context.Background(), target)
	require.NoError(t, err)
	require.True(t, st.Corrupt)
	require.False(t, st.Healthy)

	require.NoError(t, os.Remove(localPath))
	st, err = v.Verify(context.Background(), target)
	require.NoError(t, err)
	require.True(t, st.Missing)

	require.NoError(t, v.Repair(context.Background(), target))
	require.Equal(t, []string{target.Bucket + "/" + target.Key}, rep.calls)
}

func TestReplicationVerifier_ETagIsObjectMD5(t *testing.T) {
	b, root := setupBackend(t)
	bucket := "__grainfs_volumes"
	key := "__vol/v1/blk_000000000000"
	putObject(t, b, bucket, key, []byte("hello"))

	src := NewReplicationObjectSource("replication", bucket, "__vol/", b)
	ch, _ := src.Iter(context.Background(), ScopeFull, "")
	var blocks []Block
	for blk := range ch {
		blocks = append(blocks, blk)
	}
	require.Len(t, blocks, 1)
	got := blocks[0]

	data, err := os.ReadFile(filepath.Join(root, "data", bucket, key))
	require.NoError(t, err)
	h := md5.Sum(data)
	require.Equal(t, hex.EncodeToString(h[:]), got.ExpectedETag,
		"expected ETag = MD5(local file). If this fails the storage layer regressed the oracle.")
}

type fakeRepairer struct{ calls []string }

func (f *fakeRepairer) RepairReplica(ctx context.Context, bucket, key string) error {
	f.calls = append(f.calls, bucket+"/"+key)
	return nil
}

// TestReplicationVerifier_LegacyETagSkipped — blocks written before the
// MD5-oracle restoration have ExpectedETag="". The verifier must report
// Skipped (not Corrupt) so the first-deploy of this code does not mass-flag
// every legacy block as corrupt.
func TestReplicationVerifier_LegacyETagSkipped(t *testing.T) {
	b, root := setupBackend(t)
	v := NewReplicationVerifier(localOpener(root), &fakeRepairer{})
	_ = b
	st, err := v.Verify(context.Background(), Block{
		Bucket:       "__grainfs_volumes",
		Key:          "__vol/legacy/blk_000000000000",
		ExpectedETag: "",
	})
	require.NoError(t, err)
	require.True(t, st.Skipped, "empty-ETag legacy block must be Skipped, got %+v", st)
	require.False(t, st.Corrupt)
	require.False(t, st.Healthy)
}
