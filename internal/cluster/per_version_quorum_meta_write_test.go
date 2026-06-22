package cluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestWriteQuorumMetaVersionLocal_WritesToSeparateSubtree proves the per-version
// blob lands at .quorum_meta_versions/{bucket}/{key}/{vid} (separate from the
// latest-only .quorum_meta tree) and is byte-identical to the data written.
func TestWriteQuorumMetaVersionLocal_WritesToSeparateSubtree(t *testing.T) {
	b := newTestDistributedBackend(t)
	data := []byte("blob-bytes")

	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal("bkt", filepath.Join("a/b/c.txt", "vid-1"), data))

	root := b.shardSvc.dataDirs[0]
	verPath := filepath.Join(root, quorumMetaVersionsSubDir, "bkt", "a/b/c.txt", "vid-1")
	got, err := os.ReadFile(verPath)
	require.NoError(t, err, "per-version blob must exist at %s", verPath)
	require.Equal(t, data, got)

	// The latest-only tree must be untouched (no collision, separate subtree).
	latPath := filepath.Join(root, quorumMetaSubDir, "bkt", "a/b/c.txt")
	_, statErr := os.Stat(latPath)
	require.True(t, os.IsNotExist(statErr), "latest-only tree must not be written by the version primitive")
}

// TestWriteQuorumMetaVersion_RPC proves the per-version write RPC durably writes
// the blob on a remote placement node's separate subtree.
func TestWriteQuorumMetaVersion_RPC(t *testing.T) {
	ctx := context.Background()
	keeper, clusterID := testDEKKeeper(t)

	trA := transport.MustNewHTTPTransport("test-cluster-psk")
	trB := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, trA.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, trB.Listen(ctx, "127.0.0.1:0"))
	defer trA.Close()
	defer trB.Close()

	dirB := t.TempDir()
	svcA := NewShardService(t.TempDir(), trA, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	svcB := NewShardService(dirB, trB, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	trB.RegisterBufferedRoute(transport.RouteShardRPC, svcB.NativeRPCHandler())

	data := []byte("ver-blob")
	require.NoError(t, svcA.WriteQuorumMetaVersion(ctx, trB.LocalAddr(), "bkt", filepath.Join("k", "vid-1"), data))

	got, err := os.ReadFile(filepath.Join(dirB, "shards", quorumMetaVersionsSubDir, "bkt", "k", "vid-1"))
	require.NoError(t, err, "remote node must have written the per-version blob")
	require.Equal(t, data, got)
}

// TestWriteQuorumMeta_AlsoWritesPerVersion proves a versioned PUT writes the
// per-version blob (immutable: two versions → two blobs), and that the existing
// latest-only behavior is unchanged.
func TestWriteQuorumMeta_AlsoWritesPerVersion(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))
	require.NoError(t, b.SetBucketVersioning("bucket", "Enabled"))

	put1, err := b.PutObject(ctx, "bucket", "obj", bytes.NewReader(bytes.Repeat([]byte("a"), 128)), "application/octet-stream")
	require.NoError(t, err)
	put2, err := b.PutObject(ctx, "bucket", "obj", bytes.NewReader(bytes.Repeat([]byte("b"), 128)), "application/octet-stream")
	require.NoError(t, err)

	root := b.shardSvc.dataDirs[0]
	for _, vid := range []string{put1.VersionID, put2.VersionID} {
		p := filepath.Join(root, quorumMetaVersionsSubDir, "bucket", "obj", vid)
		_, statErr := os.Stat(p)
		require.NoError(t, statErr, "per-version blob must exist for version %s at %s", vid, p)
	}

	// Latest-only blob still present and HeadObject unchanged (behavior-neutral).
	head, err := b.HeadObject(ctx, "bucket", "obj")
	require.NoError(t, err)
	require.Equal(t, put2.VersionID, head.VersionID)
}

// mustEncodeMetaCmd encodes a PutObjectMetaCmd to a quorum-meta blob for use in
// tests. Reused across tasks.
func mustEncodeMetaCmd(t *testing.T, cmd PutObjectMetaCmd) []byte {
	t.Helper()
	blob, err := EncodeCommand(CmdPutObjectMeta, cmd)
	require.NoError(t, err)
	return blob
}

// TestWriteQuorumMetaVersionLocal_SkipsWhenExistingWins verifies that a stale
// blind-writer (e.g. leaderless backfill) cannot overwrite a newer on-disk blob.
func TestWriteQuorumMetaVersionLocal_SkipsWhenExistingWins(t *testing.T) {
	b := newTestDistributedBackend(t)
	sub := filepath.Join("k", "vid-1")

	newer := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "vid-1", ModTime: 200, MetaSeq: 2})
	older := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "vid-1", ModTime: 100, MetaSeq: 1})

	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal("bkt", sub, newer))
	// An older blind-writer (e.g. backfill) must NOT overwrite the newer on-disk blob.
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal("bkt", sub, older))

	got, err := os.ReadFile(filepath.Join(b.shardSvc.dataDirs[0], quorumMetaVersionsSubDir, "bkt", "k", "vid-1"))
	require.NoError(t, err)
	require.Equal(t, newer, got, "older blind-write must be skipped; newer blob preserved")
}

// TestWriteQuorumMetaVersionLocal_OverwritesWhenCandidateWins verifies that a
// strictly-newer candidate (higher MetaSeq) correctly overwrites the on-disk blob.
func TestWriteQuorumMetaVersionLocal_OverwritesWhenCandidateWins(t *testing.T) {
	b := newTestDistributedBackend(t)
	sub := filepath.Join("k", "vid-1")
	older := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "vid-1", ModTime: 100, MetaSeq: 1})
	newer := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "vid-1", ModTime: 100, MetaSeq: 2}) // same ModTime/VID, higher MetaSeq
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal("bkt", sub, older))
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal("bkt", sub, newer))
	got, err := os.ReadFile(filepath.Join(b.shardSvc.dataDirs[0], quorumMetaVersionsSubDir, "bkt", "k", "vid-1"))
	require.NoError(t, err)
	require.Equal(t, newer, got, "higher-MetaSeq candidate must overwrite (relocation/RMW path)")
}

// TestWriteQuorumMetaLocal_SkipsWhenExistingWins verifies that a stale
// blind-writer (e.g. leaderless backfill) cannot overwrite a newer on-disk blob
// in the latest-only leaf writer.
func TestWriteQuorumMetaLocal_SkipsWhenExistingWins(t *testing.T) {
	b := newTestDistributedBackend(t)
	newer := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "v2", ModTime: 200})
	older := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100})
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", newer))
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", older))
	got, err := os.ReadFile(filepath.Join(b.shardSvc.dataDirs[0], quorumMetaSubDir, "bkt", "k"))
	require.NoError(t, err)
	require.Equal(t, newer, got)
}

// TestWriteQuorumMetaLocal_OverwritesOnTie verifies the latest-only writer
// OVERWRITES on a full (ModTime, VersionID, MetaSeq) tie. The guard uses strict
// "existing beats candidate" (quorum_meta.go:445-452), so a read-modify-write
// mutation (here an ACL change) carrying equal ModTime/MetaSeq is NEVER
// suppressed — only a strictly-newer on-disk blob causes a skip. This intent
// previously lived only in a code comment.
func TestWriteQuorumMetaLocal_OverwritesOnTie(t *testing.T) {
	b := newTestDistributedBackend(t)
	first := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 1})
	// Same (ModTime, VID, MetaSeq) — a TIE — but a different ACL, so the encoded
	// bytes differ. The RMW mutation must land (overwrite), not be suppressed.
	second := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 1, ACL: uint8(s3auth.ACLPublicRead)})
	require.NotEqual(t, first, second, "tie blobs must differ in bytes to prove an overwrite happened")

	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", first))
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", second))

	got, err := os.ReadFile(filepath.Join(b.shardSvc.dataDirs[0], quorumMetaSubDir, "bkt", "k"))
	require.NoError(t, err)
	require.Equal(t, second, got, "latest-only writer must overwrite on a (ModTime,VID,MetaSeq) tie so an RMW mutation is not suppressed")
}

// TestWriteQuorumMetaVersionLocal_ConcurrentWritesLWWMax verifies that when N
// goroutines concurrently call writeQuorumMetaVersionLocal for the same
// (bucket, versionSubpath), the on-disk blob after all writes settles on the
// blob with the highest MetaSeq (LWW winner). The same ModTime and VersionID
// are used so MetaSeq is the sole discriminator, matching the relocation
// re-write scenario. Runs with -race to expose the (guard-read, rename) window
// that existed before the per-target lock was introduced.
func TestWriteQuorumMetaVersionLocal_ConcurrentWritesLWWMax(t *testing.T) {
	const N = 20
	b := newTestDistributedBackend(t)
	bucket := "bkt"
	sub := filepath.Join("k", "vid-1")

	// N blobs with same ModTime+VID but increasing MetaSeq (0 … N-1).
	blobs := make([][]byte, N)
	for i := range blobs {
		blobs[i] = mustEncodeMetaCmd(t, PutObjectMetaCmd{
			Bucket:    bucket,
			Key:       "k",
			VersionID: "vid-1",
			ModTime:   1000,
			MetaSeq:   uint64(i),
		})
	}

	var wg sync.WaitGroup
	wg.Add(N)
	start := make(chan struct{})
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			errs[i] = b.shardSvc.writeQuorumMetaVersionLocal(bucket, sub, blobs[i])
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "goroutine %d returned error", i)
	}

	// After all concurrent writes settle, the on-disk blob must be the LWW winner
	// (MetaSeq == N-1). Without the per-target lock the guard-read/rename window
	// allows a lower-MetaSeq winner to clobber the highest one.
	got, err := os.ReadFile(filepath.Join(b.shardSvc.dataDirs[0], quorumMetaVersionsSubDir, bucket, "k", "vid-1"))
	require.NoError(t, err)
	cmd, err := b.shardSvc.decodeQuorumMetaCmdBlob(got)
	require.NoError(t, err)
	require.Equal(t, uint64(N-1), cmd.MetaSeq,
		fmt.Sprintf("on-disk blob must be LWW winner (MetaSeq=%d), got MetaSeq=%d", N-1, cmd.MetaSeq))
}

// TestWriteQuorumMeta_PerVersionFailureFailsPutClosed proves the per-version blob
// is the AUTHORITATIVE metadata for a versioned object: if it cannot be durably
// written, the PUT FAILS CLOSED (was: best-effort, the PUT succeeded with a
// missing blob — a deferred 404 / lost object). The per-version blob is written
// BEFORE the latest-only blob, so on failure the latest blob is never published:
// the PUT's shard cleanup is then a clean rollback, never a published latest blob
// left pointing at data the caller is about to delete.
func TestWriteQuorumMeta_PerVersionFailureFailsPutClosed(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))
	require.NoError(t, b.SetBucketVersioning("bucket", "Enabled"))

	// Plant a FILE where the version dir for key "obj" must be, so MkdirAll fails
	// ("not a directory") for the per-version write.
	root := b.shardSvc.dataDirs[0]
	blocker := filepath.Join(root, quorumMetaVersionsSubDir, "bucket", "obj")
	require.NoError(t, os.MkdirAll(filepath.Dir(blocker), 0o755))
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o644))

	_, err := b.PutObject(ctx, "bucket", "obj", bytes.NewReader([]byte("data")), "application/octet-stream")
	require.Error(t, err, "versioned PUT must fail closed when the per-version blob cannot be durably written")

	// The per-version write runs first, so the latest-only blob is never published.
	_, statErr := os.Stat(filepath.Join(root, quorumMetaSubDir, "bucket", "obj"))
	require.True(t, os.IsNotExist(statErr),
		"latest-only blob must NOT be published when the per-version write fails")
}
