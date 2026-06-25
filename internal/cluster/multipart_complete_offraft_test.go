package cluster

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// countSegmentShardFiles walks the backend's on-disk shard data dirs and counts
// the segment shard files (.../<key>/segments/<blobID>/shard_N) under the bucket.
// A multipart complete that re-assembles writes new segment shards; an idempotent
// retry that short-circuits on the det-vid object writes none. Read-only helper.
func countSegmentShardFiles(t *testing.T, b *DistributedBackend, bucket string) int {
	t.Helper()
	n := 0
	for _, dataDir := range b.shardSvc.DataDirs() {
		root := filepath.Join(dataDir, bucket)
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if info.IsDir() || !strings.HasPrefix(info.Name(), "shard_") {
				return nil
			}
			// shard_N lives directly under .../segments/<blobID>/.
			if filepath.Base(filepath.Dir(filepath.Dir(path))) == "segments" {
				n++
			}
			return nil
		})
		require.NoError(t, err)
	}
	return n
}

// TestCompleteMultipart_NoCompletePropose proves M3: a versioning-enabled
// multipart complete makes NO CmdCompleteMultipart raft propose — the per-version
// blob is the sole durable authority. Uses the recordingMultipartRaftNode to
// observe every command type proposed during the complete.
func TestCompleteMultipart_NoCompletePropose(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	payload := []byte("no-complete-propose-payload")
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader(payload), "")
	require.NoError(t, err)

	rec := &recordingMultipartRaftNode{RaftNode: b.node}
	b.node = rec

	obj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	require.NotNil(t, obj)

	for _, ct := range rec.commandTypes() {
		require.NotEqualf(t, 6, ct,
			"multipart complete must NOT propose the retired CmdCompleteMultipart slot 6 (blob is blob authority)")
	}
}

// TestCompleteMultipart_NonVersionedNoCompletePropose proves the same for a
// non-versioned bucket: the latest-only blob is the blob authority — no propose.
func TestCompleteMultipart_NonVersionedNoCompletePropose(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "plainbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	payload := []byte("non-versioned-no-propose")
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader(payload), "")
	require.NoError(t, err)

	rec := &recordingMultipartRaftNode{RaftNode: b.node}
	b.node = rec

	obj, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	require.NotNil(t, obj)

	for _, ct := range rec.commandTypes() {
		require.NotEqualf(t, 6, ct,
			"non-versioned multipart complete must NOT propose the retired CmdCompleteMultipart slot 6")
	}
}

// TestCompleteMultipart_RetryShortCircuitsNoReassembly proves the det-vid
// existence short-circuit carries idempotency without re-assembly: a second
// CompleteMultipartUpload (manifest already deleted by the first) returns the
// committed object and writes NO new segment shards.
func TestCompleteMultipart_RetryShortCircuitsNoReassembly(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	payload := []byte("retry-short-circuit-no-reassembly")
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader(payload), "")
	require.NoError(t, err)

	obj1, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	shardsAfterFirst := countSegmentShardFiles(t, b, bkt)
	require.NotZero(t, shardsAfterFirst, "first complete must write segment shards")

	obj2, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err, "retry after success must be idempotent")
	require.Equal(t, obj1.VersionID, obj2.VersionID)
	require.Equal(t, obj1.ETag, obj2.ETag)
	require.Equal(t, obj1.Size, obj2.Size)

	require.Equal(t, shardsAfterFirst, countSegmentShardFiles(t, b, bkt),
		"idempotent retry must short-circuit on the det-vid object and write no new segment shards")
}

// TestCompleteMultipart_NonVersionedLatestOnlyFailClosed proves M3 F7: with the
// non-versioned multipart object's latest-only quorum-meta blob now the SOLE
// authority, a forced fault on that write makes CompleteMultipartUpload return an
// error (not a phantom success). After the failure no FSM obj: record exists and
// the object is NOT readable — nothing was durably committed.
func TestCompleteMultipart_NonVersionedLatestOnlyFailClosed(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "plainbkt", "mp.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	// Non-versioned: the latest-only blob is the blob authority.

	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader([]byte("fail-closed-payload")), "")
	require.NoError(t, err)

	// Force the latest-only quorum-meta local write to fail: pre-create the per-bucket
	// .quorum_meta directory read-only so os.CreateTemp in writeQuorumMetaLocal fails.
	// (The EC segment write targets a different subtree and is unaffected.)
	qmetaBucketDir := filepath.Join(b.shardSvc.DataDirs()[0], quorumMetaSubDir, bkt)
	require.NoError(t, os.MkdirAll(qmetaBucketDir, 0o755))
	require.NoError(t, os.Chmod(qmetaBucketDir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(qmetaBucketDir, 0o755) })

	_, err = b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.Error(t, err, "a failed latest-only blob-authority write must FAIL the complete (no phantom success)")

	// No FSM obj: record was written (apply is never reached — the complete is raft-free).
	require.NoError(t, b.store.View(func(txn MetadataTxn) error {
		_, gerr := txn.Get(b.ks().ObjectMetaKey(bkt, key))
		require.ErrorIs(t, gerr, ErrMetaKeyNotFound, "fail-closed complete must not leave an FSM obj: record")
		return nil
	}))

	// Restore write access; the object must NOT be readable (nothing committed).
	require.NoError(t, os.Chmod(qmetaBucketDir, 0o755))
	_, err = b.HeadObject(ctx, bkt, key)
	require.ErrorIs(t, err, storage.ErrObjectNotFound, "no object may be readable after a fail-closed complete")
}

// TestCompleteMultipart_VersionedDuplicateReturnsDeterministicVersion is the
// regression test for the versioned short-circuit bug: if a newer PutObject
// lands on the same key between the original complete and a duplicate/retry
// complete, the retry must return the MULTIPART version's metadata (det-vid,
// ETag, Size) — not the newer PUT's metadata. Before the fix, the short-circuit
// called headObjectMeta (latest) which returned the newer version.
func TestCompleteMultipart_VersionedDuplicateReturnsDeterministicVersion(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "mp-dup.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))

	// First: complete a multipart upload and capture the committed object.
	up, err := b.CreateMultipartUpload(ctx, bkt, key, "application/octet-stream")
	require.NoError(t, err)
	mpPayload := bytes.Repeat([]byte("m"), 64)
	part, err := b.UploadPart(ctx, bkt, key, up.UploadID, 1, bytes.NewReader(mpPayload), "")
	require.NoError(t, err)

	obj1, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	require.NotEmpty(t, obj1.VersionID, "first complete must return a versioned object")

	// Second: PUT a different object to the SAME key → this lands a NEWER version.
	newObj, err := b.PutObject(ctx, bkt, key, bytes.NewReader([]byte("newer-put-data")), "text/plain")
	require.NoError(t, err)
	require.NotEqual(t, obj1.VersionID, newObj.VersionID, "PutObject must produce a different version")

	// Third: duplicate/retry CompleteMultipartUpload for the SAME uploadID.
	// Must return the MULTIPART object's metadata (det-vid == obj1.VersionID),
	// NOT the newer PUT's metadata.
	obj2, err := b.CompleteMultipartUpload(ctx, bkt, key, up.UploadID, []storage.Part{*part})
	require.NoError(t, err, "idempotent retry must succeed, not return ErrUploadNotFound")
	require.Equal(t, obj1.VersionID, obj2.VersionID,
		"retry must return the det-vid version, not the newer PutObject version")
	require.Equal(t, obj1.ETag, obj2.ETag,
		"retry must return the multipart ETag, not the newer PutObject ETag")
	require.Equal(t, obj1.Size, obj2.Size,
		"retry must return the multipart size, not the newer PutObject size")
}
