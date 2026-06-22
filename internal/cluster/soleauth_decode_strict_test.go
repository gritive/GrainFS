package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// seedCorruptVersionBlob writes an undecodable per-version blob at (bucket,key,vid).
func seedCorruptVersionBlob(t *testing.T, b *DistributedBackend, bucket, key, vid string) {
	t.Helper()
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal(bucket, filepath.Join(key, vid), []byte("not-a-decodable-blob")))
}

// TestReadQuorumMetaVersionsDecodeStrict covers the new read1 reader: decode-strict
// (any served corrupt blob fails the read closed) but availability-tolerant.
func TestReadQuorumMetaVersionsDecodeStrict(t *testing.T) {
	t.Run("clean blobs → all returned, deduped", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(context.Background(), "b"))
		seedVersionBlob(t, b, "b", "k", vidA1, PutObjectMetaCmd{ETag: "v1"})
		seedVersionBlob(t, b, "b", "k", vidA2, PutObjectMetaCmd{ETag: "v2"})

		cmds, err := b.readQuorumMetaVersionsDecodeStrict("b", "k")
		require.NoError(t, err)
		require.Len(t, cmds, 2)
	})

	t.Run("corrupt local blob → fail closed (vs tolerant reader which drops it)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(context.Background(), "b"))
		seedVersionBlob(t, b, "b", "k", vidA1, PutObjectMetaCmd{ETag: "v1"})
		seedCorruptVersionBlob(t, b, "b", "k", vidA2)

		// Tolerant reader silently drops the corrupt blob.
		tol, terr := b.readQuorumMetaVersions("b", "k")
		require.NoError(t, terr)
		require.Len(t, tol, 1, "tolerant reader drops the corrupt blob")

		// Strict reader fails closed.
		_, err := b.readQuorumMetaVersionsDecodeStrict("b", "k")
		require.Error(t, err, "a served corrupt blob must fail the read closed")
	})
}

// TestRead1DecodeStrictResurrection is the resurrection RED: under soleauth=on a
// corrupt latest (max-VID) blob over an older live version must NOT resurrect the
// older version — the read fails closed instead.
func TestRead1DecodeStrictResurrection(t *testing.T) {
	ctx := context.Background()

	t.Run("HEAD latest: corrupt max-VID blob + older live → error, not resurrection", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "b"))
		setVersioningForTest(t, b, "b", "Enabled")
		seedVersionBlob(t, b, "b", "k", vidA1, PutObjectMetaCmd{ETag: "older-live"})
		seedCorruptVersionBlob(t, b, "b", "k", vidA2) // max-VID, corrupt (would-be marker)

		_, err := b.HeadObject(ctx, "b", "k")
		require.Error(t, err, "corrupt latest must NOT resurrect the older live version")
	})

	t.Run("specific-version: corrupt sibling VID poisons a clean target read → error", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "b"))
		setVersioningForTest(t, b, "b", "Enabled")
		seedVersionBlob(t, b, "b", "k", vidA1, PutObjectMetaCmd{ETag: "clean-target"})
		seedCorruptVersionBlob(t, b, "b", "k", vidB1) // sibling, corrupt

		_, err := b.HeadObjectVersion(ctx, "b", "k", vidA1)
		require.Error(t, err, "an undecodable sibling version makes the key's version set untrustworthy → fail closed")
	})

	t.Run("GetObjectTags: corrupt blob → error", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "b"))
		setVersioningForTest(t, b, "b", "Enabled")
		seedVersionBlob(t, b, "b", "k", vidA1, PutObjectMetaCmd{ETag: "v1"})
		seedCorruptVersionBlob(t, b, "b", "k", vidA2)

		_, err := b.GetObjectTags("b", "k", "")
		require.Error(t, err)
	})

	t.Run("clean on read still works (no false fail)", func(t *testing.T) {
		b := newTestDistributedBackend(t)
		require.NoError(t, b.CreateBucket(ctx, "b"))
		setVersioningForTest(t, b, "b", "Enabled")
		seedVersionBlob(t, b, "b", "k", vidA1, PutObjectMetaCmd{ETag: "v1"})
		seedVersionBlob(t, b, "b", "k", vidA2, PutObjectMetaCmd{ETag: "v2-latest"})

		obj, err := b.HeadObject(ctx, "b", "k")
		require.NoError(t, err)
		require.Equal(t, "v2-latest", obj.ETag)
	})
}

// TestRead1DecodeStrictOffUnchanged confirms the off/pending path keeps the
// tolerant reader: a corrupt blob is dropped (availability-first), NOT an error.
func TestRead1DecodeStrictCorruptBlobFailsClosed(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")
	seedVersionBlob(t, b, "b", "k", vidA1, PutObjectMetaCmd{ETag: "older-live"})
	seedCorruptVersionBlob(t, b, "b", "k", vidA2)

	// Blob-primary: the versioned read is decode-strict. An undecodable sibling blob
	// could BE the authoritative latest, so the read fails closed rather than
	// tolerantly dropping it and resurrecting an older live version. (Was the old
	// off-path tolerant derive.)
	_, err := b.HeadObject(ctx, "b", "k")
	require.Error(t, err, "decode-strict: a corrupt per-version blob fails the read closed")
}
