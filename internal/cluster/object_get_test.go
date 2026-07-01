package cluster

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestObjectAndPlacementFromCmd_AppendableDeriveFields verifies that
// objectAndPlacementFromCmd correctly materializes IsAppendable and Coalesced
// from a PutObjectMetaCmd onto the returned storage.Object.
// This is Task 1 F1: the read-derive must populate these fields so that
// a blob-resident appendable object reads back correctly.
func TestObjectAndPlacementFromCmd_AppendableDeriveFields(t *testing.T) {
	cmd := PutObjectMetaCmd{
		Bucket: "b", Key: "k", Size: 30, ModTime: 1700000000,
		VersionID: "v1",
		Segments: []SegmentMetaEntry{
			{BlobID: "s1", Size: 10, SegmentIdx: 0},
			{BlobID: "s2", Size: 20, SegmentIdx: 1},
		},
		Coalesced: []CoalescedShardRef{
			{CoalescedID: "c1", Size: 30, ETag: "etag1", ShardKey: "k/coalesced/c1"},
		},
		IsAppendable: true,
		MetaSeq:      5,
	}

	obj, _ := objectAndPlacementFromCmd(cmd)

	require.True(t, obj.IsAppendable, "storage.Object.IsAppendable must be true")
	require.Len(t, obj.Coalesced, 1)
	require.Equal(t, "c1", obj.Coalesced[0].CoalescedID)
	require.Equal(t, int64(30), obj.Coalesced[0].Size)
	require.Equal(t, "etag1", obj.Coalesced[0].ETag)
	require.Len(t, obj.Segments, 2, "Segments from cmd must be materialized")
	require.Equal(t, "s1", obj.Segments[0].BlobID)
	require.Equal(t, "s2", obj.Segments[1].BlobID)
}

// TestHeadObjectMeta_AppendableFromBlobOnly proves an appendable object whose
// metadata lives ONLY in the quorum-meta blob (no BadgerDB obj:/lat: record —
// the off-raft AppendObject path of Slice 1) is fully readable via HEAD + GET.
// This is the anchor for Task 7: the FSM carve-out fallback for appendable
// objects is dead because AppendObject no longer writes FSM object meta, so the
// read MUST be served by readQuorumMeta -> objectAndPlacementFromCmd.
func TestHeadObjectMeta_AppendableFromBlobOnly(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	const bkt, key = "bk", "appendable.bin"
	require.NoError(t, b.CreateBucket(ctx, bkt))

	body := []byte("hello")
	_, err := b.AppendObject(ctx, bkt, key, 0, bytes.NewReader(body))
	require.NoError(t, err)

	// The appendable manifest is durable in the quorum-meta blob.
	cmd, err := b.readQuorumMetaCmd(bkt, key)
	require.NoError(t, err)
	require.True(t, cmd.IsAppendable, "blob manifest must mark the object appendable")

	// Off-raft: NO BadgerDB obj:/lat: records back this appendable object.
	require.NoError(t, b.store.View(func(txn MetadataTxn) error {
		_, gerr := txn.Get(b.ks().ObjectMetaKey(bkt, key))
		require.ErrorIs(t, gerr, ErrMetaKeyNotFound, "append must not write an FSM latest obj: record")
		_, gerr = txn.Get(b.ks().LatestKey(bkt, key))
		require.ErrorIs(t, gerr, ErrMetaKeyNotFound, "append must not write an FSM lat: pointer")
		return nil
	}))

	// HEAD resolves the appendable object entirely from the blob.
	head, _, err := b.headObjectMeta(ctx, bkt, key)
	require.NoError(t, err)
	require.True(t, head.IsAppendable, "HEAD must derive IsAppendable from the blob")
	require.Equal(t, int64(len(body)), head.Size)

	// GET stitches the blob-derived segments and round-trips the bytes.
	rc, getObj, err := b.GetObject(ctx, bkt, key)
	require.NoError(t, err)
	defer rc.Close()
	require.True(t, getObj.IsAppendable)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, body, got, "appendable GET must round-trip byte-identical from the blob")
}

// TestHeadObjectMeta_PlainNonVersionedBlobReadable proves a plain
// (non-appendable, non-coalesced) non-versioned object resolves from its
// latest-only quorum-meta blob — the sole authority for non-versioned objects
// under blob-primary (the FSM obj: read fallback is removed).
func TestHeadObjectMeta_PlainNonVersionedBlobReadable(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	const bkt, key = "bk", "legacy.txt"
	require.NoError(t, b.CreateBucket(ctx, bkt))

	// Plain non-versioned object: only the latest-only quorum-meta blob exists.
	seedLatestBlob(t, b, bkt, key, PutObjectMetaCmd{
		Size: 11, ContentType: "text/plain", ETag: "etag-legacy",
		ModTime: 1700000000, NodeIDs: []string{b.currentSelfAddr()},
	})

	// HEAD must resolve via the latest-only blob and return the plain object.
	head, _, err := b.headObjectMeta(ctx, bkt, key)
	require.NoError(t, err)
	require.False(t, head.IsAppendable)
	require.Equal(t, int64(11), head.Size)
	require.Equal(t, "etag-legacy", head.ETag)
}
