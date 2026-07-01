package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestNonVersionedDelete_BlobAuthoritative proves a non-versioned DELETE is
// blob-authoritative (raft-free): it overwrites the latest-only key with an
// IsDeleteMarker tombstone (fail-closed), so GET folds to 404 and LIST omits the
// key — with NO CmdDeleteObject propose (a non-versioned object has no FSM obj:
// record to delete).
func TestNonVersionedDelete_BlobAuthoritative(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "plainbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	// No SetBucketVersioning → non-versioned (latest-only authority).

	_, err := b.PutObject(ctx, bkt, key, bytes.NewReader([]byte("payload")), "application/octet-stream")
	require.NoError(t, err)

	// Sanity: present before delete.
	_, err = b.HeadObject(ctx, bkt, key)
	require.NoError(t, err)

	_, err = b.DeleteObjectReturningMarker(bkt, key)
	require.NoError(t, err)

	// GET/HEAD fold the latest-only delete tombstone → 404.
	_, err = b.HeadObject(ctx, bkt, key)
	require.ErrorIs(t, err, storage.ErrObjectNotFound, "deleted non-versioned object must read 404")

	// LIST omits the deleted key.
	objs, err := b.ListObjects(ctx, bkt, "", 1000)
	require.NoError(t, err)
	for _, o := range objs {
		require.NotEqual(t, key, o.Key, "deleted non-versioned object must not appear in LIST")
	}
}

// TestNonVersionedDelete_AbsentKeyIsNoop proves deleting a never-existed key on a
// non-versioned bucket is a no-op success (S3 delete-of-absent semantics) — no
// error, no tombstone, raft-free.
func TestNonVersionedDelete_AbsentKeyIsNoop(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt = "plainbkt"
	require.NoError(t, b.CreateBucket(ctx, bkt))

	_, err := b.DeleteObjectReturningMarker(bkt, "never-existed")
	require.NoError(t, err, "delete of an absent non-versioned key must succeed (no-op)")

	_, err = b.HeadObject(ctx, bkt, "never-existed")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}
