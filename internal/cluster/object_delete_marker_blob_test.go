package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestDeleteObjectMarker_BlobDurableNoPropose proves the blob-primary soft delete:
// for a versioning-enabled bucket, DeleteObject writes a durable delete-marker
// per-version blob with NO raft propose (no FSM obj: record), the latest folds to
// 404, and ListObjectVersions shows both the object version and the marker.
func TestDeleteObjectMarker_BlobDurableNoPropose(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	require.NoError(t, b.SetBucketVersioning(bkt, "Enabled"))
	vid := putVersioned(t, b, ctx, bkt, key, "data")

	markerID, err := b.DeleteObjectReturningMarker(bkt, key)
	require.NoError(t, err)
	require.NotEmpty(t, markerID)

	// Latest folds to a delete marker → HEAD 404.
	_, err = b.HeadObject(ctx, bkt, key)
	require.ErrorIs(t, err, storage.ErrObjectNotFound)

	// The delete-marker per-version blob is durable.
	local, err := b.shardSvc.readQuorumMetaVersionsLocal(bkt, key)
	require.NoError(t, err)
	var marker *PutObjectMetaCmd
	for i := range local {
		if local[i].VersionID == markerID {
			marker = &local[i]
		}
	}
	require.NotNil(t, marker, "delete-marker per-version blob must be durable")
	require.True(t, marker.IsDeleteMarker)

	// Raft-free: no FSM obj: record for the marker.
	require.NoError(t, b.store.View(func(txn MetadataTxn) error {
		_, gerr := txn.Get(b.ks().ObjectMetaKeyV(bkt, key, markerID))
		require.ErrorIs(t, gerr, ErrMetaKeyNotFound, "delete marker must not write an FSM obj: record")
		return nil
	}))

	// ListObjectVersions shows the object version and the marker.
	vers, err := b.ListObjectVersions(ctx, bkt, key, 0)
	require.NoError(t, err)
	vids := map[string]bool{}
	for _, v := range vers {
		vids[v.VersionID] = true
	}
	require.True(t, vids[vid], "object version listed")
	require.True(t, vids[markerID], "delete marker listed")
}
