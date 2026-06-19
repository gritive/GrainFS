package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFSM_DeleteMultipartDone_GCsMarkers verifies that CmdDeleteMultipartDone
// batch-deletes mpudone markers while leaving unlisted markers intact.
func TestFSM_DeleteMultipartDone_GCsMarkers(t *testing.T) {
	fsm := NewFSM(newTestStore(t), newStateKeyspaceEmpty())

	// Write two done-markers by completing two multipart uploads.
	for _, id := range []string{"u-gc-1", "u-gc-2"} {
		create, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
			UploadID: id, Bucket: "bkt", Key: id + ".bin",
			ContentType: "application/octet-stream", CreatedAt: 100,
		})
		require.NoError(t, err)
		require.NoError(t, fsm.Apply(create))

		complete, err := EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
			Bucket: "bkt", Key: id + ".bin", UploadID: id,
			Size: 64, ContentType: "application/octet-stream",
			ETag: "etag-" + id, ModTime: 200, VersionID: "v1",
		})
		require.NoError(t, err)
		require.NoError(t, fsm.Apply(complete))
	}

	// Also write a third marker that must NOT be deleted.
	const survivor = "u-survivor"
	survCreate, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID: survivor, Bucket: "bkt", Key: survivor + ".bin",
		ContentType: "application/octet-stream", CreatedAt: 100,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(survCreate))

	survComplete, err := EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket: "bkt", Key: survivor + ".bin", UploadID: survivor,
		Size: 64, ContentType: "application/octet-stream",
		ETag: "etag-survivor", ModTime: 200, VersionID: "v1",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(survComplete))

	// Delete only the first two markers.
	del, err := EncodeCommand(CmdDeleteMultipartDone, DeleteMultipartDoneCmd{
		UploadIDs: []string{"u-gc-1", "u-gc-2"},
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(del))

	// u-gc-1 and u-gc-2 must be gone.
	require.NoError(t, fsm.db.View(func(txn MetadataTxn) error {
		for _, id := range []string{"u-gc-1", "u-gc-2"} {
			_, err := txn.Get(fsm.keys.MultipartDoneKey(id))
			require.ErrorIs(t, err, ErrMetaKeyNotFound, "marker for %s should be deleted", id)
		}
		return nil
	}))

	// u-survivor must still exist.
	require.NoError(t, fsm.db.View(func(txn MetadataTxn) error {
		_, err := txn.Get(fsm.keys.MultipartDoneKey(survivor))
		require.NoError(t, err, "marker for %s should survive", survivor)
		return nil
	}))
}

// TestFSM_DeleteMultipartDone_IdempotentForMissingKey verifies that deleting a
// non-existent marker is silently ignored (idempotent GC).
func TestFSM_DeleteMultipartDone_IdempotentForMissingKey(t *testing.T) {
	fsm := NewFSM(newTestStore(t), newStateKeyspaceEmpty())

	del, err := EncodeCommand(CmdDeleteMultipartDone, DeleteMultipartDoneCmd{
		UploadIDs: []string{"no-such-upload"},
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(del), "delete of non-existent marker must not error")
}
