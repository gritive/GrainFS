package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestMultipartDone_RoundTrip(t *testing.T) {
	m := multipartDone{UploadID: "u1", Bucket: "b", Key: "k", VersionID: "v7", ModTime: 123}
	raw, err := marshalMultipartDone(m)
	require.NoError(t, err)
	got, err := unmarshalMultipartDone(raw)
	require.NoError(t, err)
	require.Equal(t, m, got)
}

// TestFSM_CompleteMultipart_MarkerExistsAfterComplete verifies that
// applyCompleteMultipart writes a multipartDone marker in the same txn as
// the manifest delete.
func TestFSM_CompleteMultipart_MarkerExistsAfterComplete(t *testing.T) {
	fsm := NewFSM(newTestStore(t), newStateKeyspaceEmpty())

	data, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID: "u-marker", Bucket: "bkt", Key: "obj.bin",
		ContentType: "application/octet-stream", CreatedAt: 100,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	complete, err := EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket: "bkt", Key: "obj.bin", UploadID: "u-marker",
		Size: 512, ContentType: "application/octet-stream",
		ETag: "etag1", ModTime: 200, VersionID: "v1",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(complete))

	// Marker must exist after completion.
	require.NoError(t, fsm.db.View(func(txn MetadataTxn) error {
		item, err := txn.Get(fsm.keys.MultipartDoneKey("u-marker"))
		require.NoError(t, err)
		raw, err := fsm.itemValueCopy(item)
		require.NoError(t, err)
		marker, err := unmarshalMultipartDone(raw)
		require.NoError(t, err)
		require.Equal(t, "u-marker", marker.UploadID)
		require.Equal(t, "bkt", marker.Bucket)
		require.Equal(t, "obj.bin", marker.Key)
		require.Equal(t, "v1", marker.VersionID)
		require.Equal(t, int64(200), marker.ModTime)
		return nil
	}))
}

// TestFSM_CompleteMultipart_MismatchErrors verifies that a second
// applyCompleteMultipart with the SAME uploadID but a DIFFERENT bucket/key
// returns an error (genuine conflict, not idempotent retry).
func TestFSM_CompleteMultipart_MismatchErrors(t *testing.T) {
	fsm := NewFSM(newTestStore(t), newStateKeyspaceEmpty())

	data, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID: "u-mismatch", Bucket: "bkt", Key: "original.bin",
		ContentType: "application/octet-stream", CreatedAt: 100,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	// First complete: manifest present, bucket/key = original.bin
	first, err := EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket: "bkt", Key: "original.bin", UploadID: "u-mismatch",
		Size: 512, ContentType: "application/octet-stream",
		ETag: "etag1", ModTime: 200, VersionID: "v1",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(first))

	// Second complete: manifest gone (marker present), but different key → must error.
	conflict, err := EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket: "bkt", Key: "other.bin", UploadID: "u-mismatch",
		Size: 1024, ContentType: "application/octet-stream",
		ETag: "etag2", ModTime: 300, VersionID: "v2",
	})
	require.NoError(t, err)
	err = fsm.Apply(conflict)
	require.Error(t, err)
	require.NotErrorIs(t, err, storage.ErrUploadNotFound)
}
