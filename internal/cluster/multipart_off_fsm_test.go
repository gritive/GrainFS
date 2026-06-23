package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestMultipart_CreateWritesNoFSMManifest is the M2b hinge guard: Create must
// no longer write the FSM `mpu:` manifest key — the manifest lives only on the
// .qmeta_mpu blob. UploadPart and ListParts validate the session off that blob,
// and Abort removes it so a subsequent UploadPart fails ErrUploadNotFound.
func TestMultipart_CreateWritesNoFSMManifest(t *testing.T) {
	b, _ := newTestDistributedBackendWithDB(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	up, err := b.CreateMultipartUpload(ctx, "bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, up.UploadID)

	// No FSM mpu: manifest key was written.
	err = b.store.View(func(txn MetadataTxn) error {
		_, gerr := txn.Get(b.ks().MultipartKey(up.UploadID))
		return gerr
	})
	require.ErrorIs(t, err, ErrMetaKeyNotFound, "Create must not write the FSM mpu: key")

	// The session is usable off the blob: UploadPart + ListParts succeed.
	part, err := b.UploadPart(ctx, "bucket", "mp.bin", up.UploadID, 1, bytes.NewReader([]byte("part-1")), "")
	require.NoError(t, err)
	require.Equal(t, 1, part.PartNumber)

	listed, err := b.ListParts(ctx, "bucket", "mp.bin", up.UploadID, 100)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, part.ETag, listed[0].ETag)

	// Abort removes the manifest blob; the session is then gone.
	require.NoError(t, b.AbortMultipartUpload(ctx, "bucket", "mp.bin", up.UploadID))

	_, err = b.UploadPart(ctx, "bucket", "mp.bin", up.UploadID, 2, bytes.NewReader([]byte("part-2")), "")
	require.ErrorIs(t, err, storage.ErrUploadNotFound, "UploadPart after Abort must fail ErrUploadNotFound")

	_, err = b.ListParts(ctx, "bucket", "mp.bin", up.UploadID, 100)
	require.ErrorIs(t, err, storage.ErrUploadNotFound, "ListParts after Abort must fail ErrUploadNotFound")
}

// TestMultipart_CompleteOffFSM_FirstCompleteSucceeds is the hinge correctness
// guard: with the manifest off the FSM, the FIRST Complete must succeed (the
// proposer validates the manifest blob; the apply no longer gates on the mpu:
// key) and the object must be readable back.
func TestMultipart_CompleteOffFSM_FirstCompleteSucceeds(t *testing.T) {
	b, _ := newTestDistributedBackendWithDB(t)
	configureChunkedMultipartTestBackend(b)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	up, err := b.CreateMultipartUpload(ctx, "bucket", "mp.bin", "application/octet-stream")
	require.NoError(t, err)

	payload := []byte("complete-off-fsm-payload")
	part, err := b.UploadPart(ctx, "bucket", "mp.bin", up.UploadID, 1, bytes.NewReader(payload), "")
	require.NoError(t, err)

	obj, err := b.CompleteMultipartUpload(ctx, "bucket", "mp.bin", up.UploadID, []storage.Part{*part})
	require.NoError(t, err)
	require.NotNil(t, obj)
	require.Equal(t, int64(len(payload)), obj.Size)

	rc, _, err := b.GetObject(ctx, "bucket", "mp.bin")
	require.NoError(t, err)
	got := make([]byte, len(payload))
	_, _ = rc.Read(got)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got)
}
