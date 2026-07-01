package packblob

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

// newTestDEKPackedBackend builds a production-shaped (DEK-sealed) PackedBackend so
// the small-object path stores ciphertext in the gen-aware blob store.
func newTestDEKPackedBackend(t *testing.T) *PackedBackend {
	t.Helper()
	dir := t.TempDir()
	inner := cluster.NewSingletonBackendForTest(t)

	kek := make([]byte, encrypt.KEKSize)
	clusterID := make([]byte, 16)
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err)

	pb, err := NewPackedBackendWithOptions(inner, dir+"/blobs", 64*1024, PackedBackendOptions{
		DEKKeeper: keeper,
		ClusterID: clusterID,
	})
	require.NoError(t, err)
	t.Cleanup(func() { pb.Close() })
	return pb
}

// TestCopyObject_EncryptedReferenceCopyDecryptable is the prod-copy regression
// guard for the AAD bug class fixed in #667. The accelerator path
// (CopyObjectWithRequest) reference-copies a packed object: the dst index entry
// points at the SAME encrypted blob location as the source. This stays
// decryptable under dst because packblob seals/opens each blob entry with an AAD
// key stored IN the entry — Read derives it from the entry header (BlobStore.Read
// takes only a BlobLocation), NOT from the caller's object key. That is the exact
// property the test-fixture LocalBackend lacked: its segment AAD bound
// (bucket,key,blobID) and was reconstructed from the dst identity at read time,
// so a raw byte-copy to the dst path produced an un-decryptable object.
//
// If packblob ever recomputes the read AAD from the caller's object key (or a copy
// rebinds the AAD), this guard goes RED.
func TestCopyObject_EncryptedReferenceCopyDecryptable(t *testing.T) {
	pb := newTestDEKPackedBackend(t)
	ctx := context.Background()
	require.NoError(t, pb.CreateBucket(ctx, "src"))
	require.NoError(t, pb.CreateBucket(ctx, "dst"))

	const plaintext = "encrypted-copy-me-via-reference"
	_, err := pb.PutObject(ctx, "src", "original.txt", strings.NewReader(plaintext), "text/plain")
	require.NoError(t, err)

	var copier storage.Copier = pb
	_, err = copier.CopyObject("src", "original.txt", "dst", "copy.txt")
	require.NoError(t, err)

	rc, _, err := pb.GetObject(ctx, "dst", "copy.txt")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext, string(got),
		"a reference-copied encrypted object must decrypt under the dst key")
}
