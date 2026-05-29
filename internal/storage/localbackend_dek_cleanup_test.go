package storage

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// (newDEKLocalBackend lives in local_test.go.)

// TestPreferWriteAtFalseWhenDataEncrypted: PreferWriteAt must gate on whether the
// DATA is encrypted (b.segEnc), not the retired meta encryptor. A DEK backend
// must not advertise in-place WriteAt for internal buckets (chunked-AEAD data
// cannot be written in place). RED on master: gated on the always-nil encryptor,
// so it returns true even on an encrypted backend.
func TestPreferWriteAtFalseWhenDataEncrypted(t *testing.T) {
	dek := newDEKLocalBackend(t)
	require.False(t, dek.PreferWriteAt("__grainfs_vfs_x"),
		"encrypted (segEnc) backend must not advertise in-place WriteAt for internal buckets")

	plain, err := NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = plain.Close() })
	require.True(t, plain.PreferWriteAt("__grainfs_vfs_x"),
		"plaintext backend keeps WriteAt for internal buckets")
}

// TestCopyObjectSegmentedEncryptedReEncodes: copying a segmented object on a DEK
// backend must re-encode through the decrypted reader so the destination segment
// files seal under the dst AAD. RED on master: the raw-copyFile fast-path (gated
// on the always-nil meta encryptor) copies src ciphertext to the dst path, whose
// segment AAD binds (dstBucket,dstKey,blobID), so the dst is un-decryptable.
func TestCopyObjectSegmentedEncryptedReEncodes(t *testing.T) {
	b := newDEKLocalBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt"))

	data := bytes.Repeat([]byte("Z"), 10<<20) // 10 MiB → segmented
	_, err := b.AppendObject(ctx, "bkt", "src", 0, bytes.NewReader(data))
	require.NoError(t, err)

	_, err = b.CopyObject("bkt", "src", "bkt", "dst")
	require.NoError(t, err)

	rc, _, err := b.GetObject(ctx, "bkt", "dst")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	require.NoError(t, err, "copied encrypted segments must decrypt at the dst (re-encoded, not raw-copied)")
	require.Equal(t, data, got)
}
