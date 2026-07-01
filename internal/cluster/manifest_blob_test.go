package cluster

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestManifestBlob_RoundTripSiblingRoot verifies that the manifest blob
// primitive stores and retrieves an upload manifest at the sibling-root
// .qmeta_mpu/{bucket}/{uploadID} path, and that the file is invisible to the
// object-store walker (ScanQuorumMetaBucket).
func TestManifestBlob_RoundTripSiblingRoot(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	m := clusterMultipartMeta{Bucket: "bkt", Key: "k", ContentType: "text/plain", CreatedAt: 100}
	require.NoError(t, b.writeManifestBlob(ctx, m, "up-1", b.selfNodeIDs())) // single node placement
	got, ok, err := b.readManifestBlob("bkt", "up-1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "k", got.Key)
	require.DirExists(t, filepath.Join(b.dataDir0(), ".qmeta_mpu", "bkt")) // add b.dataDir0() accessor if absent
	objs, _ := b.shardSvc.ScanQuorumMetaBucket("bkt", "")                  // ShardService, not backend
	require.Empty(t, objs, "manifest blob invisible to the object-store walker")
	require.NoError(t, b.deleteManifestBlob("bkt", "up-1"))
	_, ok, _ = b.readManifestBlob("bkt", "up-1")
	require.False(t, ok)
}

func TestLocalManifestStore_RoundTripAndStrictScan(t *testing.T) {
	store := NewLocalManifestStore([]string{t.TempDir()})
	meta := clusterMultipartMeta{Bucket: "bkt", Key: "k", ContentType: "text/plain", CreatedAt: 100}
	raw, err := marshalClusterMultipartMeta(meta)
	require.NoError(t, err)

	require.NoError(t, store.Write("bkt", "up-1", raw))

	got, ok, err := store.Read("bkt", "up-1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, raw, got)

	entries, err := store.ScanStrict("bkt")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "up-1", entries[0].UploadID)
	require.Equal(t, "k", entries[0].Meta.Key)

	require.NoError(t, store.Delete("bkt", "up-1"))
	_, ok, err = store.Read("bkt", "up-1")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestLocalManifestStore_WriteFsyncsDirectoryAfterRename(t *testing.T) {
	root := t.TempDir()
	store := NewLocalManifestStore([]string{root})
	var synced []string
	store.syncDirHook = func(dir string) error {
		synced = append(synced, dir)
		return nil
	}
	meta := clusterMultipartMeta{Bucket: "bkt", Key: "k", ContentType: "text/plain", CreatedAt: 100}
	raw, err := marshalClusterMultipartMeta(meta)
	require.NoError(t, err)

	require.NoError(t, store.Write("bkt", "up-1", raw))

	require.Contains(t, synced, filepath.Join(root, manifestMPUSubDir, "bkt"))
}

// TestUnpackManifestEntries_CorruptLengthNoPanic verifies that unpackManifestEntries
// returns an error (not a panic) when fed a truncated buffer or a length-prefix whose
// high bit is set (which a signed-int decode would turn negative).
func TestUnpackManifestEntries_CorruptLengthNoPanic(t *testing.T) {
	t.Run("truncated_input", func(t *testing.T) {
		// Only 3 bytes — not enough for a 4-byte length prefix.
		_, err := unpackManifestEntries([]byte{0x00, 0x00, 0x01})
		require.Error(t, err)
	})

	t.Run("oversized_length_high_bit_set", func(t *testing.T) {
		// Length prefix with bit-31 set: 0x80000010 = 2147483664.
		// A signed-int decode would produce a negative n, causing data[:n] to panic.
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, 0x80000010)
		_, err := unpackManifestEntries(buf)
		require.Error(t, err)
	})

	t.Run("claimed_length_exceeds_remaining", func(t *testing.T) {
		// Valid-looking length prefix (16 bytes) but only 4 bytes of payload follow.
		buf := make([]byte, 8)
		binary.BigEndian.PutUint32(buf[:4], 16)
		// remaining 4 bytes are zero-payload, clearly shorter than claimed 16
		_, err := unpackManifestEntries(buf)
		require.Error(t, err)
	})
}
