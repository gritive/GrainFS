package cluster

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalQuorumMetaStore_SemanticCASAndLWW(t *testing.T) {
	store := NewLocalQuorumMetaStore([]string{t.TempDir()})
	base := PutObjectMetaCmd{
		Bucket:     "bkt",
		Key:        "obj",
		ETag:       "base",
		VersionID:  "v1",
		ModTime:    10,
		MetaSeq:    1,
		MetaSeqCAS: true,
	}
	baseBlob, err := encodeQuorumMetaBlob(base)
	require.NoError(t, err)
	require.NoError(t, store.writeQuorumMetaLocal("bkt", "obj", baseBlob))

	staleCAS := base
	staleCAS.ETag = "stale"
	staleCAS.MetaSeq = 1
	staleBlob, err := encodeQuorumMetaBlob(staleCAS)
	require.NoError(t, err)
	err = store.writeQuorumMetaLocal("bkt", "obj", staleBlob)
	require.True(t, errors.Is(err, errQuorumMetaCASReject), "stale CAS must reject, not blindly overwrite")

	olderLWW := base
	olderLWW.ETag = "older"
	olderLWW.ModTime = 1
	olderLWW.MetaSeqCAS = false
	olderBlob, err := encodeQuorumMetaBlob(olderLWW)
	require.NoError(t, err)
	require.NoError(t, store.writeQuorumMetaLocal("bkt", "obj", olderBlob), "LWW loss is a nil no-op")

	got, err := store.readQuorumMetaRawCmd("bkt", "obj")
	require.NoError(t, err)
	require.Equal(t, "base", got.ETag)
}

func TestLocalQuorumMetaStore_ScanQuorumMetaBucketPageHonorsMarkerLimitAndTombstones(t *testing.T) {
	store := NewLocalQuorumMetaStore([]string{t.TempDir()})
	for i := range 5 {
		cmd := PutObjectMetaCmd{
			Bucket:    "bkt",
			Key:       fmt.Sprintf("obj/%03d", i),
			ETag:      fmt.Sprintf("etag-%d", i),
			VersionID: fmt.Sprintf("v%d", i),
			ModTime:   int64(i + 1),
		}
		if i == 3 {
			cmd.IsDeleteMarker = true
		}
		blob, err := encodeQuorumMetaBlob(cmd)
		require.NoError(t, err)
		require.NoError(t, store.writeQuorumMetaLocal("bkt", cmd.Key, blob))
	}

	got, truncated, err := store.ScanQuorumMetaBucketPage("bkt", "obj/", "obj/001", 2)
	require.NoError(t, err)
	require.False(t, truncated)
	require.Len(t, got, 2)
	require.Equal(t, "obj/002", got[0].Key)
	require.Equal(t, "obj/004", got[1].Key)

	got, truncated, err = store.ScanQuorumMetaBucketPage("bkt", "obj/", "", 2)
	require.NoError(t, err)
	require.True(t, truncated)
	require.Len(t, got, 2)
	require.Equal(t, "obj/000", got[0].Key)
	require.Equal(t, "obj/001", got[1].Key)
}

func TestLocalQuorumMetaStore_WriteFsyncsDirectoryAfterRename(t *testing.T) {
	root := t.TempDir()
	store := NewLocalQuorumMetaStore([]string{root})
	var synced []string
	store.syncDirHook = func(dir string) error {
		synced = append(synced, dir)
		return nil
	}
	cmd := PutObjectMetaCmd{
		Bucket:    "bkt",
		Key:       "obj",
		ETag:      "etag",
		VersionID: "v1",
		ModTime:   10,
		MetaSeq:   1,
	}
	blob, err := encodeQuorumMetaBlob(cmd)
	require.NoError(t, err)

	require.NoError(t, store.writeQuorumMetaLocal("bkt", "obj", blob))

	require.Contains(t, synced, filepath.Join(root, quorumMetaSubDir, "bkt"))
}

func TestLocalQuorumMetaStore_VersionWriteFsyncsDirectoryAfterRename(t *testing.T) {
	root := t.TempDir()
	store := NewLocalQuorumMetaStore([]string{root})
	var synced []string
	store.syncDirHook = func(dir string) error {
		synced = append(synced, dir)
		return nil
	}
	cmd := PutObjectMetaCmd{
		Bucket:    "bkt",
		Key:       "obj",
		ETag:      "etag",
		VersionID: "v1",
		ModTime:   10,
		MetaSeq:   1,
	}
	blob, err := encodeQuorumMetaBlob(cmd)
	require.NoError(t, err)

	require.NoError(t, store.writeQuorumMetaVersionLocal("bkt", filepath.Join("obj", "v1"), blob))

	require.Contains(t, synced, filepath.Join(root, quorumMetaVersionsSubDir, "bkt", "obj"))
}
