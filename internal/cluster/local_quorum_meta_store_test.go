package cluster

import (
	"errors"
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
