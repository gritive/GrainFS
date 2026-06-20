package cluster

import (
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestForceLockedVersionLeaves proves the ForceLocked variants:
//   - writeQuorumMetaVersionLocalForceLocked bypasses the write-time LWW guard
//     so an older blob (lower MetaSeq) can overwrite a strictly-newer on-disk blob
//     when the caller holds the per-bucket write-lock.
//   - deleteQuorumMetaVersionLocalForceLocked removes the blob under the held lock.
func TestForceLockedVersionLeaves(t *testing.T) {
	s := newShardServiceTestWithDataDir(t)
	sub := path.Join("k", "v1")
	require.NoError(t, s.writeQuorumMetaVersionLocal("b", sub, mustEncode(t, PutObjectMetaCmd{Bucket: "b", Key: "k", VersionID: "v1", ModTime: 200, MetaSeq: 9, ETag: "new"}), 0))
	mu := s.bucketSoleAuthLock("b")
	mu.Lock()
	require.NoError(t, s.writeQuorumMetaVersionLocalForceLocked("b", sub, mustEncode(t, PutObjectMetaCmd{Bucket: "b", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 1, ETag: "old"})))
	gotOld, _ := s.readQuorumMetaVersionsLocal("b", "k")
	require.Equal(t, "old", gotOld[0].ETag) // guard bypassed
	require.NoError(t, s.deleteQuorumMetaVersionLocalForceLocked("b", "k", "v1"))
	mu.Unlock()
	gone, _ := s.readQuorumMetaVersionsLocal("b", "k")
	require.Empty(t, gone)
}
