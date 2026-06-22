package cluster

import (
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// newShardServiceTestWithDataDir creates a bare ShardService (no transport, no
// DistributedBackend) for unit tests that only exercise the local quorum-meta
// primitives. It is the lightweight twin of newTestDistributedBackend for
// tests that do not need Raft or a full cluster.
func newShardServiceTestWithDataDir(t *testing.T) *ShardService {
	t.Helper()
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	return svc
}

// mustEncode encodes a PutObjectMetaCmd to a quorum-meta blob. Alias of
// mustEncodeMetaCmd for tests in this file.
func mustEncode(t *testing.T, cmd PutObjectMetaCmd) []byte {
	t.Helper()
	return mustEncodeMetaCmd(t, cmd)
}

// writeRawVersionBlobForTest writes arbitrary raw bytes directly to the
// per-version quorum-meta path for (bucket, key, vid), bypassing the
// LWW-guard write. Used to plant undecodable blobs for fail-closed tests.
func writeRawVersionBlobForTest(t *testing.T, s *ShardService, bucket, key, vid string, data []byte) {
	t.Helper()
	dir := filepath.Join(s.dataDirs[0], quorumMetaVersionsSubDir, bucket, key)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, vid), data, 0o644))
}

// TestScanQuorumMetaVersionsBucketAllStrict_FailsClosed proves the strict
// enumerator errors on an undecodable blob, while the tolerant one skips it.
func TestScanQuorumMetaVersionsBucketAllStrict_FailsClosed(t *testing.T) {
	s := newShardServiceTestWithDataDir(t)
	require.NoError(t, s.writeQuorumMetaVersionLocal("b", path.Join("k", "v1"), mustEncode(t, PutObjectMetaCmd{Bucket: "b", Key: "k", VersionID: "v1"})))
	writeRawVersionBlobForTest(t, s, "b", "k", "v2", []byte("\x00garbage"))

	tolerant, terr := s.ScanQuorumMetaVersionsBucketAll("b", "")
	require.NoError(t, terr)
	require.Len(t, tolerant, 1)
	_, serr := s.scanQuorumMetaVersionsBucketAllStrict("b", "")
	require.Error(t, serr)
}
