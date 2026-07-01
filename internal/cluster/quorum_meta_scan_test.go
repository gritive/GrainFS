package cluster

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQuorumMetaScanners_SkipInFlightTempFiles is the regression guard for the
// flaky TestShardPlacementMonitor_RepairsMissingSegmentShard_EndToEnd ("repair:
// only 0/1 other shards readable"): writeQuorumMetaLocal's atomic publish
// creates ".qmeta-*.tmp" in the SAME directory as its rename target, and the
// temp holds a complete decodable meta blob. A store walker that treats every
// file as {bucket}/{key} fabricates an object keyed by the temp name whenever a
// scan races an in-flight write — the placement monitor then reports phantom
// "missing local shard" entries (shard key ".qmeta-<ts>.tmp/segments/<id>") and
// the triggered repair fails because no shards exist under that path. LIST's
// ScanQuorumMetaBucket has the same walk and would return a duplicate entry.
//
// Simulate the race deterministically: place a copy of a valid blob as a
// ".qmeta-*.tmp" file next to the real key. RED without isQuorumMetaTempName
// skips in IterQuorumMetaECShardTargets/ScanQuorumMetaBucket; GREEN with them.
func TestQuorumMetaScanners_SkipInFlightTempFiles(t *testing.T) {
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(t.TempDir(), nil, WithShardDEKKeeper(keeper, clusterID))

	cmd := PutObjectMetaCmd{
		Bucket: "b", Key: "obj", Size: 2048, ContentType: "application/octet-stream",
		ETag: "etag", VersionID: "v1", ECData: 1, ECParity: 1, NodeIDs: []string{"self", "self"},
		Segments: []SegmentMetaEntry{{
			BlobID: "0123456789abcdef", Size: 2048, ECData: 1, ECParity: 1,
			StripeBytes: 1 << 20, NodeIDs: []string{"self", "self"},
		}},
	}
	blob, err := encodeQuorumMetaBlob(cmd)
	require.NoError(t, err)
	require.NoError(t, svc.writeQuorumMetaLocal("b", "obj", blob))

	// The in-flight temp: same directory, same content, not yet renamed.
	bucketDir := filepath.Join(svc.dataDirs[0], quorumMetaSubDir, "b")
	require.NoError(t, os.WriteFile(filepath.Join(bucketDir, ".qmeta-1234567890.tmp"), blob, 0o644))

	var targets []ECShardScanTarget
	require.NoError(t, svc.IterQuorumMetaECShardTargets(func(tg ECShardScanTarget) error {
		targets = append(targets, tg)
		return nil
	}))
	require.Len(t, targets, 1, "scan must emit only the real key's segment target, not a phantom from the temp file")
	require.Equal(t, "obj", targets[0].ObjectKey)
	require.Equal(t, "obj/segments/0123456789abcdef", targets[0].ShardKey)

	cmds, err := svc.ScanQuorumMetaBucket("b", "")
	require.NoError(t, err)
	require.Len(t, cmds, 1, "bucket scan must not return a duplicate entry decoded from the temp file")
	require.Equal(t, "obj", cmds[0].Key)
}
