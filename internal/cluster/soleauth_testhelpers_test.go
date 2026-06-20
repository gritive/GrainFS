package cluster

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// seedVersionBlob writes a per-version quorum-meta blob for (bucket, key, vid)
// on b's local ShardService. The supplied PutObjectMetaCmd fields are copied
// verbatim; Bucket/Key/VersionID are always overridden to match the seed coordinates.
func seedVersionBlob(t *testing.T, b *DistributedBackend, bucket, key, vid string, cmd PutObjectMetaCmd) {
	t.Helper()
	cmd.Bucket = bucket
	cmd.Key = key
	cmd.VersionID = vid
	blob, err := EncodeCommand(CmdPutObjectMeta, cmd)
	require.NoError(t, err)
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal(bucket, filepath.Join(key, vid), blob, 0))
}

// setVersioningForTest sets a bucket's versioning state via Raft proposal.
// The bucket must already exist.
func setVersioningForTest(t *testing.T, b *DistributedBackend, bucket, state string) {
	t.Helper()
	require.NoError(t, b.SetBucketVersioning(bucket, state))
}

// setSoleAuthForTest advances the bucket's soleauth to the target state by
// walking valid transitions (mirrors seedSoleAuth in soleauth_test.go but
// operates on a DistributedBackend rather than an FSM directly).
func setSoleAuthForTest(t *testing.T, b *DistributedBackend, bucket, target string) {
	t.Helper()
	switch target {
	case soleAuthOff:
		// default — no proposal needed
	case soleAuthPending:
		require.NoError(t, b.SetBucketSoleAuthority(bucket, soleAuthPending))
	case soleAuthOn:
		require.NoError(t, b.SetBucketSoleAuthority(bucket, soleAuthPending))
		require.NoError(t, b.SetBucketSoleAuthority(bucket, soleAuthOn))
	default:
		t.Fatalf("setSoleAuthForTest: unknown target %q", target)
	}
}

// byVID finds the single object-manifest entry with VersionID == vid in the
// slice. Fails the test if none is found or if more than one matches.
func byVID(t *testing.T, objs []storage.SnapshotObject, vid string) storage.SnapshotObject {
	t.Helper()
	var found *storage.SnapshotObject
	for i := range objs {
		if objs[i].VersionID == vid {
			if found != nil {
				t.Fatalf("byVID: duplicate VersionID %q in manifest objects", vid)
			}
			cp := objs[i]
			found = &cp
		}
	}
	if found == nil {
		t.Fatalf("byVID: VersionID %q not found in %d objects", vid, len(objs))
	}
	return *found
}

// putMeta proposes a minimal object-version metadata record via Raft.
func (b *DistributedBackend) putMeta(bucket, key, versionID, etag string, size int64, ct string) error {
	return b.propose(context.Background(), CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      bucket,
		Key:         key,
		VersionID:   versionID,
		ETag:        etag,
		Size:        size,
		ContentType: ct,
		ModTime:     time.Now().UnixMilli(),
	})
}
