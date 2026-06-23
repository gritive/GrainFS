package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestCoalescedRefsFromMeta_PreservesAllFields asserts F5: coalescedRefsFromMeta
// must carry EVERY field (not only CoalescedID) so downstream GC/reachability
// consumers see the coalesced blob's size, shardKey, EC placement, and nodeIDs.
func TestCoalescedRefsFromMeta_PreservesAllFields(t *testing.T) {
	in := []CoalescedShardRef{{
		CoalescedID: "c1",
		Size:        1234,
		ETag:        "etag-c1",
		ShardKey:    "k/coalesced/c1",
		Version:     1,
		ECData:      4,
		ECParity:    2,
		StripeBytes: 8192,
		NodeIDs:     []string{"n1", "n2", "n3", "n4", "n5", "n6"},
	}}
	out := coalescedRefsFromMeta(in)
	require.Len(t, out, 1)
	got := out[0]
	require.Equal(t, "c1", got.CoalescedID)
	require.Equal(t, int64(1234), got.Size)
	require.Equal(t, "etag-c1", got.ETag)
	require.Equal(t, "k/coalesced/c1", got.ShardKey)
	require.Equal(t, uint8(4), got.ECData)
	require.Equal(t, uint8(2), got.ECParity)
	require.Equal(t, uint32(8192), got.StripeBytes)
	require.Equal(t, []string{"n1", "n2", "n3", "n4", "n5", "n6"}, got.NodeIDs)
}

// TestSnapshotObjectFromQuorumCmd_CarriesCoalesced asserts F5: the blob-backed
// GC known-set mapping must copy Coalesced refs so a blob-resident coalesced
// object's coalesced blob is reachable (ChunkLocators) and never premature-GC'd.
func TestSnapshotObjectFromQuorumCmd_CarriesCoalesced(t *testing.T) {
	cmd := PutObjectMetaCmd{
		Bucket: "b",
		Key:    "k",
		Size:   10,
		Coalesced: []CoalescedShardRef{{
			CoalescedID: "c1",
			Size:        10,
			ShardKey:    "k/coalesced/c1",
			ECData:      4,
			ECParity:    2,
			NodeIDs:     []string{"n1", "n2", "n3", "n4", "n5", "n6"},
		}},
	}
	so := snapshotObjectFromQuorumCmd("b", cmd, true)
	require.Len(t, so.Coalesced, 1)
	require.Equal(t, "c1", so.Coalesced[0].CoalescedID)
	require.Equal(t, "k/coalesced/c1", so.Coalesced[0].ShardKey)

	// ChunkLocators must include the coalesced blob so the GC known-set pins it.
	locs := so.ChunkLocators()
	require.Contains(t, locs, storage.ParseLocator("c1").String())
}

// TestShardTargetStillReferenced_SeesBlobResidentCoalesced asserts F5: the
// quarantine reachability check must also see a blob-resident coalesced object's
// coalesced shard (it previously read only BadgerDB, treating blob-backed
// appendables as de-referenced).
func TestShardTargetStillReferenced_SeesBlobResidentCoalesced(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	bucket, key := "b", "k"
	require.NoError(t, b.CreateBucket(ctx, bucket))

	// Build a blob-resident appendable object with a coalesced ref, written via the
	// quorum-meta blob (no FSM obj: record).
	appendForCrashTest(t, b, bucket, key, "aaaa")
	appendForCrashTest(t, b, bucket, key, "bbbb")
	require.NoError(t, b.processCoalesceJobB2(ctx, coalesceJob{Bucket: bucket, Key: key}))

	cmd, err := b.readQuorumMetaCmd(bucket, key)
	require.NoError(t, err)
	require.Len(t, cmd.Coalesced, 1)
	shardKey := cmd.Coalesced[0].ShardKey
	require.NotEmpty(t, shardKey)

	target := ECShardScanTarget{
		Kind:      ECShardCoalesced,
		Bucket:    bucket,
		ObjectKey: key,
		VersionID: cmd.VersionID,
		ShardKey:  shardKey,
	}
	require.True(t, b.ShardTargetStillReferenced(ctx, target),
		"a blob-resident coalesced shard must be seen as still referenced")
}
