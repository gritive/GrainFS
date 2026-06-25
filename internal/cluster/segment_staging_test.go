package cluster

// PR1 (segment staging write): the staged-write primitive must encrypt a shard with the FINAL
// logical shard key as AAD even while writing it to a STAGING physical path, so a post-promote read
// (which decrypts with the final key) succeeds. The promote is a local rename of the staging dir to
// the final path. If the AAD were the staging key, ReadLocalShard(finalKey) would fail to decrypt —
// that is exactly what this locks against (codex plan-gate load-bearing fix #2).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteLocalShardStaged_AADIsFinalKey_PromoteReadable(t *testing.T) {
	svc, _ := newTestShardService(t)
	ctx := context.Background()
	const bucket = "b"
	const finalKey = "obj/segments/blob1"       // final logical shard key (drives AAD + final path)
	const stagingKey = ".segstaging/txn1/blob1" // staging physical path (no "/segments/")
	data := []byte("segment-shard-payload-0123456789")

	// Write to the STAGING physical path, but with the FINAL key as the encryption AAD.
	require.NoError(t, svc.writeLocalShardStaged(ctx, bucket, stagingKey, finalKey, 0, data))

	// Promote: local rename of the staging shard dir(s) to the final path.
	require.NoError(t, svc.PromoteLocalStagedShards(bucket, stagingKey, finalKey))

	// Reading at the FINAL key (which decrypts with the final-key AAD) must return the bytes —
	// proving the staged write used the final key as AAD, not the staging key.
	got, err := svc.ReadLocalShard(bucket, finalKey, 0)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

// PR1 Task 5: the orphan-shard walker must WHOLESALE-skip the .segstaging/ staging area, so an
// in-flight / crashed staged segment shard is never parsed as a fake full-object orphan and deleted.
// (Reclaim of abandoned staging is PR2's age-out walker, not the orphan-shard walker.)
func TestWalkOrphanShards_SkipsSegStaging(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	staging := writeShardLeaf(t, root, "bkt/.segstaging/txn1/blob1", []int{0}, oldEnough)

	got := collectOrphans(t, b, map[string]bool{})
	require.NotContains(t, got, staging,
		".segstaging staging dir must be skipped, never yielded as an orphan-shard candidate")
}
