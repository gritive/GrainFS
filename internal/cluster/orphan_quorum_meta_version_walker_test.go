package cluster

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeVersionBlob writes a per-version quorum-meta blob (an encoded
// PutObjectMetaCmd) to this node's local .quorum_meta_versions subtree and
// back-dates its mtime so the age gate treats it as old (backdate > floor).
func writeVersionBlob(t *testing.T, b *DistributedBackend, bucket, key, versionID string, backdate time.Duration) string {
	t.Helper()
	blob, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
		ECData:    1,
		NodeIDs:   []string{b.currentSelfAddr()},
	})
	require.NoError(t, err)
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal(bucket, filepath.Join(key, versionID), blob))
	target := filepath.Join(b.shardSvc.DataDirs()[0], quorumMetaVersionsSubDir, bucket, key, versionID)
	past := time.Now().Add(-backdate)
	require.NoError(t, os.Chtimes(target, past, past))
	return target
}

// writeTombstoneBlob writes a hard-delete tombstone (IsHardDeleted) per-version blob
// for (bucket,key,vid) with the given placement nodes and back-dates its mtime past
// the age floor.
func writeTombstoneBlob(t *testing.T, b *DistributedBackend, bucket, key, versionID string, nodeIDs []string, backdate time.Duration) string {
	t.Helper()
	blob, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:        bucket,
		Key:           key,
		VersionID:     versionID,
		ECData:        1,
		NodeIDs:       nodeIDs,
		IsHardDeleted: true,
		MetaSeq:       1,
	})
	require.NoError(t, err)
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal(bucket, filepath.Join(key, versionID), blob))
	target := filepath.Join(b.shardSvc.DataDirs()[0], quorumMetaVersionsSubDir, bucket, key, versionID)
	past := time.Now().Add(-backdate)
	require.NoError(t, os.Chtimes(target, past, past))
	return target
}

func collectOrphanVersions(t *testing.T, b *DistributedBackend) []string {
	t.Helper()
	var got []string
	require.NoError(t, b.WalkOrphanQuorumMetaVersions(func(_, _, versionID, _ string) error {
		got = append(got, versionID)
		return nil
	}))
	return got
}

// TestPerVersionBlobReclaimable_DataBlob proves a data blob is reclaimable iff the
// authoritative cross-cluster state for its version is a tombstone (this node missed
// the hard delete) — and kept when it is still the live authority.
func TestPerVersionBlobReclaimable_DataBlob(t *testing.T) {
	t.Run("data blob superseded by a tombstone → reclaimable", func(t *testing.T) {
		b := orphanWalkerBackend(t)
		self := b.currentSelfAddr()
		// The authoritative version state for vX is a tombstone (here served by self;
		// in a cluster it would win the LWW from a peer that did the hard delete).
		writeTombstoneBlob(t, b, "bkt", "k", "vX", []string{self}, oldEnough)
		dataCmd := PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "vX", NodeIDs: []string{self}}
		require.True(t, b.perVersionBlobReclaimable(dataCmd))
	})

	t.Run("live data blob (no tombstone) → kept", func(t *testing.T) {
		b := orphanWalkerBackend(t)
		self := b.currentSelfAddr()
		writeVersionBlob(t, b, "bkt", "k", "vLive", oldEnough) // data blob, no tombstone
		dataCmd := PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "vLive", NodeIDs: []string{self}}
		require.False(t, b.perVersionBlobReclaimable(dataCmd))
	})
}

// TestPerVersionBlobReclaimable_Tombstone proves a tombstone is GC'd only once the
// delete has converged (all placement nodes reachable, none holds a data blob), and
// kept (fail-closed) when a placement node is unreachable.
func TestPerVersionBlobReclaimable_Tombstone(t *testing.T) {
	t.Run("converged (single placement node, no data blob) → GC", func(t *testing.T) {
		b := orphanWalkerBackend(t)
		self := b.currentSelfAddr()
		writeTombstoneBlob(t, b, "bkt", "k", "vT", []string{self}, oldEnough)
		tomb := PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "vT", NodeIDs: []string{self}, IsHardDeleted: true}
		require.True(t, b.perVersionBlobReclaimable(tomb))
	})

	t.Run("unreachable placement node → keep (fail-closed)", func(t *testing.T) {
		b := orphanWalkerBackend(t)
		self := b.currentSelfAddr()
		writeTombstoneBlob(t, b, "bkt", "k", "vT", []string{self}, oldEnough)
		tomb := PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "vT", NodeIDs: []string{self, "bogus-unreachable-peer"}, IsHardDeleted: true}
		require.False(t, b.perVersionBlobReclaimable(tomb), "cannot confirm propagation to an unreachable node")
	})
}

func TestWalkOrphanQuorumMetaVersions(t *testing.T) {
	b := orphanWalkerBackend(t)
	self := b.currentSelfAddr()

	// blob A: converged tombstone, old → reclaim candidate (GC).
	writeTombstoneBlob(t, b, "bkt", "k", "vA", []string{self}, oldEnough)
	// blob B: live data blob (no tombstone) → must NOT be yielded.
	writeVersionBlob(t, b, "bkt", "k", "vB", oldEnough)
	// blob C: tombstone but fresh (within age floor) → must NOT be yielded.
	writeTombstoneBlob(t, b, "bkt", "k", "vC", []string{self}, 0)

	got := collectOrphanVersions(t, b)
	require.Equal(t, []string{"vA"}, got)
}

func TestWalkOrphanQuorumMetaVersions_GateClosed_NoOp(t *testing.T) {
	b := orphanWalkerBackend(t)
	b.SetOrphanShardSweepGate(func() bool { return false }) // gate closed → fail-closed
	writeTombstoneBlob(t, b, "bkt", "k", "vA", []string{b.currentSelfAddr()}, oldEnough)
	require.Empty(t, collectOrphanVersions(t, b))
}

func TestDeleteOrphanQuorumMetaVersion_GCsConvergedTombstone(t *testing.T) {
	b := orphanWalkerBackend(t)
	target := writeTombstoneBlob(t, b, "bkt", "k", "vA", []string{b.currentSelfAddr()}, oldEnough)
	require.FileExists(t, target)

	require.NoError(t, b.DeleteOrphanQuorumMetaVersion("bkt", "k", "vA"))
	_, err := os.Stat(target)
	require.True(t, os.IsNotExist(err), "converged tombstone should be GC'd")
}

func TestDeleteOrphanQuorumMetaVersion_KeepsLiveDataBlob(t *testing.T) {
	b := orphanWalkerBackend(t)
	target := writeVersionBlob(t, b, "bkt", "k", "vA", oldEnough) // live data blob, no tombstone

	require.NoError(t, b.DeleteOrphanQuorumMetaVersion("bkt", "k", "vA"))
	require.FileExists(t, target, "a live data blob with no superseding tombstone must be kept")
}

// TestPerVersionReconcile_ReclaimsStaleDataBlob proves the reconciler removes a
// stale DATA blob once the version's authoritative state is a tombstone, stopping
// the derive-by-scan source from resurfacing the hard-deleted version, while a live
// sibling version is untouched.
func TestPerVersionReconcile_ReclaimsStaleDataBlob(t *testing.T) {
	b := orphanWalkerBackend(t)
	self := b.currentSelfAddr()
	// vLive: a live data blob (retained version).
	writeVersionBlob(t, b, "bkt", "k", "vLive", oldEnough)
	// vDead: hard-deleted — its authoritative state is a tombstone. (Single-node: the
	// tombstone IS the local blob; the reclaim of a stale DATA copy is exercised via
	// perVersionBlobReclaimable above and the C9 two-node e2e.)
	writeTombstoneBlob(t, b, "bkt", "k", "vDead", []string{self}, oldEnough)

	var cands [][3]string
	require.NoError(t, b.WalkOrphanQuorumMetaVersions(func(bk, k, v, _ string) error {
		cands = append(cands, [3]string{bk, k, v})
		return nil
	}))
	for _, c := range cands {
		require.NoError(t, b.DeleteOrphanQuorumMetaVersion(c[0], c[1], c[2]))
	}

	after, err := b.readQuorumMetaVersions("bkt", "k")
	require.NoError(t, err)
	require.Len(t, after, 1, "the converged tombstone is GC'd; only the live version remains")
	require.Equal(t, "vLive", after[0].VersionID)
}
