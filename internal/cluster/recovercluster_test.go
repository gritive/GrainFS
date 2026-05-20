package cluster

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/raft"
)

func TestRecoverClusterPlanRejectsGroups(t *testing.T) {
	source := t.TempDir()
	target := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(source, "groups", "group-1"), 0o755))

	_, err := BuildRecoverClusterPlan(RecoverClusterOptions{
		SourceData:  source,
		TargetData:  target,
		NewNodeID:   "node-recovered",
		NewRaftAddr: "127.0.0.1:19000",
	})
	require.ErrorContains(t, err, "multi-Raft group recovery is not supported")
}

func TestRecoverClusterPlanRequiresOptions(t *testing.T) {
	_, err := BuildRecoverClusterPlan(RecoverClusterOptions{})
	require.ErrorContains(t, err, "source-data, target-data, new-node-id, and new-raft-addr are required")
}

func TestRecoverClusterPlanRequiresSnapshot(t *testing.T) {
	source := t.TempDir()
	target := t.TempDir()
	db, _, _, _, err := openRaftV2Stores(filepath.Join(source, "raft"))
	require.NoError(t, err)
	require.NoError(t, db.Close())

	_, err = BuildRecoverClusterPlan(RecoverClusterOptions{
		SourceData:  source,
		TargetData:  target,
		NewNodeID:   "node-recovered",
		NewRaftAddr: "127.0.0.1:19000",
	})
	require.ErrorIs(t, err, ErrRecoverClusterNoSnapshot)
}

func TestRecoverClusterExecuteRewritesMembershipAndMarker(t *testing.T) {
	source := t.TempDir()
	target := filepath.Join(t.TempDir(), "target")
	snapshotData := writeRecoverClusterSourceSnapshot(t, source, []raft.Server{
		{ID: "old-a", Suffrage: raft.Voter},
		{ID: "old-b", Suffrage: raft.Voter},
	})

	plan, err := BuildRecoverClusterPlan(RecoverClusterOptions{
		SourceData:  source,
		TargetData:  target,
		NewNodeID:   "node-recovered",
		NewRaftAddr: "127.0.0.1:19000",
	})
	require.NoError(t, err)
	require.Equal(t, int64(len(snapshotData)), plan.SnapshotSize)

	require.NoError(t, ExecuteRecoverClusterPlan(plan))

	dbSnap, _, _, snaps, err := openRaftV2StoresReadOnly(filepath.Join(target, "raft"))
	require.NoError(t, err)
	defer dbSnap.Close()
	snap, err := snaps.Latest()
	require.NoError(t, err)
	require.Equal(t, []raft.Server{{ID: "node-recovered", Suffrage: raft.Voter}}, snap.Servers)

	marker, err := LoadRecoverClusterMarker(target)
	require.NoError(t, err)
	require.NotNil(t, marker)
	require.False(t, marker.Writable)
	require.Equal(t, "node-recovered", marker.RecoveredNodeID)
	require.Len(t, marker.OriginalServers, 2)

	db, err := badger.Open(badgerutil.SmallOptions(filepath.Join(target, "meta")))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey("photos"))
		return err
	}))
}

func TestRecoverClusterMarkWritable(t *testing.T) {
	source := t.TempDir()
	target := filepath.Join(t.TempDir(), "target")
	writeRecoverClusterSourceSnapshot(t, source, []raft.Server{{ID: "old-a", Suffrage: raft.Voter}})
	plan, err := BuildRecoverClusterPlan(RecoverClusterOptions{
		SourceData:  source,
		TargetData:  target,
		NewNodeID:   "node-recovered",
		NewRaftAddr: "127.0.0.1:19000",
	})
	require.NoError(t, err)
	require.NoError(t, ExecuteRecoverClusterPlan(plan))

	require.NoError(t, MarkRecoverClusterWritable(target))
	marker, err := LoadRecoverClusterMarker(target)
	require.NoError(t, err)
	require.True(t, marker.Writable)
	require.False(t, marker.VerifiedAt.IsZero())
}

func TestRecoverClusterMarkWritableRequiresMarker(t *testing.T) {
	err := MarkRecoverClusterWritable(t.TempDir())
	require.ErrorContains(t, err, "recovery marker not found")
}

func TestRecoverClusterMarkWritableVerifiesTarget(t *testing.T) {
	target := t.TempDir()
	require.NoError(t, WriteRecoverClusterMarker(target, RecoverClusterMarker{
		Writable:        false,
		RecoveredNodeID: "node-recovered",
		SourceSnapshot:  SnapshotID{Index: 12, Term: 3},
		CreatedAt:       time.Now().UTC(),
	}))

	err := MarkRecoverClusterWritable(target)
	require.ErrorContains(t, err, "verify target meta db")
}

func TestRecoverClusterPlanRejectsRecoveryMarkerInTarget(t *testing.T) {
	source := t.TempDir()
	target := t.TempDir()
	writeRecoverClusterSourceSnapshot(t, source, []raft.Server{{ID: "old-a", Suffrage: raft.Voter}})
	require.NoError(t, os.MkdirAll(filepath.Join(target, "recovery"), 0o755))

	_, err := BuildRecoverClusterPlan(RecoverClusterOptions{
		SourceData:  source,
		TargetData:  target,
		NewNodeID:   "node-recovered",
		NewRaftAddr: "127.0.0.1:19000",
	})
	require.ErrorContains(t, err, "target is not fresh")
}

func TestRecoverClusterPlanRejectsJointSnapshotUnlessStripped(t *testing.T) {
	source := t.TempDir()
	target := filepath.Join(t.TempDir(), "target")
	writeRecoverClusterSourceSnapshotWithOptions(t, source, []raft.Server{
		{ID: "old-a", Suffrage: raft.Voter},
		{ID: "old-b", Suffrage: raft.Voter},
	}, raft.Snapshot{
		JointPhase:           raft.JointEntering,
		JointOldVoters:       []string{"old-a", "old-b"},
		JointNewVoters:       []string{"old-a", "old-c"},
		JointEnterIndex:      11,
		JointManagedLearners: []string{"learner-a"},
	})

	_, err := BuildRecoverClusterPlan(RecoverClusterOptions{
		SourceData:  source,
		TargetData:  target,
		NewNodeID:   "node-recovered",
		NewRaftAddr: "127.0.0.1:19000",
	})
	require.ErrorContains(t, err, "--strip-joint-state")

	plan, err := BuildRecoverClusterPlan(RecoverClusterOptions{
		SourceData:      source,
		TargetData:      target,
		NewNodeID:       "node-recovered",
		NewRaftAddr:     "127.0.0.1:19000",
		StripJointState: true,
	})
	require.NoError(t, err)
	require.Equal(t, raft.JointEntering, plan.JointPhase)
	require.Equal(t, []string{"old-a", "old-b"}, plan.JointOldVoters)
	require.Equal(t, []string{"old-a", "old-c"}, plan.JointNewVoters)
	require.Equal(t, uint64(11), plan.JointEnterIndex)
	require.Equal(t, []string{"learner-a"}, plan.JointManagedLearners)
}

func writeRecoverClusterSourceSnapshot(t *testing.T, dataDir string, servers []raft.Server) []byte {
	return writeRecoverClusterSourceSnapshotWithOptions(t, dataDir, servers, raft.Snapshot{})
}

func writeRecoverClusterSourceSnapshotWithOptions(t *testing.T, dataDir string, servers []raft.Server, extra raft.Snapshot) []byte {
	t.Helper()
	metaDir := filepath.Join(dataDir, "meta")
	db, err := badger.Open(badgerutil.SmallOptions(metaDir))
	require.NoError(t, err)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	cmd, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "photos"})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(cmd))
	data, err := fsm.Snapshot()
	require.NoError(t, err)
	require.NoError(t, db.Close())

	dbSnap, logs, _, snaps, err := openRaftV2Stores(filepath.Join(dataDir, "raft"))
	require.NoError(t, err)
	defer dbSnap.Close()
	snap := extra
	snap.LastIncludedIndex = 12
	snap.LastIncludedTerm = 3
	snap.Index = 12
	snap.Term = 3
	snap.Data = data
	snap.Servers = servers
	if snap.FormatVersion == 0 {
		snap.FormatVersion = raft.FSMSnapshotFormatVersion
	}
	require.NoError(t, snaps.Save(&snap))
	installer, ok := logs.(interface {
		InstallSnapshotBoundary(lastIncludedIndex, lastIncludedTerm uint64) error
	})
	require.True(t, ok)
	require.NoError(t, installer.InstallSnapshotBoundary(12, 3))
	return data
}
