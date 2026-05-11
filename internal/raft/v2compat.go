package raft

// v2compat.go — compile-only shims so that *raft.Node continues to satisfy
// cluster.RaftNode after M5 PR 29 folded the snapshot surface
// (CreateSnapshot / SnapshotStatus) into the cluster.RaftNode interface.
//
// The v1 raft package is unreachable from production code paths as of PR 29
// (the GRAINFS_RAFT_V2 flag is gone; serveruntime + cluster always construct
// the v2 adapter). These methods exist solely so v1-specific tests that
// assign *raft.Node into cluster.RaftNode keep compiling. PR 30 deletes the
// v1 raft package outright and removes this file.
//
// Calling either method panics — if a production code path ever reaches one
// of these, it means a v1 *raft.Node escaped into a code path that expected
// v2 semantics. Crash loud so the bug is obvious.

// CreateSnapshot is a v1 stub that panics. v1 snapshot lifecycle is owned by
// raft.SnapshotManager (DistributedBackend.SetSnapshotManager), not the
// cluster.RaftNode interface. PR 30 deletes v1 outright.
func (n *Node) CreateSnapshot(lastIncludedIndex uint64, data []byte) error {
	panic("internal/raft.Node.CreateSnapshot: v1 raft path is unreachable as of M5 PR 29; PR 30 deletes the v1 package")
}

// SnapshotStatus is a v1 stub that panics. Same rationale as CreateSnapshot.
func (n *Node) SnapshotStatus() (SnapshotStatus, error) {
	panic("internal/raft.Node.SnapshotStatus: v1 raft path is unreachable as of M5 PR 29; PR 30 deletes the v1 package")
}
