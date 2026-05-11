package cluster

// raftfactory.go — factory function that constructs raft v2 nodes for the
// per-group cluster path. As of M5 PR 29 the GRAINFS_RAFT_V2 flag is gone;
// v2 is the only path. PR 30 will delete internal/raft/ v1 entirely.
//
// Only group_lifecycle.go (instantiateLocalGroup) routes through this factory.

import (
	"fmt"
	"os"
	"path/filepath"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/raft"
	raftv2 "github.com/gritive/GrainFS/internal/raft/v2"
)

// v2 Badger key prefixes (per-group, distinct so a single DB can host all
// three stores). Match the prefixes used by internal/raft/v2 tests.
var (
	raftV2LogPrefix    = []byte("raft/v2/log/")
	raftV2StablePrefix = []byte("raft/v2/hardstate/")
	raftV2SnapPrefix   = []byte("raft/v2/snap/")
)

// raftV2StoreSubdir is the per-group sub-directory holding the v2 Badger DB.
// Sibling to the v1 "raft" sub-directory so the on-disk schemas do not
// collide. A v2 deployment cannot read v1 state; acceptable because the
// v2 BadgerLogStore was never durably wired before PR 26 (in-memory only).
const raftV2StoreSubdir = "raft-v2"

// newRaftNode constructs a v2 RaftNode wrapped in the cluster adapter.
//
// v1→v2 Config translation: v2.Config mirrors v1.Config field-by-field.
// v1 fields that have no v2 behavior in PR 22+ are accepted but ignored:
//   - ManagedMode: v2 has the field but no auto-promote watcher in PR 22
//   - LogGCInterval: v2 has no periodic log GC watcher in PR 22
//   - MaxAppendEntriesInflightBytes: accepted by v2 but ignored
//   - LearnerCatchupThreshold: v2 field present but unused until AddLearner lands
//   - JointAbortTimeout: v2 has joint consensus but no abort timeout in PR 22
//
// When v2StoreDir is non-empty, v2 opens a Badger DB at <v2StoreDir>/raft-v2/
// and wires durable LogStore + StableStore + SnapshotStore. When v2StoreDir
// is empty, v2 falls back to its built-in in-memory store (used by smoke
// tests with no on-disk lifecycle).
//
// The returned closeFn must be invoked when the node shuts down to release
// the v2 Badger DB handle.
//
// logStore is accepted for source-compat with PR 28b callers and ignored;
// PR 30 removes the parameter when the v1 LogStore type is deleted.
func newRaftNode(rcfg raft.Config, _ raft.LogStore, v2StoreDir string) (RaftNode, func() error, error) {
	return newRaftNodeV2(rcfg, v2StoreDir)
}

// newRaftNodeV2 instantiates a v2 node wrapped in the adapter. When
// v2StoreDir is non-empty, durable Badger-backed LogStore + StableStore +
// SnapshotStore are wired into v2.Config. When empty, all three default to
// the in-memory implementations (matches PR 22 behaviour for unit tests that
// never call SetTransport against a real network).
func newRaftNodeV2(rcfg raft.Config, v2StoreDir string) (*raftV2Node, func() error, error) {
	v2cfg := raftv2.Config{
		ID:                            rcfg.ID,
		Peers:                         rcfg.Peers,
		ElectionTimeout:               rcfg.ElectionTimeout,
		HeartbeatTimeout:              rcfg.HeartbeatTimeout,
		ManagedMode:                   rcfg.ManagedMode,
		LogGCInterval:                 rcfg.LogGCInterval,
		MaxEntriesPerAE:               rcfg.MaxEntriesPerAE,
		MaxAppendEntriesInflight:      rcfg.MaxAppendEntriesInflight,
		MaxAppendEntriesInflightBytes: rcfg.MaxAppendEntriesInflightBytes,
		TrailingLogs:                  rcfg.TrailingLogs,
		LearnerCatchupThreshold:       rcfg.LearnerCatchupThreshold,
		JointAbortTimeout:             rcfg.JointAbortTimeout,
		ElectionPriorityKey:           rcfg.ElectionPriorityKey,
	}

	var closeFn func() error
	if v2StoreDir != "" {
		db, ls, ss, sn, err := openRaftV2Stores(v2StoreDir)
		if err != nil {
			return nil, nil, fmt.Errorf("raftv2 open stores: %w", err)
		}
		v2cfg.LogStore = ls
		v2cfg.StableStore = ss
		v2cfg.SnapshotStore = sn
		closeFn = db.Close
	}

	n, err := raftv2.NewNode(v2cfg)
	if err != nil {
		if closeFn != nil {
			_ = closeFn()
		}
		return nil, nil, fmt.Errorf("raftv2.NewNode: %w", err)
	}
	return newRaftV2Node(n), closeFn, nil
}

// NewRaftV2NodeForServeruntime is the serveruntime entry point that
// constructs a v2 Raft node. serveruntime never routes through newRaftNode;
// its raft node is constructed once per process at boot.
//
// raftDir is the meta-raft root directory; v2 stores land at
// <raftDir>/raft-v2/. The returned closeFn must be invoked at shutdown to
// release the Badger DB handle (caller registers via bootState.AddCleanup).
func NewRaftV2NodeForServeruntime(rcfg raft.Config, raftDir string) (RaftNode, func() error, error) {
	return newRaftNodeV2(rcfg, raftDir)
}

// openRaftV2Stores opens a Badger DB at <dir>/raft-v2/ and returns the three
// v2 durable stores keyed under distinct prefixes. Caller is responsible for
// closing db (returned for that purpose).
func openRaftV2Stores(dir string) (*badger.DB, raftv2.LogStore, raftv2.StableStore, raftv2.SnapshotStore, error) {
	storeDir := filepath.Join(dir, raftV2StoreSubdir)
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("mkdir %s: %w", storeDir, err)
	}
	db, err := badger.Open(badgerutil.SmallOptions(storeDir))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open badger %s: %w", storeDir, err)
	}
	ls, err := raftv2.NewBadgerLogStore(db, raftV2LogPrefix)
	if err != nil {
		_ = db.Close()
		return nil, nil, nil, nil, fmt.Errorf("NewBadgerLogStore: %w", err)
	}
	ss, err := raftv2.NewBadgerStableStore(db, raftV2StablePrefix)
	if err != nil {
		_ = db.Close()
		return nil, nil, nil, nil, fmt.Errorf("NewBadgerStableStore: %w", err)
	}
	sn, err := raftv2.NewBadgerSnapshotStore(db, raftV2SnapPrefix)
	if err != nil {
		_ = db.Close()
		return nil, nil, nil, nil, fmt.Errorf("NewBadgerSnapshotStore: %w", err)
	}
	return db, ls, ss, sn, nil
}
