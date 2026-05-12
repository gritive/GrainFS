package cluster

import (
	"fmt"
	"os"
	"path/filepath"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/raft"
)

var (
	raftV2LogPrefix    = []byte("raft/v2/log/")
	raftV2StablePrefix = []byte("raft/v2/hardstate/")
	raftV2SnapPrefix   = []byte("raft/v2/snap/")
)

const raftV2StoreSubdir = "raft-v2"

func newRaftNode(rcfg raft.Config, v2StoreDir string) (RaftNode, func() error, error) {
	return newRaftNodeV2(rcfg, v2StoreDir)
}

func newRaftNodeV2(rcfg raft.Config, v2StoreDir string) (*raftNodeAdapter, func() error, error) {
	v2cfg := raft.Config{
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
		JoinMode:                      rcfg.JoinMode,
	}

	var closeFn func() error
	if v2StoreDir != "" {
		db, ls, ss, sn, err := openRaftV2Stores(v2StoreDir)
		if err != nil {
			return nil, nil, fmt.Errorf("raft open stores: %w", err)
		}
		v2cfg.LogStore = ls
		v2cfg.StableStore = ss
		v2cfg.SnapshotStore = sn
		closeFn = db.Close
	}

	n, err := raft.NewNode(v2cfg)
	if err != nil {
		if closeFn != nil {
			_ = closeFn()
		}
		return nil, nil, fmt.Errorf("raft.NewNode: %w", err)
	}
	return newRaftNodeAdapter(n), closeFn, nil
}

func NewRaftV2NodeForServeruntime(rcfg raft.Config, raftDir string) (RaftNode, func() error, error) {
	return newRaftNodeV2(rcfg, raftDir)
}

func openRaftV2Stores(dir string) (*badger.DB, raft.LogStore, raft.StableStore, raft.SnapshotStore, error) {
	storeDir := filepath.Join(dir, raftV2StoreSubdir)
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("mkdir %s: %w", storeDir, err)
	}
	db, err := badger.Open(badgerutil.SmallOptions(storeDir))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open badger %s: %w", storeDir, err)
	}
	ls, err := raft.NewBadgerLogStore(db, raftV2LogPrefix)
	if err != nil {
		_ = db.Close()
		return nil, nil, nil, nil, fmt.Errorf("NewBadgerLogStore: %w", err)
	}
	ss, err := raft.NewBadgerStableStore(db, raftV2StablePrefix)
	if err != nil {
		_ = db.Close()
		return nil, nil, nil, nil, fmt.Errorf("NewBadgerStableStore: %w", err)
	}
	sn, err := raft.NewBadgerSnapshotStore(db, raftV2SnapPrefix)
	if err != nil {
		_ = db.Close()
		return nil, nil, nil, nil, fmt.Errorf("NewBadgerSnapshotStore: %w", err)
	}
	return db, ls, ss, sn, nil
}
