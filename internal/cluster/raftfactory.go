package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/raft"
)

type RaftV2StoreOptions struct {
	EncryptionKey []byte
}

func newRaftNode(rcfg raft.Config, v2StoreDir string) (RaftNode, func() error, error) {
	return newRaftNodeV2(rcfg, v2StoreDir)
}

func newRaftNodeV2(rcfg raft.Config, v2StoreDir string) (*raftNodeAdapter, func() error, error) {
	return newRaftNodeV2WithStoreOptions(rcfg, v2StoreDir, RaftV2StoreOptions{})
}

func newRaftNodeV2WithStoreOptions(rcfg raft.Config, v2StoreDir string, opts RaftV2StoreOptions) (*raftNodeAdapter, func() error, error) {
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
		ls, ss, sn, dbClose, err := raft.OpenV2Stores(v2StoreDir, opts.EncryptionKey)
		if err != nil {
			return nil, nil, fmt.Errorf("raft open stores: %w", err)
		}
		v2cfg.LogStore = ls
		v2cfg.StableStore = ss
		v2cfg.SnapshotStore = sn
		closeFn = dbClose
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

func NewRaftV2NodeForServeruntimeWithStoreOptions(rcfg raft.Config, raftDir string, opts RaftV2StoreOptions) (RaftNode, func() error, error) {
	return newRaftNodeV2WithStoreOptions(rcfg, raftDir, opts)
}
