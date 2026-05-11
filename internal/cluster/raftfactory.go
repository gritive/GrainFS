package cluster

// raftfactory.go — factory function that dispatches raft node creation to v1
// or v2 based on the GRAINFS_RAFT_V2 environment flag.
//
// Only group_lifecycle.go (instantiateLocalGroup) routes through this factory.
// MetaRaft and the migrate path remain v1-only in PR 22 per plan §M4 scope.

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/raft"
	raftv2 "github.com/gritive/GrainFS/internal/raft/v2"
)

// newRaftNode creates a RaftNode using either v1 or v2 based on the
// GRAINFS_RAFT_V2=cluster flag.
//
// v1→v2 Config translation: v2.Config mirrors v1.Config field-by-field.
// v1 fields that have no v2 equivalent are ignored when v2 is selected:
//   - ManagedMode: v2 has the field but no auto-promote watcher in PR 22
//   - LogGCInterval: v2 has no periodic log GC watcher in PR 22
//   - MaxAppendEntriesInflightBytes: accepted by v2 but ignored
//   - LearnerCatchupThreshold: v2 field present but unused until AddLearner lands
//   - JointAbortTimeout: v2 has joint consensus but no abort timeout in PR 22
//
// logStore is used for v1. For v2 the LogStore interfaces are incompatible
// (v1: AppendEntries/GetEntry vs v2: Append/EntriesFrom/CompactBefore), so v2
// uses its built-in in-memory store in PR 22. Durable v2 storage is deferred
// to PR 23 once a v2-native BadgerLogStore wrapper is wired into this factory.
func newRaftNode(rcfg raft.Config, logStore raft.LogStore) (RaftNode, error) {
	if IsV2Enabled("cluster") {
		return newRaftNodeV2(rcfg)
	}
	return raft.NewNode(rcfg, logStore), nil
}

// newRaftNodeV2 instantiates a v2 node wrapped in the adapter using an
// in-memory log store (see newRaftNode doc for why durable store is deferred).
func newRaftNodeV2(rcfg raft.Config) (*raftV2Node, error) {
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
		// LogStore: nil → v2 uses its built-in in-memory store
	}

	n, err := raftv2.NewNode(v2cfg)
	if err != nil {
		return nil, fmt.Errorf("raftv2.NewNode: %w", err)
	}
	return newRaftV2Node(n), nil
}
