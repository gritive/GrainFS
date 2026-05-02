package raft

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// raftConflictTermJumpsTotal counts the number of times nextIndex was
	// advanced by a ConflictTerm hint instead of decremented one-by-one.
	raftConflictTermJumpsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "raft_conflict_term_jumps_total",
		Help: "Total AppendEntries nextIndex jumps via ConflictTerm hint (fast backtracking).",
	})

	// raftAESplitCountTotal counts the number of AppendEntries messages that
	// were capped at MaxEntriesPerAE due to payload size limit.
	raftAESplitCountTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "raft_ae_split_count_total",
		Help: "Total AppendEntries messages capped at MaxEntriesPerAE entries.",
	})

	raftAEStaleReplyTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "raft_ae_stale_reply_total",
		Help: "Total stale AppendEntries replies ignored by the leader pipeline.",
	})

	raftAEConflictResetsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "raft_ae_conflict_resets_total",
		Help: "Total peer pipeline resets caused by AppendEntries conflict replies.",
	})

	raftAEWindowFullTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "raft_ae_pipeline_window_full_total",
		Help: "Total times a peer AppendEntries pipeline could not send because its window was full.",
	})

	raftAESnapshotExclusiveTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "raft_ae_snapshot_exclusive_total",
		Help: "Total snapshot installs sent through the exclusive peer replication path.",
	})
)
