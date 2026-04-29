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
)
