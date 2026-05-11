package raftv2

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// actorLeaderTransitions counts every becomeLeader and stepDown event.
	// Elevated rate during soak indicates leader instability.
	actorLeaderTransitions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_raft_v2_actor_leader_transitions_total",
		Help: "Total actor leader transitions (becomeLeader + stepDownToFollower) in v2.",
	})

	// actorTermBumps counts every advance of currentTerm inside the actor.
	actorTermBumps = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_raft_v2_actor_term_bumps_total",
		Help: "Total currentTerm advances in the v2 actor goroutine.",
	})

	// actorCmdChDepthGauge samples the instantaneous depth of cmdCh.
	// Updated on heartbeat tick (multi-voter) to avoid hot-path cost.
	// Single-voter clusters do not update this gauge (no ticker).
	actorCmdChDepthGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_raft_v2_actor_cmd_ch_depth",
		Help: "Sampled depth of the v2 actor cmdCh. Updated on heartbeat tick; 0 on single-voter nodes.",
	})

	// actorAppendEntriesRPCDuration measures handleAppendEntries duration by outcome.
	// Labels: outcome ∈ {success, conflict, error}.
	actorAppendEntriesRPCDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grainfs_raft_v2_actor_append_entries_duration_seconds",
		Help:    "handleAppendEntries duration in seconds, by outcome.",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.010, 0.050, 0.100, 0.500},
	}, []string{"outcome"})
)
