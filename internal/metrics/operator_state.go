package metrics

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const operatorStateScrapeTimeout = 2 * time.Second

type OperatorStateSources struct {
	Server  OperatorServerStateSource
	Cluster OperatorClusterStateSource
	Raft    OperatorRaftStateSource
	Buckets OperatorBucketStateSource
	Volumes OperatorVolumeStateSource
}

type OperatorServerStateSource interface {
	ServerStateSnapshot() OperatorServerState
}

type OperatorClusterStateSource interface {
	ClusterStateSnapshot() (OperatorClusterState, error)
}

type OperatorRaftStateSource interface {
	RaftStateSnapshot() ([]OperatorRaftState, error)
}

type OperatorBucketStateSource interface {
	BucketStateSnapshot(context.Context) (OperatorBucketState, error)
}

type OperatorVolumeStateSource interface {
	VolumeStateSnapshot(context.Context) (OperatorVolumeState, error)
}

type OperatorServerState struct {
	NodeID  string
	Version string
}

type OperatorClusterState struct {
	MembersByState  map[string]int
	QuorumAvailable bool
}

type OperatorRaftState struct {
	NodeID          string
	Group           string
	Role            string
	Term            uint64
	CommitIndex     uint64
	AppliedIndex    uint64
	HasAppliedIndex bool
}

type OperatorBucketState struct {
	Active int
}

type OperatorVolumeState struct {
	HealthCounts        map[string]int
	CapacityBytesTotal  int64
	AllocatedBytesTotal int64
}

var (
	operatorServerUpDesc = prometheus.NewDesc(
		"grainfs_server_up",
		"Whether this GrainFS server process is serving metrics.",
		[]string{"node_id"}, nil,
	)
	operatorServerInfoDesc = prometheus.NewDesc(
		"grainfs_server_info",
		"Static GrainFS server identity and build information.",
		[]string{"node_id", "version"}, nil,
	)
	operatorClusterMembersDesc = prometheus.NewDesc(
		"grainfs_cluster_members",
		"Cluster member counts by bounded membership and liveness state.",
		[]string{"state"}, nil,
	)
	operatorClusterQuorumAvailableDesc = prometheus.NewDesc(
		"grainfs_cluster_quorum_available",
		"Whether healthy Raft voters can satisfy the current voter majority.",
		nil, nil,
	)
	operatorRaftRoleDesc = prometheus.NewDesc(
		"grainfs_raft_role",
		"Current Raft role for this node and bounded Raft group.",
		[]string{"node_id", "group", "role"}, nil,
	)
	operatorRaftTermDesc = prometheus.NewDesc(
		"grainfs_raft_term",
		"Current Raft term for this node and bounded Raft group.",
		[]string{"node_id", "group"}, nil,
	)
	operatorRaftCommitIndexDesc = prometheus.NewDesc(
		"grainfs_raft_commit_index",
		"Current committed Raft log index for this node and bounded Raft group.",
		[]string{"node_id", "group"}, nil,
	)
	operatorRaftAppliedIndexDesc = prometheus.NewDesc(
		"grainfs_raft_applied_index",
		"Current applied Raft log index for this node and bounded Raft group.",
		[]string{"node_id", "group"}, nil,
	)
	operatorRaftApplyLagDesc = prometheus.NewDesc(
		"grainfs_raft_apply_lag",
		"Difference between committed and applied Raft indexes for this node and bounded Raft group.",
		[]string{"node_id", "group"}, nil,
	)
	operatorBucketsByStateDesc = prometheus.NewDesc(
		"grainfs_buckets_by_state",
		"User bucket counts by bounded state.",
		[]string{"state"}, nil,
	)
	operatorVolumesByHealthDesc = prometheus.NewDesc(
		"grainfs_volumes_by_health",
		"Volume counts by bounded health state.",
		[]string{"health"}, nil,
	)
	operatorVolumeCapacityBytesTotalDesc = prometheus.NewDesc(
		"grainfs_volume_capacity_bytes_total",
		"Aggregate logical capacity bytes across visible volumes.",
		nil, nil,
	)
	operatorVolumeAllocatedBytesTotalDesc = prometheus.NewDesc(
		"grainfs_volume_allocated_bytes_total",
		"Aggregate allocated bytes across visible volumes.",
		nil, nil,
	)
	operatorStateScrapeErrorsTotalDesc = prometheus.NewDesc(
		"grainfs_operator_state_scrape_errors_total",
		"Cumulative scrape-time failures while reading optional operator state sources.",
		[]string{"source"}, nil,
	)
)

var (
	clusterMemberStates = []string{
		"self_voter",
		"self_learner",
		"healthy_voter_peer",
		"unhealthy_voter_peer",
		"healthy_learner_peer",
		"unhealthy_learner_peer",
		"unknown_peer",
	}
	raftGroups           = map[string]struct{}{"meta": {}, "data": {}}
	raftRoles            = []string{"leader", "follower", "candidate", "unknown"}
	raftRolesSet         = map[string]struct{}{"leader": {}, "follower": {}, "candidate": {}, "unknown": {}}
	bucketStates         = []string{"active", "list_error"} //nolint:unused // operator-state scaffolding (v0.0.388-389); kept until the feature wires it.
	volumeHealthStates   = []string{"healthy", "degraded", "unknown", "missing_replica", "incident"}
	operatorErrorSources = []string{"status", "buckets", "volumes", "raft"}
)

type OperatorStateCollector struct {
	mu             sync.RWMutex
	sources        OperatorStateSources
	errorMu        sync.Mutex
	scrapeFailures map[string]uint64
}

func NewOperatorStateCollector(sources OperatorStateSources) *OperatorStateCollector {
	return newOperatorStateCollector(sources)
}

func newOperatorStateCollector(sources OperatorStateSources) *OperatorStateCollector {
	return &OperatorStateCollector{
		sources:        sources,
		scrapeFailures: make(map[string]uint64, len(operatorErrorSources)),
	}
}

var (
	defaultOperatorStateCollectorMu sync.Mutex
	defaultOperatorStateCollector   = newOperatorStateCollector(OperatorStateSources{})
)

func RegisterOperatorStateCollector(sources OperatorStateSources) {
	defaultOperatorStateCollectorMu.Lock()
	defer defaultOperatorStateCollectorMu.Unlock()

	defaultOperatorStateCollector.SetSources(sources)
	if err := prometheus.Register(defaultOperatorStateCollector); err != nil {
		var already prometheus.AlreadyRegisteredError
		if !errors.As(err, &already) {
			panic(fmt.Errorf("RegisterOperatorStateCollector: %w", err))
		}
	}
}

func (c *OperatorStateCollector) SetSources(sources OperatorStateSources) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sources = sources
}

func (c *OperatorStateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- operatorServerUpDesc
	ch <- operatorServerInfoDesc
	ch <- operatorClusterMembersDesc
	ch <- operatorClusterQuorumAvailableDesc
	ch <- operatorRaftRoleDesc
	ch <- operatorRaftTermDesc
	ch <- operatorRaftCommitIndexDesc
	ch <- operatorRaftAppliedIndexDesc
	ch <- operatorRaftApplyLagDesc
	ch <- operatorBucketsByStateDesc
	ch <- operatorVolumesByHealthDesc
	ch <- operatorVolumeCapacityBytesTotalDesc
	ch <- operatorVolumeAllocatedBytesTotalDesc
	ch <- operatorStateScrapeErrorsTotalDesc
}

func (c *OperatorStateCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	sources := c.sources
	c.mu.RUnlock()

	if sources.Server != nil {
		state := sources.Server.ServerStateSnapshot()
		nodeID := boundedMetricLabel(state.NodeID, "unknown")
		version := boundedMetricLabel(state.Version, "unknown")
		ch <- prometheus.MustNewConstMetric(operatorServerUpDesc, prometheus.GaugeValue, 1, nodeID)
		ch <- prometheus.MustNewConstMetric(operatorServerInfoDesc, prometheus.GaugeValue, 1, nodeID, version)
	}

	if sources.Cluster != nil {
		state, err := sources.Cluster.ClusterStateSnapshot()
		if err != nil {
			c.incScrapeFailure("status")
		} else {
			for _, label := range clusterMemberStates {
				ch <- prometheus.MustNewConstMetric(operatorClusterMembersDesc, prometheus.GaugeValue, float64(state.MembersByState[label]), label)
			}
			ch <- prometheus.MustNewConstMetric(operatorClusterQuorumAvailableDesc, prometheus.GaugeValue, boolGauge(state.QuorumAvailable))
		}
	}

	if sources.Raft != nil {
		states, err := sources.Raft.RaftStateSnapshot()
		if err != nil {
			c.incScrapeFailure("raft")
		} else {
			c.collectRaft(ch, states)
		}
	}

	if sources.Buckets != nil {
		ctx, cancel := context.WithTimeout(context.Background(), operatorStateScrapeTimeout)
		state, err := sources.Buckets.BucketStateSnapshot(ctx)
		cancel()
		if err != nil {
			c.incScrapeFailure("buckets")
			ch <- prometheus.MustNewConstMetric(operatorBucketsByStateDesc, prometheus.GaugeValue, 0, "active")
			ch <- prometheus.MustNewConstMetric(operatorBucketsByStateDesc, prometheus.GaugeValue, 1, "list_error")
		} else {
			ch <- prometheus.MustNewConstMetric(operatorBucketsByStateDesc, prometheus.GaugeValue, float64(state.Active), "active")
			ch <- prometheus.MustNewConstMetric(operatorBucketsByStateDesc, prometheus.GaugeValue, 0, "list_error")
		}
	}

	if sources.Volumes != nil {
		ctx, cancel := context.WithTimeout(context.Background(), operatorStateScrapeTimeout)
		state, err := sources.Volumes.VolumeStateSnapshot(ctx)
		cancel()
		if err != nil {
			c.incScrapeFailure("volumes")
		} else {
			for _, label := range volumeHealthStates {
				ch <- prometheus.MustNewConstMetric(operatorVolumesByHealthDesc, prometheus.GaugeValue, float64(state.HealthCounts[label]), label)
			}
			ch <- prometheus.MustNewConstMetric(operatorVolumeCapacityBytesTotalDesc, prometheus.GaugeValue, nonNegativeFloat(state.CapacityBytesTotal))
			ch <- prometheus.MustNewConstMetric(operatorVolumeAllocatedBytesTotalDesc, prometheus.GaugeValue, nonNegativeFloat(state.AllocatedBytesTotal))
		}
	}

	c.collectScrapeFailures(ch)
}

func (c *OperatorStateCollector) collectRaft(ch chan<- prometheus.Metric, states []OperatorRaftState) {
	seen := make(map[string]struct{}, len(states))
	for _, state := range states {
		nodeID := boundedMetricLabel(state.NodeID, "unknown")
		group := normalizeRaftGroup(state.Group)
		if group == "" {
			c.incScrapeFailure("raft")
			continue
		}
		key := nodeID + "\x00" + group
		if _, ok := seen[key]; ok {
			c.incScrapeFailure("raft")
			continue
		}
		seen[key] = struct{}{}

		role := normalizeRaftRole(state.Role)
		for _, candidate := range raftRoles {
			ch <- prometheus.MustNewConstMetric(operatorRaftRoleDesc, prometheus.GaugeValue, boolGauge(candidate == role), nodeID, group, candidate)
		}
		ch <- prometheus.MustNewConstMetric(operatorRaftTermDesc, prometheus.GaugeValue, float64(state.Term), nodeID, group)
		ch <- prometheus.MustNewConstMetric(operatorRaftCommitIndexDesc, prometheus.GaugeValue, float64(state.CommitIndex), nodeID, group)
		if state.HasAppliedIndex {
			applied := state.AppliedIndex
			ch <- prometheus.MustNewConstMetric(operatorRaftAppliedIndexDesc, prometheus.GaugeValue, float64(applied), nodeID, group)
			ch <- prometheus.MustNewConstMetric(operatorRaftApplyLagDesc, prometheus.GaugeValue, float64(saturatingSub(state.CommitIndex, applied)), nodeID, group)
		}
	}
}

func (c *OperatorStateCollector) incScrapeFailure(source string) {
	c.errorMu.Lock()
	defer c.errorMu.Unlock()
	c.scrapeFailures[normalizeErrorSource(source)]++
}

func (c *OperatorStateCollector) collectScrapeFailures(ch chan<- prometheus.Metric) {
	c.errorMu.Lock()
	defer c.errorMu.Unlock()
	for _, source := range operatorErrorSources {
		ch <- prometheus.MustNewConstMetric(operatorStateScrapeErrorsTotalDesc, prometheus.CounterValue, float64(c.scrapeFailures[source]), source)
	}
}

func normalizeRaftGroup(group string) string {
	if _, ok := raftGroups[group]; ok {
		return group
	}
	return ""
}

func normalizeRaftRole(role string) string {
	if _, ok := raftRolesSet[role]; ok {
		return role
	}
	return "unknown"
}

func normalizeErrorSource(source string) string {
	for _, candidate := range operatorErrorSources {
		if source == candidate {
			return source
		}
	}
	return "status"
}

func boundedMetricLabel(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func boolGauge(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func saturatingSub(a, b uint64) uint64 {
	if b > a {
		return 0
	}
	return a - b
}

func nonNegativeFloat(v int64) float64 {
	if v < 0 {
		return 0
	}
	return float64(v)
}
