package metrics

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestOperatorStateCollector_EmitsBoundedStateMetrics(t *testing.T) {
	c := NewOperatorStateCollector(OperatorStateSources{
		Server: fakeOperatorServerSource{state: OperatorServerState{NodeID: "node-a", Version: "0.0.test"}},
		Cluster: fakeOperatorClusterSource{state: OperatorClusterState{
			MembersByState: map[string]int{
				"self_voter":             1,
				"healthy_voter_peer":     1,
				"unhealthy_learner_peer": 1,
			},
			QuorumAvailable: true,
		}},
		Raft: fakeOperatorRaftSource{states: []OperatorRaftState{{
			NodeID:          "node-a",
			Group:           "meta",
			Role:            "leader",
			Term:            7,
			CommitIndex:     15,
			AppliedIndex:    12,
			HasAppliedIndex: true,
		}}},
		Buckets: fakeOperatorBucketSource{state: OperatorBucketState{Active: 2}},
		Volumes: fakeOperatorVolumeSource{state: OperatorVolumeState{
			HealthCounts: map[string]int{
				"healthy":         1,
				"missing_replica": 1,
			},
			CapacityBytesTotal:  4096,
			AllocatedBytesTotal: 1024,
		}},
	})

	want := `# HELP grainfs_server_up Whether this GrainFS server process is serving metrics.
# TYPE grainfs_server_up gauge
grainfs_server_up{node_id="node-a"} 1
# HELP grainfs_server_info Static GrainFS server identity and build information.
# TYPE grainfs_server_info gauge
grainfs_server_info{node_id="node-a",version="0.0.test"} 1
# HELP grainfs_cluster_members Cluster member counts by bounded membership and liveness state.
# TYPE grainfs_cluster_members gauge
grainfs_cluster_members{state="healthy_learner_peer"} 0
grainfs_cluster_members{state="healthy_voter_peer"} 1
grainfs_cluster_members{state="self_learner"} 0
grainfs_cluster_members{state="self_voter"} 1
grainfs_cluster_members{state="unknown_peer"} 0
grainfs_cluster_members{state="unhealthy_learner_peer"} 1
grainfs_cluster_members{state="unhealthy_voter_peer"} 0
# HELP grainfs_cluster_quorum_available Whether healthy Raft voters can satisfy the current voter majority.
# TYPE grainfs_cluster_quorum_available gauge
grainfs_cluster_quorum_available 1
# HELP grainfs_raft_role Current Raft role for this node and bounded Raft group.
# TYPE grainfs_raft_role gauge
grainfs_raft_role{group="meta",node_id="node-a",role="candidate"} 0
grainfs_raft_role{group="meta",node_id="node-a",role="follower"} 0
grainfs_raft_role{group="meta",node_id="node-a",role="leader"} 1
grainfs_raft_role{group="meta",node_id="node-a",role="unknown"} 0
# HELP grainfs_raft_term Current Raft term for this node and bounded Raft group.
# TYPE grainfs_raft_term gauge
grainfs_raft_term{group="meta",node_id="node-a"} 7
# HELP grainfs_raft_commit_index Current committed Raft log index for this node and bounded Raft group.
# TYPE grainfs_raft_commit_index gauge
grainfs_raft_commit_index{group="meta",node_id="node-a"} 15
# HELP grainfs_raft_applied_index Current applied Raft log index for this node and bounded Raft group.
# TYPE grainfs_raft_applied_index gauge
grainfs_raft_applied_index{group="meta",node_id="node-a"} 12
# HELP grainfs_raft_apply_lag Difference between committed and applied Raft indexes for this node and bounded Raft group.
# TYPE grainfs_raft_apply_lag gauge
grainfs_raft_apply_lag{group="meta",node_id="node-a"} 3
# HELP grainfs_buckets_by_state User bucket counts by bounded state.
# TYPE grainfs_buckets_by_state gauge
grainfs_buckets_by_state{state="active"} 2
grainfs_buckets_by_state{state="list_error"} 0
# HELP grainfs_volumes_by_health Volume counts by bounded health state.
# TYPE grainfs_volumes_by_health gauge
grainfs_volumes_by_health{health="degraded"} 0
grainfs_volumes_by_health{health="healthy"} 1
grainfs_volumes_by_health{health="incident"} 0
grainfs_volumes_by_health{health="missing_replica"} 1
grainfs_volumes_by_health{health="unknown"} 0
# HELP grainfs_volume_capacity_bytes_total Aggregate logical capacity bytes across visible volumes.
# TYPE grainfs_volume_capacity_bytes_total gauge
grainfs_volume_capacity_bytes_total 4096
# HELP grainfs_volume_allocated_bytes_total Aggregate allocated bytes across visible volumes.
# TYPE grainfs_volume_allocated_bytes_total gauge
grainfs_volume_allocated_bytes_total 1024
# HELP grainfs_operator_state_scrape_errors_total Cumulative scrape-time failures while reading optional operator state sources.
# TYPE grainfs_operator_state_scrape_errors_total counter
grainfs_operator_state_scrape_errors_total{source="buckets"} 0
grainfs_operator_state_scrape_errors_total{source="raft"} 0
grainfs_operator_state_scrape_errors_total{source="status"} 0
grainfs_operator_state_scrape_errors_total{source="volumes"} 0
`
	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(want)))
}

func TestOperatorStateCollector_ScrapeErrorsAreMonotonic(t *testing.T) {
	c := NewOperatorStateCollector(OperatorStateSources{
		Buckets: fakeOperatorBucketSource{err: errors.New("boom")},
		Volumes: fakeOperatorVolumeSource{err: errors.New("boom")},
	})

	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(`# HELP grainfs_buckets_by_state User bucket counts by bounded state.
# TYPE grainfs_buckets_by_state gauge
grainfs_buckets_by_state{state="active"} 0
grainfs_buckets_by_state{state="list_error"} 1
# HELP grainfs_operator_state_scrape_errors_total Cumulative scrape-time failures while reading optional operator state sources.
# TYPE grainfs_operator_state_scrape_errors_total counter
grainfs_operator_state_scrape_errors_total{source="buckets"} 1
grainfs_operator_state_scrape_errors_total{source="raft"} 0
grainfs_operator_state_scrape_errors_total{source="status"} 0
grainfs_operator_state_scrape_errors_total{source="volumes"} 1
`), "first scrape"))

	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(`# HELP grainfs_buckets_by_state User bucket counts by bounded state.
# TYPE grainfs_buckets_by_state gauge
grainfs_buckets_by_state{state="active"} 0
grainfs_buckets_by_state{state="list_error"} 1
# HELP grainfs_operator_state_scrape_errors_total Cumulative scrape-time failures while reading optional operator state sources.
# TYPE grainfs_operator_state_scrape_errors_total counter
grainfs_operator_state_scrape_errors_total{source="buckets"} 2
grainfs_operator_state_scrape_errors_total{source="raft"} 0
grainfs_operator_state_scrape_errors_total{source="status"} 0
grainfs_operator_state_scrape_errors_total{source="volumes"} 2
`), "second scrape"))
}

func TestOperatorStateCollector_DeduplicatesRaftGroups(t *testing.T) {
	c := NewOperatorStateCollector(OperatorStateSources{
		Raft: fakeOperatorRaftSource{states: []OperatorRaftState{
			{NodeID: "node-a", Group: "data", Role: "leader", Term: 1, CommitIndex: 3},
			{NodeID: "node-a", Group: "data", Role: "follower", Term: 2, CommitIndex: 4},
			{NodeID: "node-a", Group: "group-17", Role: "leader", Term: 3, CommitIndex: 5},
		}},
	})

	want := `# HELP grainfs_raft_role Current Raft role for this node and bounded Raft group.
# TYPE grainfs_raft_role gauge
grainfs_raft_role{group="data",node_id="node-a",role="candidate"} 0
grainfs_raft_role{group="data",node_id="node-a",role="follower"} 0
grainfs_raft_role{group="data",node_id="node-a",role="leader"} 1
grainfs_raft_role{group="data",node_id="node-a",role="unknown"} 0
# HELP grainfs_raft_term Current Raft term for this node and bounded Raft group.
# TYPE grainfs_raft_term gauge
grainfs_raft_term{group="data",node_id="node-a"} 1
# HELP grainfs_raft_commit_index Current committed Raft log index for this node and bounded Raft group.
# TYPE grainfs_raft_commit_index gauge
grainfs_raft_commit_index{group="data",node_id="node-a"} 3
# HELP grainfs_operator_state_scrape_errors_total Cumulative scrape-time failures while reading optional operator state sources.
# TYPE grainfs_operator_state_scrape_errors_total counter
grainfs_operator_state_scrape_errors_total{source="buckets"} 0
grainfs_operator_state_scrape_errors_total{source="raft"} 2
grainfs_operator_state_scrape_errors_total{source="status"} 0
grainfs_operator_state_scrape_errors_total{source="volumes"} 0
`
	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(want)))
}

type fakeOperatorServerSource struct {
	state OperatorServerState
}

func (f fakeOperatorServerSource) ServerStateSnapshot() OperatorServerState { return f.state }

type fakeOperatorClusterSource struct {
	state OperatorClusterState
	err   error
}

func (f fakeOperatorClusterSource) ClusterStateSnapshot() (OperatorClusterState, error) {
	return f.state, f.err
}

type fakeOperatorRaftSource struct {
	states []OperatorRaftState
	err    error
}

func (f fakeOperatorRaftSource) RaftStateSnapshot() ([]OperatorRaftState, error) {
	return f.states, f.err
}

type fakeOperatorBucketSource struct {
	state OperatorBucketState
	err   error
}

func (f fakeOperatorBucketSource) BucketStateSnapshot(context.Context) (OperatorBucketState, error) {
	return f.state, f.err
}

type fakeOperatorVolumeSource struct {
	state OperatorVolumeState
	err   error
}

func (f fakeOperatorVolumeSource) VolumeStateSnapshot(context.Context) (OperatorVolumeState, error) {
	return f.state, f.err
}
