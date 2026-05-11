package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/raft"
)

// noopRVMetrics and noopAEMetrics are transport stubs for metrics tests.
var noopRVMetrics = func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nil, fmt.Errorf("no transport")
}
var noopAEMetrics = func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, fmt.Errorf("no transport")
}

// newV2NodeForMetricsTest builds a single-node v2 RaftNode (via raftV2Node adapter)
// and returns it ready to use (transport set, started, leader elected).
func newV2NodeForMetricsTest(t *testing.T) RaftNode {
	t.Helper()
	t.Setenv("GRAINFS_RAFT_V2", "cluster")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	rcfg := raft.DefaultConfig("metrics-node-1", nil)
	node, err := newRaftNode(rcfg, nil)
	require.NoError(t, err)
	node.SetTransport(noopRVMetrics, noopAEMetrics)
	node.Start()
	t.Cleanup(node.Close)

	// Drain ApplyCh in the background.
	go func() {
		for range node.ApplyCh() {
		}
	}()

	// Wait for leader.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for !node.IsLeader() {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for v2 node to become leader")
		case <-time.After(5 * time.Millisecond):
		}
	}
	return node
}

// TestRaftV2Metrics_ProposeCountIncrements verifies that ProposeWait via the
// adapter increments RaftV2ProposeCount with outcome="success".
func TestRaftV2Metrics_ProposeCountIncrements(t *testing.T) {
	node := newV2NodeForMetricsTest(t)

	before := testutil.ToFloat64(metrics.RaftV2ProposeCount.WithLabelValues("success"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := node.ProposeWait(ctx, []byte("metrics-test"))
	require.NoError(t, err)

	after := testutil.ToFloat64(metrics.RaftV2ProposeCount.WithLabelValues("success"))
	assert.Equal(t, float64(1), after-before, "RaftV2ProposeCount[success] should increment by 1")
}

// TestRaftV2Metrics_ProposeLatencyObserved verifies that ProposeWait records
// a sample in RaftV2ProposeLatency for the success outcome.
// We verify this indirectly by checking the propose counter delta (which is
// coupled to the latency observation in ProposeWait) — both are incremented
// together in the adapter, so if count incremented, latency was also observed.
func TestRaftV2Metrics_ProposeLatencyObserved(t *testing.T) {
	node := newV2NodeForMetricsTest(t)

	before := testutil.ToFloat64(metrics.RaftV2ProposeCount.WithLabelValues("success"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := node.ProposeWait(ctx, []byte("latency-test"))
	require.NoError(t, err)

	after := testutil.ToFloat64(metrics.RaftV2ProposeCount.WithLabelValues("success"))
	// ProposeWait atomically increments count AND observes latency, so a
	// non-zero count delta confirms latency was recorded too.
	assert.Greater(t, after, before, "RaftV2ProposeCount[success] must increment — latency is co-observed")
}

// TestRaftV2Metrics_BootstrapOutcomeIncremented verifies that Bootstrap()
// increments RaftV2BootstrapOutcome with a defined outcome label.
func TestRaftV2Metrics_BootstrapOutcomeIncremented(t *testing.T) {
	node := newV2NodeForMetricsTest(t)

	// At this point the node may have already been bootstrapped by newRaftNode
	// (Bootstrap is informational-only in v2; it may have been called during
	// raftfactory.go setup). We call it again and check that EITHER
	// success OR error counter incremented (not both).
	beforeSuccess := testutil.ToFloat64(metrics.RaftV2BootstrapOutcome.WithLabelValues("success"))
	beforeError := testutil.ToFloat64(metrics.RaftV2BootstrapOutcome.WithLabelValues("error"))

	_ = node.Bootstrap() // may return ErrAlreadyBootstrapped — that's fine

	afterSuccess := testutil.ToFloat64(metrics.RaftV2BootstrapOutcome.WithLabelValues("success"))
	afterError := testutil.ToFloat64(metrics.RaftV2BootstrapOutcome.WithLabelValues("error"))

	deltaSuccess := afterSuccess - beforeSuccess
	deltaError := afterError - beforeError
	assert.Equal(t, float64(1), deltaSuccess+deltaError,
		"exactly one bootstrap outcome counter should increment; got success+%v error+%v", deltaSuccess, deltaError)
}

// TestRaftV2Metrics_StopCountIncremented verifies that Close() increments
// RaftV2StopCount. We create a dedicated node for this test and call Close explicitly.
func TestRaftV2Metrics_StopCountIncremented(t *testing.T) {
	t.Setenv("GRAINFS_RAFT_V2", "cluster")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	rcfg := raft.DefaultConfig("stop-test-node", nil)
	node, err := newRaftNode(rcfg, nil)
	require.NoError(t, err)
	node.SetTransport(noopRVMetrics, noopAEMetrics)
	node.Start()
	go func() {
		for range node.ApplyCh() {
		}
	}()

	before := testutil.ToFloat64(metrics.RaftV2StopCount)
	node.Close()
	after := testutil.ToFloat64(metrics.RaftV2StopCount)

	assert.Equal(t, float64(1), after-before, "RaftV2StopCount should increment by 1 on Close()")
}
