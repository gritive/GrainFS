package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// GroupRebalancer executes a voter move within a data Raft group.
// PR-D: only MockGroupRebalancer/StubGroupRebalancer implemented.
// PR-E: DataGroupPlanExecutor wires real §4.4 ops.
type GroupRebalancer interface {
	// MoveReplica adds toNode as a learner, waits for catch-up, promotes to voter,
	// then removes fromNode. Returns when the move is complete or ctx is cancelled.
	MoveReplica(ctx context.Context, groupID, fromNode, toNode string) error
}

// RebalancerConfig holds tunable parameters.
type RebalancerConfig struct {
	EvalInterval    time.Duration // how often leader evaluates load; default 30s (P1)
	ImbalanceThresh float64       // trigger when max-min DiskUsedPct > this; default 30
	PlanTimeout     time.Duration // execution timeout before AbortPlan; default 10min
}

// DefaultRebalancerConfig returns production defaults.
func DefaultRebalancerConfig() RebalancerConfig {
	return RebalancerConfig{
		EvalInterval:    30 * time.Second, // P1: 30s to reduce log churn (~44 MB/day)
		ImbalanceThresh: 30.0,
		PlanTimeout:     10 * time.Minute,
	}
}

// MetaRaftClient is the subset of MetaRaft used by Rebalancer.
type MetaRaftClient interface {
	IsLeader() bool
	ProposeRebalancePlan(ctx context.Context, plan RebalancePlan) error
	ProposeAbortPlan(ctx context.Context, planID string, reason clusterpb.AbortPlanReason) error
	FSM() *MetaFSM
}

// Rebalancer evaluates cluster load and proposes RebalancePlans.
// Runs only on the meta-Raft leader; follower ticks are no-ops.
type Rebalancer struct {
	nodeID    string
	meta      MetaRaftClient
	groups    *DataGroupManager
	rebalance GroupRebalancer
	cfg       RebalancerConfig
	logger    zerolog.Logger
	stopCh    chan struct{}
	stopOnce  sync.Once
}

// NewRebalancer creates a Rebalancer.
func NewRebalancer(nodeID string, meta MetaRaftClient, groups *DataGroupManager, cfg RebalancerConfig) *Rebalancer {
	return &Rebalancer{
		nodeID: nodeID,
		meta:   meta,
		groups: groups,
		cfg:    cfg,
		logger: log.With().Str("component", "rebalancer").Logger(),
		stopCh: make(chan struct{}),
	}
}

// SetGroupRebalancer wires the executor. Must be called before Run.
func (r *Rebalancer) SetGroupRebalancer(gr GroupRebalancer) {
	r.rebalance = gr
}

// Run starts the evaluation loop. Blocks until ctx cancelled or Stop() called.
func (r *Rebalancer) Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.EvalInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.tickOnce(ctx)
		}
	}
}

// Stop signals the loop to exit. Idempotent.
func (r *Rebalancer) Stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
}

// tickOnce is a single evaluation cycle.
func (r *Rebalancer) tickOnce(ctx context.Context) {
	if !r.meta.IsLeader() {
		return
	}
	fsm := r.meta.FSM()

	// Check for timed-out active plan.
	if plan := fsm.ActivePlan(); plan != nil {
		if time.Since(plan.CreatedAt) > r.cfg.PlanTimeout {
			r.logger.Warn().Str("plan_id", plan.PlanID).Msg("plan execution timeout, aborting")
			if err := r.meta.ProposeAbortPlan(ctx, plan.PlanID, clusterpb.AbortPlanReasonTimeout); err != nil {
				r.logger.Error().Err(err).Msg("ProposeAbortPlan failed")
			}
		}
		return // active plan in progress — do not propose another
	}

	snap := fsm.LoadSnapshot()
	if len(snap) < 2 {
		return
	}

	heaviest, lightest, diff := findImbalance(snap)
	if diff < r.cfg.ImbalanceThresh {
		return
	}

	groupID := r.selectGroupToMigrate(heaviest)
	if groupID == "" {
		return
	}

	planID, err := uuid.NewV7()
	if err != nil {
		r.logger.Error().Err(err).Msg("uuid.NewV7 failed")
		return
	}
	plan := RebalancePlan{
		PlanID:    planID.String(),
		GroupID:   groupID,
		FromNode:  heaviest,
		ToNode:    lightest,
		CreatedAt: time.Now(),
	}
	r.logger.Info().
		Str("plan_id", plan.PlanID).
		Str("group", groupID).
		Str("from", heaviest).
		Str("to", lightest).
		Float64("diff_pct", diff).
		Msg("proposing rebalance plan")

	if err := r.meta.ProposeRebalancePlan(ctx, plan); err != nil {
		r.logger.Error().Err(err).Msg("ProposeRebalancePlan failed")
	}
}

// ExecutePlan calls GroupRebalancer.MoveReplica and commits AbortPlan on completion.
// Called by the onRebalancePlan FSM callback (separate goroutine per plan).
func (r *Rebalancer) ExecutePlan(ctx context.Context, plan *RebalancePlan) error {
	if r.rebalance == nil {
		return fmt.Errorf("rebalancer: GroupRebalancer not wired")
	}
	err := r.rebalance.MoveReplica(ctx, plan.GroupID, plan.FromNode, plan.ToNode)
	if err != nil {
		r.logger.Error().Err(err).Str("plan_id", plan.PlanID).Msg("MoveReplica failed, aborting plan")
		_ = r.meta.ProposeAbortPlan(ctx, plan.PlanID, clusterpb.AbortPlanReasonExecutionFailed)
		return err
	}
	// Success: AbortPlan serves as the plan completion marker.
	return r.meta.ProposeAbortPlan(ctx, plan.PlanID, clusterpb.AbortPlanReasonCompleted)
}

// selectGroupToMigrate returns the group ID where heaviestNode is a voter, or "".
func (r *Rebalancer) selectGroupToMigrate(heaviestNode string) string {
	for _, g := range r.groups.All() {
		for _, peer := range g.PeerIDs() {
			if peer == heaviestNode {
				return g.ID()
			}
		}
	}
	return ""
}

// findImbalance returns (heaviest, lightest, diff) from the load snapshot.
func findImbalance(snap map[string]LoadStatEntry) (heaviest, lightest string, diff float64) {
	var maxPct, minPct float64 = -1, 101
	for _, e := range snap {
		if e.DiskUsedPct > maxPct {
			maxPct = e.DiskUsedPct
			heaviest = e.NodeID
		}
		if e.DiskUsedPct < minPct {
			minPct = e.DiskUsedPct
			lightest = e.NodeID
		}
	}
	return heaviest, lightest, maxPct - minPct
}
