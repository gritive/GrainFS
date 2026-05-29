package cluster

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// revocationSource is the evacuator's read view of the meta FSM.
type revocationSource interface {
	RevokedNodeIDs() map[string]struct{}
	Nodes() []MetaNodeEntry
}

// voterMover removes a revoked voter from a data group. Production impl is
// *DataGroupPlanExecutor (EvacuateVoter); tests inject a fake. EvacuateVoter is
// idempotent and itself decides shrink vs. move from the real raft config (P1-2).
type voterMover interface {
	EvacuateVoter(ctx context.Context, groupID, revokedID, replacementID string) error
}

// evacTarget is one (group I lead, revoked voter still present) pair.
type evacTarget struct {
	groupID     string
	revokedNode string
	peerIDs     []string
}

// DataGroupEvacuator evicts revoked nodes from the data groups THIS node leads.
// Periodic tick (replay/crash-safe) plus an onNodeRevoked wake. Each group leader
// evicts independently. It does NOT touch the PeerIDs mirror directly — convergence
// is a consequence of EvacuateVoter's real ChangeMembership (P1-1).
type DataGroupEvacuator struct {
	localNodeID string
	src         revocationSource
	mover       voterMover
	logger      zerolog.Logger

	ledTargets  func() []evacTarget
	pickHealthy func(groupID string, exclude map[string]struct{}) (string, bool)

	tick     time.Duration
	wakeCh   chan struct{}
	stopCh   chan struct{}
	stopOnce sync.Once
}

// reconcileOnce performs a single eviction pass over all led targets. Idempotent:
// no-op when no led group contains a revoked voter. Errors (incl.
// ErrLeadershipTransferred, transient not-leader, quorum-blocked) are logged and
// retried on the next tick — never fatal.
func (e *DataGroupEvacuator) reconcileOnce(ctx context.Context) {
	revoked := e.src.RevokedNodeIDs()
	if len(revoked) == 0 {
		return
	}
	exclude := make(map[string]struct{}, len(revoked))
	for id := range revoked {
		exclude[id] = struct{}{}
	}

	for _, tgt := range e.ledTargets() {
		if !slices.Contains(tgt.peerIDs, tgt.revokedNode) {
			continue
		}
		survivors := 0
		for _, p := range tgt.peerIDs {
			if p != tgt.revokedNode {
				survivors++
			}
		}
		if survivors == 0 {
			e.logger.Warn().Str("group", tgt.groupID).Str("revoked", tgt.revokedNode).
				Msg("evacuator: refusing to evict last voter; leaving for operator")
			continue
		}

		replacement := "" // "" => shrink
		if to, ok := e.pickHealthy(tgt.groupID, exclude); ok && to != "" {
			replacement = to
		}
		if err := e.mover.EvacuateVoter(ctx, tgt.groupID, tgt.revokedNode, replacement); err != nil {
			if errors.Is(err, ErrLeadershipTransferred) {
				e.logger.Info().Str("group", tgt.groupID).
					Msg("evacuator: leadership transferred; new leader will retry")
				continue
			}
			e.logger.Error().Err(err).Str("group", tgt.groupID).
				Str("revoked", tgt.revokedNode).Msg("evacuator: eviction failed; retry next tick")
		}
	}
}

// Wake nudges the reconciler to run promptly. Non-blocking; coalesces.
func (e *DataGroupEvacuator) Wake() {
	select {
	case e.wakeCh <- struct{}{}:
	default:
	}
}

// Run drives reconcileOnce on a ticker and on every Wake. Blocks until ctx done or Stop().
func (e *DataGroupEvacuator) Run(ctx context.Context) {
	t := time.NewTicker(e.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		case <-t.C:
			e.reconcileOnce(ctx)
		case <-e.wakeCh:
			e.reconcileOnce(ctx)
		}
	}
}

// Stop signals Run to exit. Idempotent.
func (e *DataGroupEvacuator) Stop() { e.stopOnce.Do(func() { close(e.stopCh) }) }

// NewDataGroupEvacuator builds the production evacuator. ledTargets derives groups
// this node LEADS that still contain a revoked voter. loadPick picks a non-revoked,
// non-member replacement. It drives the idempotent EvacuateVoter; PeerIDs
// convergence is a CONSEQUENCE of that real ChangeMembership (P1-1) — the evacuator
// NEVER prunes a mirror directly.
func NewDataGroupEvacuator(
	localNodeID string,
	fsm *MetaFSM,
	dgMgr *DataGroupManager,
	exec *DataGroupPlanExecutor,
	loadPick func(groupID string, exclude map[string]struct{}) (string, bool),
	tick time.Duration,
) *DataGroupEvacuator {
	e := &DataGroupEvacuator{
		localNodeID: localNodeID,
		src:         fsm,
		mover:       exec,
		logger:      log.With().Str("component", "evacuator").Logger(),
		pickHealthy: loadPick,
		tick:        tick,
		wakeCh:      make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}
	e.ledTargets = func() []evacTarget {
		revoked := fsm.RevokedNodeIDs()
		if len(revoked) == 0 {
			return nil
		}
		var out []evacTarget
		for _, dg := range dgMgr.All() {
			if dg == nil || dg.Backend() == nil || dg.Backend().Node() == nil {
				continue
			}
			if !dg.Backend().Node().IsLeader() {
				continue
			}
			for _, p := range dg.PeerIDs() {
				if _, isRevoked := revoked[p]; isRevoked {
					out = append(out, evacTarget{groupID: dg.ID(), revokedNode: p, peerIDs: dg.PeerIDs()})
				}
			}
		}
		return out
	}
	return e
}

// PickHealthyExcluding returns the lightest (lowest load) candidate not in exclude,
// or ("", false) to signal a shrink. loadFn returns (load, known).
func PickHealthyExcluding(candidates []string, loadFn func(id string) (float64, bool), exclude map[string]struct{}) (string, bool) {
	best := ""
	var bestLoad float64
	for _, id := range candidates {
		if _, ex := exclude[id]; ex {
			continue
		}
		l, _ := loadFn(id)
		if best == "" || l < bestLoad {
			best = id
			bestLoad = l
		}
	}
	if best == "" {
		return "", false
	}
	return best, true
}

// newDataGroupEvacuatorForTest builds an evacuator with injected derivation
// functions and a tiny tick. Test-only.
func newDataGroupEvacuatorForTest(
	localNodeID string,
	revoked map[string]struct{},
	mover voterMover,
	ledTargets func() []evacTarget,
	pickHealthy func(string, map[string]struct{}) (string, bool),
) *DataGroupEvacuator {
	return &DataGroupEvacuator{
		localNodeID: localNodeID,
		src:         staticRevocationSource{revoked: revoked},
		mover:       mover,
		logger:      log.With().Str("component", "evacuator-test").Logger(),
		ledTargets:  ledTargets,
		pickHealthy: pickHealthy,
		tick:        time.Millisecond,
		wakeCh:      make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}
}

type staticRevocationSource struct{ revoked map[string]struct{} }

func (s staticRevocationSource) RevokedNodeIDs() map[string]struct{} { return s.revoked }
func (s staticRevocationSource) Nodes() []MetaNodeEntry              { return nil }
