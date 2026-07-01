package cluster

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// shardGroupRosterSource is the evacuator's read view of the meta FSM.
type shardGroupRosterSource interface {
	RevokedNodeIDs() map[string]struct{}
	ShardGroups() []ShardGroupEntry
	ShardGroup(id string) (ShardGroupEntry, bool)
}

type shardGroupRosterUpdater interface {
	ProposeShardGroupForwarding(ctx context.Context, sg ShardGroupEntry) error
}

// evacTarget is one (group roster, revoked peer still present) pair.
type evacTarget struct {
	groupID     string
	revokedNode string
	peerIDs     []string
}

// DataGroupEvacuator evicts revoked nodes from shard-group placement rosters.
// Periodic tick (replay/crash-safe) plus an onNodeRevoked wake makes the
// reconciler idempotent across crashes and follower boots. The meta-FSM
// ShardGroupEntry is the source of truth; per-group raft membership is not
// consulted or mutated.
type DataGroupEvacuator struct {
	src     shardGroupRosterSource
	updater shardGroupRosterUpdater
	logger  zerolog.Logger

	ledTargets  func() []evacTarget
	pickHealthy func(groupID string, exclude map[string]struct{}) (string, bool)

	tick     time.Duration
	wakeCh   chan struct{}
	stopCh   chan struct{}
	stopOnce sync.Once
}

// reconcileOnce performs a single eviction pass over all shard-group rosters.
// Idempotent: no-op when no group contains a revoked peer. Proposal errors are
// logged and retried on the next tick — never fatal.
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
		entry, ok := e.src.ShardGroup(tgt.groupID)
		if !ok || !slices.Contains(entry.PeerIDs, tgt.revokedNode) {
			continue
		}
		survivors := 0
		for _, p := range entry.PeerIDs {
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
		nextPeers := removePeerID(entry.PeerIDs, tgt.revokedNode)
		if replacement != "" && !slices.Contains(nextPeers, replacement) {
			nextPeers = append(nextPeers, replacement)
		}
		if slices.Equal(entry.PeerIDs, nextPeers) {
			continue
		}
		if err := e.updater.ProposeShardGroupForwarding(ctx, ShardGroupEntry{ID: tgt.groupID, PeerIDs: nextPeers}); err != nil {
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
// whose placement roster still contains a revoked peer. loadPick picks a
// non-revoked, non-member replacement; no replacement means shrink.
func NewDataGroupEvacuator(
	fsm *MetaFSM,
	updater shardGroupRosterUpdater,
	loadPick func(groupID string, exclude map[string]struct{}) (string, bool),
	tick time.Duration,
) *DataGroupEvacuator {
	e := &DataGroupEvacuator{
		src:         fsm,
		updater:     updater,
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
		for _, sg := range fsm.ShardGroups() {
			for _, p := range sg.PeerIDs {
				if _, isRevoked := revoked[p]; isRevoked {
					out = append(out, evacTarget{groupID: sg.ID, revokedNode: p, peerIDs: sg.PeerIDs})
				}
			}
		}
		return out
	}
	return e
}

func removePeerID(peers []string, remove string) []string {
	out := make([]string, 0, len(peers))
	for _, p := range peers {
		if p != remove {
			out = append(out, p)
		}
	}
	return out
}

// PickHealthyExcluding returns the lightest (lowest load) candidate not in exclude,
// or ("", false) to signal a shrink. loadFn returns (load, known). Candidates with
// no load signal (known==false) are SKIPPED, not treated as load 0 — a
// down/non-reporting node must never out-rank a live loaded one, which would make
// the evacuator loop forever trying to add an unavailable replacement. With no
// known-load candidate we return ("", false) so the caller shrinks instead.
func PickHealthyExcluding(candidates []string, loadFn func(id string) (float64, bool), exclude map[string]struct{}) (string, bool) {
	best := ""
	var bestLoad float64
	for _, id := range candidates {
		if _, ex := exclude[id]; ex {
			continue
		}
		l, known := loadFn(id)
		if !known {
			continue // no health/load signal: prefer shrink over placing on a possibly-dead node
		}
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
