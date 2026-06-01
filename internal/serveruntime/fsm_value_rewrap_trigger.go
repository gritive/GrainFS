package serveruntime

import (
	"context"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
)

// newFSMValueRewrapTriggerLazy creates a rotation post-commit callback that,
// on the group leader, drains each owned data group's stale FSM values
// (policy:, obj:) onto the new active generation. Leader-only per group;
// followers receive the reseal via Raft apply.
//
// The trigger reads state.dgMgr at invocation time (lazy) rather than at wire
// time, so wireDEKKeeper (which runs before bootOwnedGroupsAndEC) can register
// it before the group backends are populated.
//
// Plan-gate fix #1 (single-flight): a per-group sync.Map guard prevents
// duplicate drain goroutines when two rotations land close together. The
// sync.Map is allocated once at wire time and persists across rotations.
//
// The rotation's activeGen (cmd.Gen) arrives as the trigger argument but is
// intentionally NOT forwarded to the drain: DrainFSMValueRewrap reads
// keeper-current at the top of each iteration so it converges toward whatever
// the keeper's active gen currently is. Forwarding a fixed gen here is what
// caused the back-to-back-rotation livelock; the argument is now ignored.
//
// After a successful drain, the leader proposes a CmdFSMValueResealDone marker.
// Raft ordering guarantees every node applies the marker after all preceding
// CmdResealFSMValues batches, so each node's post-apply hook fires exactly once
// with its store already clean. S7-1a-2.
func newFSMValueRewrapTriggerLazy(state *bootState) func(ctx context.Context, activeGen uint32) {
	var inProgress sync.Map // allocated once; persists across rotations

	return func(ctx context.Context, activeGen uint32) {
		dgMgr := state.dgMgr
		if dgMgr == nil {
			return
		}
		for _, dg := range dgMgr.All() {
			gb := dg.Backend()
			if gb == nil || gb.Node() == nil || !gb.Node().IsLeader() {
				continue
			}
			groupID := dg.ID()
			// Single-flight guard: skip if a drain is already running for this group.
			if _, loaded := inProgress.LoadOrStore(groupID, struct{}{}); loaded {
				log.Debug().Str("group", groupID).Uint32("active_gen", activeGen).
					Msg("fsm-value rewrap: drain already in progress for group; skipping duplicate rotation trigger")
				continue
			}
			go func(gb *cluster.GroupBackend, groupID string) {
				defer inProgress.Delete(groupID)
				// activeGen NOT forwarded: the drain tracks keeper-current.
				if err := cluster.DrainFSMValueRewrap(ctx, gb, 0); err != nil {
					log.Warn().Err(err).Str("group", groupID).Uint32("active_gen", activeGen).
						Msg("fsm-value rewrap drain incomplete; will retry on next rotation")
					return
				}
				// Drain converged: propose the ordering-fence marker so every node,
				// after applying all reseal batches, fires its re-Kick and reports
				// completion. activeGen is a log hint only — the re-Kick is gen-agnostic.
				if err := gb.ProposeFSMValueResealDone(ctx, activeGen); err != nil {
					log.Warn().Err(err).Str("group", groupID).Uint32("active_gen", activeGen).
						Msg("fsm-value reseal-done marker propose failed; ledger report deferred to next rotation")
				}
			}(gb, groupID)
		}
	}
}
