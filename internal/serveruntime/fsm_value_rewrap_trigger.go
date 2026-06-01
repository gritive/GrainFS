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
// Plan-gate fix #2 (pin activeGen): activeGen is passed as cmd.Gen from
// handleDEKReplicatedRotate — it is NOT re-read from the keeper. This pins
// the drain to the generation that triggered the rotation.
//
// Epoch-neutral: this trigger does NOT call ProposeDEKRewrapProgress or
// advance CurrentRewrapLaneSetEpoch. That is S7-1a-2.
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
				if err := cluster.DrainFSMValueRewrap(ctx, gb, activeGen, 0); err != nil {
					log.Warn().Err(err).Str("group", groupID).Uint32("active_gen", activeGen).
						Msg("fsm-value rewrap drain incomplete; will retry on next rotation")
				}
			}(gb, groupID)
		}
	}
}
