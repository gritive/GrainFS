package serveruntime

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// newRewrapScrubberKick adapts a RewrapController into the scrubberKick
// signature WireDEKPostCommit expects. The kick runs in a post-commit
// goroutine with no caller to return to, so errors are logged, not propagated.
//
// On a clean, ready Kick (nil error), every retired generation below active is
// reported via report(nodeID, g). Reporting the full swept set — not just
// oldGen — self-heals a missed intermediate kick: a generation skipped by an
// earlier failure will be covered on the next successful kick. report may be
// nil (unit tests, pre-metaRaft boot); nil is a no-op. Kick errors (including
// errLanesNotReady) suppress reporting so only honest clean signals reach the
// ledger.
func newRewrapScrubberKick(
	ctrl *encrypt.RewrapController,
	nodeID string,
	report func(ctx context.Context, nodeID string, gen uint32) error,
) func(context.Context, uint32) {
	return func(ctx context.Context, oldGen uint32) {
		if err := ctrl.Kick(ctx, oldGen); err != nil {
			log.Warn().Err(err).Uint32("old_gen", oldGen).Msg("dek rewrap kick incomplete or not ready; not reporting completion")
			return
		}
		if report == nil {
			return
		}
		for _, g := range ctrl.RetiredGensBelowActive() {
			if err := report(ctx, nodeID, g); err != nil {
				log.Warn().Err(err).Uint32("gen", g).Msg("dek rewrap: completion report failed; prune will stall until re-kick")
			}
		}
	}
}
