package serveruntime

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// newRewrapScrubberKick adapts a RewrapController into the scrubberKick
// signature WireDEKPostCommit expects. The kick runs in a post-commit
// goroutine with no caller to return to, so errors are logged, not propagated.
func newRewrapScrubberKick(ctrl *encrypt.RewrapController) func(context.Context, uint32) {
	return func(ctx context.Context, oldGen uint32) {
		if err := ctrl.Kick(ctx, oldGen); err != nil {
			log.Warn().Err(err).Uint32("old_gen", oldGen).Msg("dek rewrap kick failed")
		}
	}
}
