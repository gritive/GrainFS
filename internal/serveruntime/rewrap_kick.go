package serveruntime

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// rewrapProgressReportsTotal counts successful ProposeDEKRewrapProgress calls,
// labelled by epoch. An increment at epoch="1" means every registered lane
// (EC+packblob+FSMvalue-check) verified clean on this node. S7-1a-2.
var rewrapProgressReportsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "grainfs_rewrap_progress_reports_total",
	Help: "Successful DEK rewrap completion reports proposed to the meta-raft ledger, labelled by epoch.",
}, []string{"epoch"})

// newRewrapScrubberKick adapts a RewrapController into the scrubberKick
// signature WireDEKPostCommit expects. The kick runs in a post-commit
// goroutine with no caller to return to, so errors are logged, not propagated.
//
// On a clean, ready Kick (nil error), every retired generation below active is
// reported via report(ctx, nodeID, gen, epoch) with epoch=CurrentRewrapLaneSetEpoch.
// Reporting the full swept set — not just oldGen — self-heals a missed
// intermediate kick: a generation skipped by an earlier failure will be covered
// on the next successful kick. report may be nil (unit tests, pre-metaRaft
// boot); nil is a no-op. Kick errors (including errLanesNotReady) suppress
// reporting so only honest clean signals reach the ledger.
func newRewrapScrubberKick(
	ctrl *encrypt.RewrapController,
	nodeID string,
	report func(ctx context.Context, nodeID string, gen, epoch uint32) error,
) func(context.Context, uint32) {
	return func(ctx context.Context, oldGen uint32) {
		activeGen, err := ctrl.Kick(ctx, oldGen)
		if err != nil {
			log.Warn().Err(err).Uint32("old_gen", oldGen).Msg("dek rewrap kick incomplete or not ready; not reporting completion")
			return
		}
		if report == nil {
			return
		}
		// Report the gen-set derived from the SAME activeGen the sweep used, not
		// a fresh keeper read — avoids reporting a gen just swept onto as done if
		// a rotation races between the sweep and the report.
		epoch := cluster.CurrentRewrapLaneSetEpoch
		epochLabel := fmt.Sprintf("%d", epoch)
		for _, g := range ctrl.RetiredGensBelow(activeGen) {
			if err := report(ctx, nodeID, g, epoch); err != nil {
				log.Warn().Err(err).Uint32("gen", g).Msg("dek rewrap: completion report failed; prune will stall until re-kick")
				continue
			}
			rewrapProgressReportsTotal.WithLabelValues(epochLabel).Inc()
		}
	}
}
