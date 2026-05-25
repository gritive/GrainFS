package serveruntime

import (
	"context"
	"os"
)

// Run is the cluster-mode server entry point. cmd/grainfs/runServe builds a
// Config from cobra flags and pre-resolves auth/encryptor inputs, then calls
// Run. Run owns lifecycle: starts every component, blocks on ctx.Done(), and
// orchestrates graceful shutdown.
//
// Boot decomposition (PRs 1-7, see docs/superpowers/specs/
// 2026-05-08-serveruntime-boot-decomposition.md): phase functions populate
// state and register cleanup. Cleanup runs LIFO at function exit.
func Run(ctx context.Context, cfg Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	state := newBootState(cfg)
	state.cancel = cancel
	// §5 T46: default banner sink. Tests using bootstrap.Run override
	// state.bannerWriter to a buffer before phase dispatch.
	state.bannerWriter = os.Stdout
	defer state.Cleanup()

	if err := runBootRuntime(ctx, state, bootRuntimeDeps{startRotationSocket: StartRotationSocket}); err != nil {
		return err
	}

	bootShutdownDrain(ctx, state)
	return nil
}
