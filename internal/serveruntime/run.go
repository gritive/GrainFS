package serveruntime

import (
	"context"
	"fmt"
	"os"
)

// Run is the cluster-mode server entry point. cmd/grainfs/runServe builds a
// Config from cobra flags and pre-resolves auth/encryptor inputs, then calls
// Run. Run owns lifecycle: starts every component, blocks on ctx.Done(), and
// orchestrates graceful shutdown.
//
// Boot decomposition (PRs 1-7, see docs/superpowers/specs/
// 2026-05-08-serveruntime-boot-decomposition.md): phase functions populate
// state and register cleanup. Cleanup runs LIFO at function exit. The boot
// order itself is data — bootSequence() (boot_sequence.go) — which Run loops
// fail-fast, labelling every error with its phase name.
func Run(ctx context.Context, cfg Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	state := newBootState(cfg)
	state.cancel = cancel
	// §5 T46: default banner sink. Tests using bootstrap.Run override
	// state.bannerWriter to a buffer before phase dispatch.
	state.bannerWriter = os.Stdout
	// Production rotation-socket starter for bootMetaRaftStart. Tests that
	// exercise the phase directly leave this nil (a no-op). This is setup, not
	// a phase: it wires a function the sequence consumes.
	state.startRotationSocket = StartRotationSocket
	defer state.Cleanup()

	// Order-as-data: the boot order lives in bootSequence() as an explicit,
	// ordered slice (boot_sequence.go). Each phase runs fail-fast; failures get
	// a free operator-facing label ("boot phase <name>: ..."). Cleanups
	// registered by phases run LIFO via state.Cleanup (deferred above). The
	// success path is behavior-preserving versus the former hand-inlined list.
	for _, p := range bootSequence() {
		if err := p.run(ctx, state); err != nil {
			return fmt.Errorf("boot phase %s: %w", p.name, err)
		}
	}

	// bootShutdownDrain blocks on ctx.Done() then runs graceful shutdown. It
	// returns no error, so it is not a sequence phase.
	bootShutdownDrain(ctx, state)
	return nil
}
