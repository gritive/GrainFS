// §5 T46: Phase 0 anon banner wiring.
//
// Two emission sites:
//
//  1. Startup: bootPhase0Banner reads iam.anon-enabled from cfgStore once,
//     after the TLS posture gate has accepted the cluster posture, and writes
//     the WARN banner to state.bannerWriter (os.Stdout in production).
//
//  2. Runtime flip true→false: composeAnonHookWithBanner wraps the existing
//     OnAnonEnabledChange hook (the TLS posture reload check). When the
//     posture check accepts the flip and the previous value was true, the
//     hook emits the INFO "remains public" banner exactly once per transition.
//
// Lock note: like the wireTLSPostureHooks anon hook, the composed hook fires
// from inside config.Store.Set under the write lock, so it MUST NOT call
// cfg.GetBool — the previous value is tracked in a closure-captured
// atomic.Bool (lock-free per project memory rule). The TLS posture inner hook
// is invoked first; if it errors (rolling the Set back), the snapshot is left
// untouched so the next attempt sees the still-current "true".
package serveruntime

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/server"
)

// composeAnonHookWithBanner returns a new OnAnonEnabledChange hook and a
// seedPrev function. The hook runs the supplied inner hook (typically the TLS
// posture check from wireTLSPostureHooks) and, on a successful true→false
// transition, emits the "s3://default remains public" INFO banner exactly once
// per transition.
//
// initialAnon must be the value of iam.anon-enabled in the cfgStore at wire
// time. It seeds the atomic prev snapshot so the first hook firing has a
// correct previous value to compare against.
//
// seedPrev re-seeds the internal prev snapshot after a raft Restore: because
// Restore does not fire reload hooks, a subsequent legitimate Set would compare
// against the stale wire-time seed rather than the restored value. Call
// seedPrev with the restored iam.anon-enabled value from the post-restore
// callback (F26).
//
// w is the destination for the INFO banner (os.Stdout in production, a buffer
// in tests). A nil writer disables the banner emission but the wrapper still
// delegates to inner — useful when the runtime is constructed without a banner
// sink (some embedded test harnesses).
func composeAnonHookWithBanner(
	inner func(context.Context, bool) error,
	initialAnon bool,
	w io.Writer,
) (hook func(context.Context, bool) error, seedPrev func(v bool)) {
	var prev atomic.Bool
	prev.Store(initialAnon)
	hook = func(ctx context.Context, newAnon bool) error {
		if inner != nil {
			if err := inner(ctx, newAnon); err != nil {
				// Posture check rejected — Set will roll back; keep snapshot
				// unchanged so the next attempt observes the still-true value.
				return err
			}
		}
		old := prev.Swap(newAnon)
		if old && !newAnon && w != nil {
			server.EmitAnonDisabledBanner(w)
		}
		return nil
	}
	seedPrev = func(v bool) { prev.Store(v) }
	return hook, seedPrev
}

// bootPhase0Banner emits the Phase 0 anonymous-access startup banner to
// state.bannerWriter when iam.anon-enabled is true. Wired in Run() right
// after bootTLSPostureGate so the banner only prints when the posture gate
// has already accepted the cluster configuration — otherwise a refused boot
// would noise the operator with a banner that contradicts the gate's error.
func bootPhase0Banner(state *bootState) error {
	if state == nil || state.cfgStore == nil || state.bannerWriter == nil {
		return nil
	}
	anon, _ := state.cfgStore.GetBool("iam.anon-enabled")
	server.EmitBanner(state.bannerWriter, anon, false)
	return nil
}
