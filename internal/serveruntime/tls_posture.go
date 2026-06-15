// §5 T44: TLS posture helpers.
package serveruntime

import (
	"context"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// enforceTLSPosture is retained as a boot hook, but no longer gates startup on
// the removed iam.anon-enabled key. TLS requirements are now operator policy,
// not a side effect of first service-account bootstrap.
func enforceTLSPosture(cfg *config.Store, nc *nodeconfig.NodeConfig) error {
	return nil
}

// reloadHookPostureCheck is retained for older tests and callers. With the
// global anonymous switch removed, config reloads no longer reject TLS posture.
func reloadHookPostureCheck(anon bool, proxyCIDR string) error {
	return nil
}

// bootTLSPostureGate runs enforceTLSPosture as a boot phase. Wired in Run()
// AFTER bootHTTPServerAndAdmin (so state.cfgStore is populated and state.srv
// is constructed) and BEFORE bootDegradedAndServices (which goroutines
// srv.Run() — the listener actually starts there). The trusted-proxy.cidr
// atomic snapshot was already reconciled right after bootSnapshotAndApplyLoop.
func bootTLSPostureGate(state *bootState) error {
	nc := nodeconfig.New(state.cfg.DataDir)
	return enforceTLSPosture(state.cfgStore, nc)
}

// iamPostureChecker preserves the AdminAPI dependency shape. SA creation no
// longer flips a global anonymous config key, so the check is a no-op.
type iamPostureChecker struct {
	cfg *config.Store
	nc  *nodeconfig.NodeConfig
}

func (p *iamPostureChecker) CheckAnonOff(_ context.Context) error {
	return nil
}

// newIAMPostureChecker builds the adapter wired into AdminAPI at boot.
func newIAMPostureChecker(cfg *config.Store, nc *nodeconfig.NodeConfig) *iamPostureChecker {
	return &iamPostureChecker{cfg: cfg, nc: nc}
}

// wireTLSPostureHooks builds compatibility posture hooks plus the
// OnTrustedProxyCIDR hook that keeps the runtime proxy snapshot live.
//
// The trusted-proxy.cidr value is captured in a *atomic.Pointer[string] so the
// anon-change hook can read it without taking the store's RLock (it fires
// while Set holds the write lock — RLock would self-deadlock). Both hooks
// fire from the FSM apply path (single-writer), so atomic.Store is sufficient
// to keep the snapshot coherent.
//
// initialProxyCIDR should be the value read from cfgStore at wire time, BEFORE
// any Set has run — typically "" (the registered default). A refresh callback
// is also returned so that snapshot-Restore (which does NOT fire reload hooks)
// can be reconciled into the snapshot once at boot.
func wireTLSPostureHooks(initialProxyCIDR string) (
	onAnon func(context.Context, bool) error,
	onProxy func(context.Context, string) error,
	refresh func(string),
) {
	var proxySnap atomic.Pointer[string]
	v := initialProxyCIDR
	proxySnap.Store(&v)

	onAnon = func(_ context.Context, newAnon bool) error {
		proxyPtr := proxySnap.Load()
		proxy := ""
		if proxyPtr != nil {
			proxy = *proxyPtr
		}
		return reloadHookPostureCheck(newAnon, proxy)
	}
	onProxy = func(_ context.Context, newProxy string) error {
		// Update the snapshot first so a concurrent anon-change observes the
		// new value. Single-writer FSM apply path → no need for CAS.
		v := newProxy
		proxySnap.Store(&v)
		return nil
	}
	refresh = func(latest string) {
		v := latest
		proxySnap.Store(&v)
	}
	return onAnon, onProxy, refresh
}
