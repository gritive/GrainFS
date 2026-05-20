// §5 T44: TLS posture gate.
//
// Refuses to start (and refuses runtime flips) when the cluster lands in a
// posture where authenticated requests would traverse plaintext: anon disabled,
// no server TLS cert, and no trusted-proxy CIDR carrying client identity from
// a TLS-terminating front-end.
//
// Determinism split — boot vs. reload hook:
//
//   - Boot path (bootTLSPostureGate) checks all three: anon, on-disk cert,
//     trusted-proxy.cidr. Per-node cert presence is fine here because each
//     node makes its own start/refuse decision locally.
//
//   - iam.anon-enabled reload hook checks only anon + trusted-proxy.cidr.
//     The cert check is INTENTIONALLY OMITTED: MetaCmdConfigPut is replicated
//     via raft and applied on every node, so an apply-time error from one
//     follower (cert missing) while the leader's apply succeeds (cert present)
//     would leave the cluster's anon setting silently diverged — log entry
//     committed in raft, value rolled back locally on the failing follower.
//     The "cert deployed unevenly" case is caught instead by that node's
//     bootTLSPostureGate the next time it boots. Failing at the right place.
//
// Lock note: the iam.anon-enabled reload hook fires from inside
// config.Store.Set while the store's write lock is held, so the hook MUST NOT
// call cfg.GetBool / cfg.GetString (they take RLock → self-deadlock).
// trusted-proxy.cidr is tracked in a closure-captured atomic.Pointer[string]
// kept fresh by a sibling OnTrustedProxyCIDR hook.
package serveruntime

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// enforceTLSPostureValues is the pure boot-path form. Returns nil when anon is
// enabled, or a cert is on disk, or a trusted-proxy CIDR is set. The error
// message names all three remediation options.
func enforceTLSPostureValues(anon bool, certPath, keyPath, proxyCIDR string) error {
	if anon {
		return nil
	}
	if _, err := os.Stat(certPath); err == nil {
		return nil
	}
	if proxyCIDR != "" {
		return nil
	}
	return fmt.Errorf("auth required + no TLS cert + no trusted proxy. "+
		"Place cert at %s + %s, OR "+
		"set GRAINFS_TLS_CERT/KEY, OR "+
		"`grainfs config set trusted-proxy.cidr <cidr>`",
		certPath, keyPath)
}

// enforceTLSPosture is the boot-path wrapper. Reads anon + trusted-proxy.cidr
// from cfg (under RLock — safe at boot, no Set in flight) and the cert paths
// from nc. Call only from outside config.Store.Set.
func enforceTLSPosture(cfg *config.Store, nc *nodeconfig.NodeConfig) error {
	if cfg == nil || nc == nil {
		return nil
	}
	anon, _ := cfg.GetBool("iam.anon-enabled")
	proxy, _ := cfg.GetString("trusted-proxy.cidr")
	return enforceTLSPostureValues(anon, nc.TLSCertPath(), nc.TLSKeyPath(), proxy)
}

// reloadHookPostureCheck is the cluster-deterministic form used by the
// iam.anon-enabled reload hook. Does NOT touch the filesystem — see the
// "Determinism split" note in the package doc-comment for why.
func reloadHookPostureCheck(anon bool, proxyCIDR string) error {
	if anon {
		return nil
	}
	if proxyCIDR != "" {
		return nil
	}
	return fmt.Errorf("auth required + no trusted proxy. " +
		"Set GRAINFS_TLS_CERT/KEY and restart, OR " +
		"`grainfs config set trusted-proxy.cidr <cidr>`")
}

// bootTLSPostureGate runs enforceTLSPosture as a boot phase. Wired in Run()
// AFTER bootHTTPServerAndAdmin (so state.cfgStore is populated and state.srv
// is constructed) and BEFORE bootResharderAndDegraded (which goroutines
// srv.Run() — the listener actually starts there). The trusted-proxy.cidr
// atomic snapshot was already reconciled right after bootSnapshotAndApplyLoop.
func bootTLSPostureGate(state *bootState) error {
	nc := nodeconfig.New(state.cfg.DataDir)
	return enforceTLSPosture(state.cfgStore, nc)
}

// wireTLSPostureHooks builds the OnAnonEnabledChange + OnTrustedProxyCIDR pair
// that keep the runtime posture gate live.
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
