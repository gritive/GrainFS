// §5 T44: TLS posture gate.
//
// Refuses to start (and refuses runtime flips) when the cluster lands in a
// posture where authenticated requests would traverse plaintext: anon disabled,
// no server TLS cert, and no trusted-proxy CIDR carrying client identity from
// a TLS-terminating front-end. The same check runs both at boot and from the
// iam.anon-enabled reload hook so an operator cannot disable anon at runtime
// into an unsafe posture — config.Store.Set rolls back on hook error
// (internal/config/config.go:94-101).
//
// Lock note: the iam.anon-enabled reload hook fires from inside config.Store.Set
// while the store's write lock is held, so the hook MUST NOT call cfg.GetBool /
// cfg.GetString (they take RLock → self-deadlock). Boot path reads via cfg
// freely; the hook reads trusted-proxy.cidr from a closure-captured atomic
// snapshot kept in sync by a sibling OnTrustedProxyCIDR hook.
package serveruntime

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// enforceTLSPostureValues is the pure form of the posture check. Returns nil
// when anon is enabled, or a cert is on disk, or a trusted-proxy CIDR is set.
// The error message names all three remediation options.
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

// bootTLSPostureGate runs enforceTLSPosture as a boot phase. Wired in Run()
// AFTER bootHTTPServerAndAdmin (so state.cfgStore is populated and state.srv
// is constructed) and BEFORE bootResharderAndDegraded (which goroutines
// srv.Run() — the listener actually starts there).
//
// Also re-syncs the trusted-proxy.cidr atomic snapshot from cfgStore: snapshot
// Restore (meta_fsm.go:3233) does NOT fire reload hooks, so after a node boots
// from a restored snapshot the in-memory snapshot needs a one-shot reconcile
// before the hook can answer correctly.
func bootTLSPostureGate(state *bootState) error {
	nc := nodeconfig.New(state.cfg.DataDir)
	if state.refreshProxyCIDR != nil && state.cfgStore != nil {
		v, _ := state.cfgStore.GetString("trusted-proxy.cidr")
		state.refreshProxyCIDR(v)
	}
	return enforceTLSPosture(state.cfgStore, nc)
}

// wireTLSPostureHooks builds the OnAnonEnabledChange + OnTrustedProxyCIDR pair
// that keep the posture gate live at runtime.
//
// The trusted-proxy.cidr value is captured in a *atomic.Pointer[string] so the
// anon-change hook can read it without taking the store's RLock (it fires
// while Set holds the write lock — RLock would self-deadlock). Both hooks
// fire from the FSM apply path (single-writer), so atomic.Store is sufficient
// to keep the snapshot coherent for the boot-goroutine reader race window.
//
// initialProxyCIDR should be the value read from cfgStore at wire time, BEFORE
// any Set has run — typically cfgStore.GetString("trusted-proxy.cidr") which
// returns the registered default ("") on a fresh store.
func wireTLSPostureHooks(dataDir, initialProxyCIDR string) (
	onAnon func(context.Context, bool) error,
	onProxy func(context.Context, string) error,
	refresh func(string),
) {
	nc := nodeconfig.New(dataDir)
	var proxySnap atomic.Pointer[string]
	v := initialProxyCIDR
	proxySnap.Store(&v)

	onAnon = func(_ context.Context, newAnon bool) error {
		certPath := nc.TLSCertPath()
		keyPath := nc.TLSKeyPath()
		proxyPtr := proxySnap.Load()
		proxy := ""
		if proxyPtr != nil {
			proxy = *proxyPtr
		}
		return enforceTLSPostureValues(newAnon, certPath, keyPath, proxy)
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
