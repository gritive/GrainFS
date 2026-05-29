package pdp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/gritive/GrainFS/internal/metrics"
)

type spyInner struct {
	decision policy.Decision
	calls    int32
}

func (s *spyInner) Authorize(_ context.Context, _, _ string, _ policy.RequestContext) policy.EvalResult {
	atomic.AddInt32(&s.calls, 1)
	return policy.EvalResult{Decision: s.decision}
}
func (s *spyInner) AuthorizePrincipal(_ context.Context, _ principal.Principal, _ string, _ policy.RequestContext) policy.EvalResult {
	atomic.AddInt32(&s.calls, 1)
	return policy.EvalResult{Decision: s.decision}
}

type staticCfg string

func (s staticCfg) GetString(key string) (string, bool) {
	if key == ConfigKey {
		return string(s), true
	}
	return "", false
}

// decoHTTPPDP starts a loopback http httptest mock PDP and returns its URL
// (http://127.0.0.1:PORT). Loopback http is the one address an http remote PDP is
// allowed to target, so it passes both ParseConfig and the SSRF dial filter.
func decoHTTPPDP(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

func decoCfg(endpoint, policyMode string) string {
	b, _ := json.Marshal(map[string]any{"enabled": true, "endpoint": endpoint, "failure_policy": policyMode})
	return string(b)
}

func TestDecoratorDisabledIsPassThrough(t *testing.T) {
	inner := &spyInner{decision: policy.DecisionAllow}
	d := NewDecorator(inner, staticCfg(`{"enabled":false}`), nil, "admin")
	got := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got.Decision)
	require.EqualValues(t, 1, inner.calls)
}

type missingCfg struct{}

func (missingCfg) GetString(string) (string, bool) { return "", false }

func TestDecoratorMalformedConfigIsPassThrough(t *testing.T) {
	inner := &spyInner{decision: policy.DecisionAllow}
	d := NewDecorator(inner, staticCfg("{"), nil, "admin")
	got := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got.Decision)
	require.EqualValues(t, 1, inner.calls)

	inner2 := &spyInner{decision: policy.DecisionAllow}
	d2 := NewDecorator(inner2, staticCfg("{"), nil, "admin")
	got2 := d2.Authorize(context.Background(), "sa", "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got2.Decision)
	require.EqualValues(t, 1, inner2.calls)
}

func TestDecoratorMissingConfigKeyIsPassThrough(t *testing.T) {
	inner := &spyInner{decision: policy.DecisionAllow}
	d := NewDecorator(inner, missingCfg{}, nil, "admin")
	got := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got.Decision)
	require.EqualValues(t, 1, inner.calls)

	inner2 := &spyInner{decision: policy.DecisionAllow}
	d2 := NewDecorator(inner2, missingCfg{}, nil, "admin")
	got2 := d2.Authorize(context.Background(), "sa", "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got2.Decision)
	require.EqualValues(t, 1, inner2.calls)
}

func TestDecoratorShortCircuitsInnerDeny(t *testing.T) {
	var dialed int32
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) { atomic.AddInt32(&dialed, 1) })
	inner := &spyInner{decision: policy.DecisionDeny}
	d := NewDecorator(inner, staticCfg(decoCfg(endpoint, "closed")), nil, "admin")
	got := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionDeny, got.Decision)
	require.EqualValues(t, 0, dialed)
}

func TestDecoratorAllowAndAllow(t *testing.T) {
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"decision":"allow"}`)) })
	inner := &spyInner{decision: policy.DecisionAllow}
	d := NewDecorator(inner, staticCfg(decoCfg(endpoint, "closed")), nil, "admin")
	got := d.AuthorizePrincipal(context.Background(), principal.OIDC("iss", "sub", "oidc:x:u", []string{"g"}), "", policy.RequestContext{Action: "grainfs:CredentialCreate", Resource: "protocol-credential/nbd/v/d"})
	require.Equal(t, policy.DecisionAllow, got.Decision)
}

func TestDecoratorAllowThenPDPDeny(t *testing.T) {
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"decision":"deny","reason":"blocked"}`))
	})
	inner := &spyInner{decision: policy.DecisionAllow}
	d := NewDecorator(inner, staticCfg(decoCfg(endpoint, "closed")), nil, "admin")
	got := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionDeny, got.Decision)
	require.Contains(t, got.Reason, "pdp_deny")
}

func TestDecoratorFailClosedVsOpen(t *testing.T) {
	down := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) })
	closed := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(decoCfg(down, "closed")), nil, "admin")
	require.Equal(t, policy.DecisionDeny, closed.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"}).Decision)
	open := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(decoCfg(down, "open")), nil, "admin")
	got := open.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got.Decision)
	require.Contains(t, got.Reason, "pdp_skipped_fail_open")
}

func TestDecoratorCanceledCtxDeniesEvenFailOpen(t *testing.T) {
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"decision":"allow"}`)) })
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(decoCfg(endpoint, "open")), nil, "admin")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := d.AuthorizePrincipal(ctx, principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionDeny, got.Decision)
	require.Contains(t, got.Reason, "request canceled")
}

// TestDecoratorCanceledCtxIgnoresFreshCache proves an already-canceled inbound
// ctx denies "request canceled" even when the cache holds a fresh allow: the
// pre-cache cancel check must short-circuit BEFORE the fresh-hit path so the
// abandoned request cannot be resurrected by a cached allow, and emits no
// cache_total.
func TestDecoratorCanceledCtxIgnoresFreshCache(t *testing.T) {
	metrics.PDPCacheTotal.Reset()
	var dialed int32
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dialed, 1)
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	// Long ttl_allow keeps the primed entry fresh at the canceled-call time
	// (no d.now advancement, so the entry is still fresh on the 2nd call).
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(cacheCfg(endpoint, "closed", "10m", "", "")), nil, "admin")

	// Prime a FRESH allow (live ctx). Same actor/bucket/ctxReq as the canceled
	// call below so both produce the SAME cache key (a genuine fresh hit).
	got := d.Authorize(context.Background(), "sa", "", reqCtx())
	require.Equal(t, policy.DecisionAllow, got.Decision)
	require.EqualValues(t, 1, atomic.LoadInt32(&dialed), "prime dialed the PDP once")

	// Reset AFTER priming so the prime's miss does not pollute the assertion.
	metrics.PDPCacheTotal.Reset()

	// Already-canceled inbound ctx: must DENY before the fresh-hit serves the
	// cached allow.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got = d.AuthorizePrincipal(ctx, principal.ServiceAccount("sa"), "", reqCtx())
	require.Equal(t, policy.DecisionDeny, got.Decision)
	require.Contains(t, got.Reason, "request canceled")

	require.InDelta(t, 0.0, cacheTotal("hit", "allow"), 0.0001,
		"canceled request must not emit a cache hit (fresh-hit path bypassed)")
	require.EqualValues(t, 1, atomic.LoadInt32(&dialed),
		"canceled request must not dial the PDP")
}

// mutableCfg is a ConfigReader whose value can be swapped between requests to
// simulate an iam.pdp hot-reload.
type mutableCfg struct {
	mu  sync.Mutex
	val string
}

func (m *mutableCfg) set(v string) {
	m.mu.Lock()
	m.val = v
	m.mu.Unlock()
}

func (m *mutableCfg) GetString(key string) (string, bool) {
	if key != ConfigKey {
		return "", false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.val, true
}

// TestDecoratorTimeoutHotReload proves the per-request timeout tracks a config
// change against the SAME endpoint (same cached client). The PDP sleeps ~150ms.
// First request: 1s timeout (fail-open) -> allow. Second request: 20ms timeout
// against the same endpoint -> the client deadline fires -> fail-open allow but
// with the timeout error_type path, demonstrating the new deadline took effect
// without a client rebuild.
func TestDecoratorTimeoutHotReload(t *testing.T) {
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	cfg := &mutableCfg{}
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, cfg, nil, "admin")

	timeoutCfg := func(d string) string {
		b, _ := json.Marshal(map[string]any{
			"enabled": true, "endpoint": endpoint,
			"failure_policy": "open", "timeout": d,
		})
		return string(b)
	}

	// Generous timeout: PDP answers in time -> plain allow (no fail-open marker).
	cfg.set(timeoutCfg("1s"))
	got := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got.Decision)
	require.NotContains(t, got.Reason, "pdp_skipped_fail_open")

	// Same socket (client is cached), tighter timeout -> deadline fires ->
	// fail-open allow with the fail-open marker. If the client still owned the
	// construction-time 1s deadline this would not time out.
	cfg.set(timeoutCfg("20ms"))
	got = d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Equal(t, policy.DecisionAllow, got.Decision)
	require.Contains(t, got.Reason, "pdp_skipped_fail_open")
}

// TestDecoratorReleasesClientWhenDisabled proves a hot enable->disable frees the
// cached HTTP client. The first (enabled) request builds and caches the client;
// the second request, with {"enabled":false}, must release it so the idle
// keep-alive connection does not linger until process exit.
func TestDecoratorReleasesClientWhenDisabled(t *testing.T) {
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	cfg := &mutableCfg{}
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, cfg, nil, "admin")

	// Enabled request builds and caches the client.
	cfg.set(decoCfg(endpoint, "closed"))
	d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.NotNil(t, d.client)

	// Disabled request must release the cached client.
	cfg.set(`{"enabled":false}`)
	d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", policy.RequestContext{Action: "a", Resource: "r"})
	require.Nil(t, d.client)
	require.Empty(t, d.clientID)
	require.InDelta(t, 0.0, testutil.ToFloat64(metrics.PDPCacheEntries.WithLabelValues("admin")), 0.0001,
		"release() resets the entries gauge to 0 on disable")
}

func TestDecoratorAuthorizeMapsServiceAccount(t *testing.T) {
	var got Request
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(decoCfg(endpoint, "closed")), nil, "admin")
	d.Authorize(context.Background(), "sa-app", "", policy.RequestContext{Action: "grainfs:CredentialGet", Resource: "protocol-credential/nbd/v/d"})
	require.Equal(t, string(principal.KindServiceAccount), got.Principal.Kind)
	require.Equal(t, "sa-app", got.Principal.ID)
	require.Equal(t, "sa-app", got.Context["target_sa"])
}

// cacheCfg builds an iam.pdp config with caching/grace knobs.
func cacheCfg(endpoint, policyMode, ttlAllow, ttlDeny, grace string) string {
	cache := map[string]any{}
	if ttlAllow != "" {
		cache["ttl_allow"] = ttlAllow
	}
	if ttlDeny != "" {
		cache["ttl_deny"] = ttlDeny
	}
	if grace != "" {
		cache["grace_ttl"] = grace
	}
	b, _ := json.Marshal(map[string]any{
		"enabled": true, "endpoint": endpoint,
		"failure_policy": policyMode, "cache": cache,
	})
	return string(b)
}

func reqCtx() policy.RequestContext {
	return policy.RequestContext{Action: "a", Resource: "r"}
}

func cacheTotal(result, decision string) float64 {
	return testutil.ToFloat64(metrics.PDPCacheTotal.WithLabelValues("admin", result, decision))
}

func TestDecoratorCacheMissThenHit(t *testing.T) {
	metrics.PDPCacheTotal.Reset()
	var dialed int32
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dialed, 1)
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(cacheCfg(endpoint, "closed", "1m", "", "")), nil, "admin")

	// First request: cache MISS (PDP consulted, returns allow).
	got1 := d.Authorize(context.Background(), "sa", "", reqCtx())
	require.Equal(t, policy.DecisionAllow, got1.Decision)
	require.InDelta(t, 1.0, cacheTotal("miss", ""), 0.0001, "miss +1 on the consulted request")
	require.InDelta(t, 0.0, cacheTotal("hit", "allow"), 0.0001, "no hit yet")
	require.InDelta(t, 0.0, cacheTotal("grace", "allow"), 0.0001, "no grace on a normal miss")

	// Second request: fresh cache HIT (miss must NOT advance).
	got2 := d.Authorize(context.Background(), "sa", "", reqCtx())
	require.Equal(t, policy.DecisionAllow, got2.Decision)

	require.EqualValues(t, 1, atomic.LoadInt32(&dialed), "PDP dialed exactly once; 2nd served from cache")
	require.InDelta(t, 1.0, cacheTotal("hit", "allow"), 0.0001, "hit +1 on the cached request")
	require.InDelta(t, 1.0, cacheTotal("miss", ""), 0.0001, "miss unchanged by the hit (one consult)")
	require.InDelta(t, 0.0, cacheTotal("grace", "allow"), 0.0001)
}

func TestDecoratorCacheDeny(t *testing.T) {
	metrics.PDPCacheTotal.Reset()
	var dialed int32
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dialed, 1)
		_, _ = w.Write([]byte(`{"decision":"deny","reason":"blocked"}`))
	})
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(cacheCfg(endpoint, "closed", "", "1m", "")), nil, "admin")

	got1 := d.Authorize(context.Background(), "sa", "", reqCtx())
	require.Equal(t, policy.DecisionDeny, got1.Decision)
	require.Equal(t, genericDenyMsg, got1.Reason)
	got2 := d.Authorize(context.Background(), "sa", "", reqCtx())
	require.Equal(t, policy.DecisionDeny, got2.Decision)
	require.Equal(t, genericDenyMsg, got2.Reason)

	require.EqualValues(t, 1, atomic.LoadInt32(&dialed), "deny cached; 2nd served from cache")
	require.InDelta(t, 1.0, cacheTotal("hit", "deny"), 0.0001)
}

func TestDecoratorCacheFailureNotCached(t *testing.T) {
	var dialed int32
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&dialed, 1) == 1 {
			w.WriteHeader(500)
			return
		}
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(cacheCfg(endpoint, "open", "1m", "", "")), nil, "admin")

	got1 := d.Authorize(context.Background(), "sa", "", reqCtx())
	require.Equal(t, policy.DecisionAllow, got1.Decision) // fail-open allow, NOT cached
	got2 := d.Authorize(context.Background(), "sa", "", reqCtx())
	require.Equal(t, policy.DecisionAllow, got2.Decision)

	require.EqualValues(t, 2, atomic.LoadInt32(&dialed), "failure not cached; both requests dial the PDP")
}

func TestDecoratorCacheExpiry(t *testing.T) {
	var dialed int32
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dialed, 1)
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(cacheCfg(endpoint, "closed", "1m", "", "")), nil, "admin")
	base := time.Now()
	d.now = func() time.Time { return base }

	d.Authorize(context.Background(), "sa", "", reqCtx())
	require.EqualValues(t, 1, atomic.LoadInt32(&dialed))

	// Advance past ttl_allow (1m): entry expires, no grace -> re-consult.
	d.now = func() time.Time { return base.Add(2 * time.Minute) }
	d.Authorize(context.Background(), "sa", "", reqCtx())
	require.EqualValues(t, 2, atomic.LoadInt32(&dialed), "expired entry re-consults the PDP")
}

func TestDecoratorGraceServesAllow(t *testing.T) {
	metrics.PDPCacheTotal.Reset()
	var up atomic.Bool
	up.Store(true)
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			w.WriteHeader(500)
			return
		}
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	logBuf := captureLog(t)
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(cacheCfg(endpoint, "closed", "1m", "", "1h")), nil, "admin")
	base := time.Now()
	d.now = func() time.Time { return base }

	// Prime the cache with a fresh allow.
	got := d.Authorize(context.Background(), "sa", "", reqCtx())
	require.Equal(t, policy.DecisionAllow, got.Decision)

	// Advance past ttl_allow but within grace; PDP now down. Snapshot the miss
	// counter so we can prove the grace serve emits ONLY "grace", never "miss".
	missBefore := cacheTotal("miss", "")
	up.Store(false)
	d.now = func() time.Time { return base.Add(2 * time.Minute) }
	got = d.Authorize(context.Background(), "sa", "", reqCtx())
	require.Equal(t, policy.DecisionAllow, got.Decision, "grace-served stale allow despite PDP down + fail-closed")

	require.InDelta(t, 1.0, cacheTotal("grace", "allow"), 0.0001)
	require.InDelta(t, missBefore, cacheTotal("miss", ""), 0.0001,
		"a grace serve must not increment miss (no double-count with grace)")
	require.Contains(t, logBuf.String(), layerGraceServed)
}

func TestDecoratorGraceExpiredFallsToFailureClosed(t *testing.T) {
	var up atomic.Bool
	up.Store(true)
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			w.WriteHeader(500)
			return
		}
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(cacheCfg(endpoint, "closed", "1m", "", "5m")), nil, "admin")
	base := time.Now()
	d.now = func() time.Time { return base }
	require.Equal(t, policy.DecisionAllow, d.Authorize(context.Background(), "sa", "", reqCtx()).Decision)

	// Past grace_ttl (5m) with PDP down -> failure_policy closed -> deny.
	up.Store(false)
	d.now = func() time.Time { return base.Add(10 * time.Minute) }
	got := d.Authorize(context.Background(), "sa", "", reqCtx())
	require.Equal(t, policy.DecisionDeny, got.Decision)
	require.Contains(t, got.Reason, layerFailClosed)
}

func TestDecoratorGraceServesDeny(t *testing.T) {
	metrics.PDPCacheTotal.Reset()
	var up atomic.Bool
	up.Store(true)
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			w.WriteHeader(500)
			return
		}
		_, _ = w.Write([]byte(`{"decision":"deny","reason":"blocked"}`))
	})
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(cacheCfg(endpoint, "open", "", "1m", "1h")), nil, "admin")
	base := time.Now()
	d.now = func() time.Time { return base }

	require.Equal(t, policy.DecisionDeny, d.Authorize(context.Background(), "sa", "", reqCtx()).Decision)

	up.Store(false)
	d.now = func() time.Time { return base.Add(2 * time.Minute) }
	got := d.Authorize(context.Background(), "sa", "", reqCtx())
	require.Equal(t, policy.DecisionDeny, got.Decision, "grace serves cached deny even with failure_policy open")
	require.Equal(t, genericDenyMsg, got.Reason)
	require.InDelta(t, 1.0, cacheTotal("grace", "deny"), 0.0001)
}

func TestDecoratorCacheHitSuppressesAudit(t *testing.T) {
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(cacheCfg(endpoint, "closed", "1m", "", "")), nil, "admin")

	// Prime (this emits an audit line).
	d.Authorize(context.Background(), "sa", "", reqCtx())

	// Capture only the HIT request's logs.
	logBuf := captureLog(t)
	d.Authorize(context.Background(), "sa", "", reqCtx())
	require.NotContains(t, logBuf.String(), "iam.pdp.decision", "cache hit must not emit an iam.pdp audit line")
}

func TestDecoratorConfigChangeClearsCache(t *testing.T) {
	var dialedA, dialedB int32
	sockA := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dialedA, 1)
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	sockB := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dialedB, 1)
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	cfg := &mutableCfg{}
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, cfg, nil, "admin")

	cfg.set(cacheCfg(sockA, "closed", "1m", "", ""))
	d.Authorize(context.Background(), "sa", "", reqCtx())
	require.EqualValues(t, 1, atomic.LoadInt32(&dialedA))

	// Endpoint change -> cache cleared -> prior entry not served, new endpoint consulted.
	cfg.set(cacheCfg(sockB, "closed", "1m", "", ""))
	d.Authorize(context.Background(), "sa", "", reqCtx())
	require.EqualValues(t, 1, atomic.LoadInt32(&dialedB), "config change cleared the cache; new endpoint consulted")
}

func TestDecoratorReEnableSameConfigRebuildsCache(t *testing.T) {
	var dialed int32
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dialed, 1)
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	cfg := &mutableCfg{}
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, cfg, nil, "admin")
	enabled := cacheCfg(endpoint, "closed", "1m", "", "")

	cfg.set(enabled)
	d.Authorize(context.Background(), "sa", "", reqCtx())
	require.EqualValues(t, 1, atomic.LoadInt32(&dialed))

	cfg.set(`{"enabled":false}`)
	d.Authorize(context.Background(), "sa", "", reqCtx())

	// Re-enable with the IDENTICAL config: cacheGen must have been reset by
	// release() so the cache is rebuilt and caching works again.
	cfg.set(enabled)
	d.Authorize(context.Background(), "sa", "", reqCtx()) // miss -> dial #2
	require.EqualValues(t, 2, atomic.LoadInt32(&dialed))
	d.Authorize(context.Background(), "sa", "", reqCtx()) // hit -> no dial
	require.EqualValues(t, 2, atomic.LoadInt32(&dialed), "re-enabled cache serves the hit")
}

// TestDecoratorReachableDenyOverridesStaleAllow pins the deny-precedence
// security semantic: a reachable PDP returning an AUTHORITATIVE deny must
// override a STALE-but-within-grace cached allow. Grace exists only to ride out
// PDP *unavailability* (errType != ""); it must NOT resurrect a stale allow when
// the PDP is actually reachable and says deny.
func TestDecoratorReachableDenyOverridesStaleAllow(t *testing.T) {
	var dialed int32
	var denyMode atomic.Bool
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dialed, 1)
		if denyMode.Load() {
			_, _ = w.Write([]byte(`{"decision":"deny","reason":"blocked"}`))
			return
		}
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	// ttl_allow short (1m), grace large (1h, the max) -> the entry goes stale but
	// stays within grace. failure_policy irrelevant here (no failure occurs).
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(cacheCfg(endpoint, "closed", "1m", "", "1h")), nil, "admin")
	base := time.Now()
	d.now = func() time.Time { return base }

	// Prime: PDP allow -> cached allow.
	got := d.Authorize(context.Background(), "sa", "", reqCtx())
	require.Equal(t, policy.DecisionAllow, got.Decision)
	require.EqualValues(t, 1, atomic.LoadInt32(&dialed))

	// Advance past ttl_allow but within grace_ttl: the cached entry is now STALE.
	// Flip the PDP to a reachable authoritative deny (NOT a 500/failure).
	denyMode.Store(true)
	d.now = func() time.Time { return base.Add(2 * time.Minute) }

	got = d.Authorize(context.Background(), "sa", "", reqCtx())
	require.Equal(t, policy.DecisionDeny, got.Decision,
		"reachable PDP deny must override the stale cached allow; grace must not resurrect it")
	require.Equal(t, genericDenyMsg, got.Reason)
	// A reachable deny means the PDP was actually dialed on the stale request
	// (it was not served from cache as a fresh hit).
	require.EqualValues(t, 2, atomic.LoadInt32(&dialed),
		"stale entry re-consulted the PDP; the deny came from the live call, not the cache")
}

// TestDecoratorConcurrentAuthorizeRace drives many concurrent Authorize calls on
// a SHARED Decorator with caching enabled (a mix of shared and distinct
// resources so some hit and some miss), while another goroutine flips the config
// between two valid cache settings on the SAME endpoint to exercise the
// cache-rebuild path under contention. It exists to run under `-race`: it asserts
// no panic and that every result is a valid decision. The clock is fixed and
// never mutated concurrently (d.now is not lock-guarded).
func TestDecoratorConcurrentAuthorizeRace(t *testing.T) {
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	cfg := &mutableCfg{}
	// Same endpoint, vary only a cache knob (ttl_allow) so cacheGen changes and the
	// cache is rebuilt under contention WITHOUT closing the client mid-flight.
	cfgA := cacheCfg(endpoint, "closed", "1m", "", "1h")
	cfgB := cacheCfg(endpoint, "closed", "2m", "", "1h")
	cfg.set(cfgA)
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, cfg, nil, "admin")
	base := time.Now()
	d.now = func() time.Time { return base } // fixed clock, never mutated below

	const n = 50

	// Config flipper: toggles between two valid configs to contend with refresh.
	// Tracked separately from the Authorize goroutines so it can be stopped only
	// after they finish.
	stop := make(chan struct{})
	flipDone := make(chan struct{})
	go func() {
		defer close(flipDone)
		for {
			select {
			case <-stop:
				return
			default:
				cfg.set(cfgB)
				cfg.set(cfgA)
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			rc := reqCtx()
			if i%2 == 0 {
				// Distinct resource -> cache miss for half the callers.
				rc.Resource = "r-" + string(rune('a'+i%26))
			}
			got := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "", rc)
			require.Contains(t, []policy.Decision{policy.DecisionAllow, policy.DecisionDeny}, got.Decision)
		}(i)
	}

	wg.Wait()   // all Authorize callers done
	close(stop) // stop the flipper
	<-flipDone  // and wait for it to exit before the test returns
}

// captureLog redirects the zerolog global logger to a mutex-guarded buffer for
// the duration of the test and restores it afterward. The decorator audits on
// its own goroutine path while the test reads concurrently, so the buffer must
// be race-safe.
func captureLog(t *testing.T) *safeBuffer {
	t.Helper()
	buf := &safeBuffer{}
	prev := log.Logger
	log.Logger = zerolog.New(buf)
	t.Cleanup(func() { log.Logger = prev })
	return buf
}

// safeBuffer is a bytes.Buffer guarded by a mutex so the httptest handler
// goroutine and the test goroutine can both touch the log without racing.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestDecorator_SSRFBlocked_HardDeniesUnderFailOpen proves that a dial rejected by
// the SSRF egress filter HARD-DENIES even with failure_policy=open. The endpoint
// is "https://localhost:PORT": it parses OK (localhost is a plausible DNS name),
// but at dial it resolves to 127.0.0.1, which the SSRF filter forbids for https.
// That block must NOT be treated as a routine PDP failure (which fail-open would
// turn into an allow); it must deny.
func TestDecorator_SSRFBlocked_HardDeniesUnderFailOpen(t *testing.T) {
	// A real TLS listener gives us a live loopback port to aim at; the SSRF filter
	// rejects the dial before the TLS handshake, so the cert is never exercised.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	}))
	defer srv.Close()
	// srv.URL is https://127.0.0.1:PORT (or https://[::1]:PORT on a v4-less host);
	// swap the literal loopback IP for the "localhost" DNS name so ParseConfig
	// accepts it (literal loopback IPs are rejected at parse time) while the dial
	// still resolves to loopback (blocked for https by the egress filter).
	endpoint := strings.NewReplacer("127.0.0.1", "localhost", "[::1]", "localhost").Replace(srv.URL)
	// Guard: if the substitution silently no-op'd (server bound an unexpected
	// address) the test would exercise the pass-through path, not the SSRF branch.
	require.Contains(t, endpoint, "localhost", "endpoint must use the localhost DNS name to reach the SSRF dial filter")
	cfgRaw, _ := json.Marshal(map[string]any{
		"enabled": true, "endpoint": endpoint, "failure_policy": "open",
	})

	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(string(cfgRaw)), nil, "admin")
	res := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa"), "b",
		policy.RequestContext{Action: "s3:GetObject", Resource: "b/o"})
	if res.Decision != policy.DecisionDeny {
		t.Fatalf("SSRF-blocked must HARD-DENY even with failure_policy=open, got %v", res.Decision)
	}
}

// rotatingTokens is a fake TokenSource whose generation can be flipped between
// calls to simulate a bearer-token rotation.
type rotatingTokens struct {
	mu  sync.Mutex
	tok string
	gen string
}

func (r *rotatingTokens) set(tok, gen string) {
	r.mu.Lock()
	r.tok, r.gen = tok, gen
	r.mu.Unlock()
}

func (r *rotatingTokens) CurrentToken() (string, string, TokenStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gen == "" {
		return "", "", TokenAbsent
	}
	return r.tok, r.gen, TokenReady
}

// brokenTokens is a TokenSource that reports a token IS configured but unusable.
type brokenTokens struct{}

func (brokenTokens) CurrentToken() (string, string, TokenStatus) { return "", "", TokenError }

// TestDecoratorTokenErrorHardDeniesUnderFailOpen proves a configured-but-unusable
// bearer token hard-denies even with failure_policy=open — a corrupt/misconfigured
// token must never silently degrade to a token-less PDP call that fail-open could
// turn into an allow. The PDP server here would ALLOW if reached; it must not be.
func TestDecoratorTokenErrorHardDeniesUnderFailOpen(t *testing.T) {
	var reached int32
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reached, 1)
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(decoCfg(endpoint, "open")), brokenTokens{}, "admin")
	res := d.AuthorizePrincipal(context.Background(), principal.ServiceAccount("u1"), "",
		policy.RequestContext{Action: "a", Resource: "r"})
	if res.Decision != policy.DecisionDeny {
		t.Fatalf("configured-but-unusable token must HARD-DENY even with failure_policy=open, got %v", res.Decision)
	}
	if atomic.LoadInt32(&reached) != 0 {
		t.Fatalf("PDP must NOT be called when the token is unusable (called %d times)", reached)
	}
}

// TestDecoratorTokenRotationRebuildsClientAndCache proves a bearer-token rotation
// (a new generation from the TokenSource) feeds both clientIdentity and configGen,
// so the cached client is rebuilt AND the decision cache is dropped — a prior
// cached allow under the old token must not be served after rotation.
func TestDecoratorTokenRotationRebuildsClientAndCache(t *testing.T) {
	var dialed int32
	endpoint := decoHTTPPDP(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dialed, 1)
		_, _ = w.Write([]byte(`{"decision":"allow"}`))
	})
	tokens := &rotatingTokens{}
	tokens.set("tok-1", "gen-1")
	d := NewDecorator(&spyInner{decision: policy.DecisionAllow}, staticCfg(cacheCfg(endpoint, "closed", "1m", "", "")), tokens, "admin")

	// First request: cache miss -> dial #1, cached under gen-1.
	require.Equal(t, policy.DecisionAllow, d.Authorize(context.Background(), "sa", "", reqCtx()).Decision)
	require.EqualValues(t, 1, atomic.LoadInt32(&dialed))

	// Same gen: served from cache, no new dial.
	require.Equal(t, policy.DecisionAllow, d.Authorize(context.Background(), "sa", "", reqCtx()).Decision)
	require.EqualValues(t, 1, atomic.LoadInt32(&dialed), "same token gen serves the cached allow")

	// Rotate the token: configGen changes -> cache dropped, client rebuilt ->
	// the next request must re-consult the PDP (cannot serve the stale entry).
	tokens.set("tok-2", "gen-2")
	require.Equal(t, policy.DecisionAllow, d.Authorize(context.Background(), "sa", "", reqCtx()).Decision)
	require.EqualValues(t, 2, atomic.LoadInt32(&dialed), "token rotation cleared the cache; PDP re-consulted")
}
