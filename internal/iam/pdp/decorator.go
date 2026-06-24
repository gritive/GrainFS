package pdp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"

	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/gritive/GrainFS/internal/metrics"
)

// ConfigKey is the config store key holding the iam.pdp JSON document.
const ConfigKey = "iam.pdp"

// Layer markers recorded in the audit log and surfaced in EvalResult.Reason.
const (
	layerDeny        = "pdp_deny"
	layerAllow       = "pdp_allow"
	layerFailOpen    = "pdp_skipped_fail_open"
	layerFailClosed  = "pdp_unavailable"
	layerGraceServed = "pdp_grace_served"
)

// genericDenyMsg is the user-facing reason for a PDP deny. The PDP-supplied raw
// reason is recorded ONLY in the audit log, never returned to the caller.
const genericDenyMsg = "pdp_deny: denied by external policy"

const schemaVersion = 1

// scopeDataPlane is the scope label for the S3 object data-plane decorator.
// Only this scope infers Request.Protocol from the action prefix; control-plane
// scopes (admin | protocol_credential) keep the literal "admin".
const scopeDataPlane = "data_plane"

// ConfigReader is the minimal view of the config store the decorator needs.
type ConfigReader interface {
	GetString(key string) (string, bool)
}

// innerAuthorizer is the GrainFS IAM authorizer the decorator wraps. It is
// declared here (and not imported from the server wiring) so that the concrete
// *s3auth.Authorizer satisfies it structurally.
type innerAuthorizer interface {
	Authorize(ctx context.Context, saID, bucket string, ctxReq policy.RequestContext) policy.EvalResult
	AuthorizePrincipal(ctx context.Context, p principal.Principal, bucket string, ctxReq policy.RequestContext) policy.EvalResult
}

// parsedConfig holds the last successfully parsed iam.pdp config together with
// its raw source string. loadConfig stores and reads this to skip JSON unmarshal
// when the raw config string has not changed between requests.
type parsedConfig struct {
	raw string
	cfg Config
}

// Decorator chains an external PDP AFTER the GrainFS IAM authorizer using a
// deny-override rule: a request is allowed only if BOTH GrainFS and the PDP
// allow. It reads the iam.pdp config per request, so an operator can flip
// enabled/failure_policy/endpoint at runtime without a restart. When disabled
// (or the config is missing/invalid) it is a pure pass-through.
type Decorator struct {
	inner  innerAuthorizer
	cfg    ConfigReader
	tokens TokenSource // may be nil

	// now is the clock used for cache TTL/grace decisions. Defaults to time.Now;
	// tests override it for deterministic expiry.
	now func() time.Time

	// scope is the control-plane scope label (admin | protocol_credential).
	scope string

	// Pre-bound metric handles: scope is curried once in NewDecorator so emit
	// sites never call metrics.X.WithLabelValues(d.scope, ...) per request.
	mGauge    prometheus.Gauge
	mDuration prometheus.Observer
	mRequests *prometheus.CounterVec
	mCache    *prometheus.CounterVec

	// parsed caches the last successfully parsed iam.pdp config. loadConfig reads
	// and updates this field to avoid JSON unmarshal on every request.
	parsed atomic.Pointer[parsedConfig]

	// released: lock-free fast path for the disabled hot path — once dropped,
	// disabled requests Load()==true and skip the exclusive d.mu lock. refresh()
	// clears it on (re)build. Disabled path still takes the config RLock via GetString
	// (Restore-coherence; not eliminable — this is NOT a bare "lock-free" path).
	released atomic.Bool

	// sf collapses concurrent identical cache misses into one PDP round trip (D2).
	// Keyed on cacheKey + configGen so a config flip cannot collapse across
	// generations. The detached flight in consult() decouples the shared round
	// trip from any single waiter's ctx; per-waiter cancel is the DoChan select.
	sf singleflight.Group

	mu       sync.Mutex
	client   *Client
	clientID string
	cache    *decisionCache
	cacheGen string
}

// NewDecorator wraps inner with a per-request PDP chain driven by cfg. tokens
// supplies the bearer token (and its rotation generation) for an https remote
// PDP; it may be nil when no token is configured. scope is the control-plane
// scope label (admin | protocol_credential) used for all metric emissions.
func NewDecorator(inner innerAuthorizer, cfg ConfigReader, tokens TokenSource, scope string) *Decorator {
	return &Decorator{
		inner:     inner,
		cfg:       cfg,
		tokens:    tokens,
		now:       time.Now,
		scope:     scope,
		mGauge:    metrics.PDPCacheEntries.WithLabelValues(scope),
		mDuration: metrics.PDPRequestDuration.WithLabelValues(scope),
		mRequests: metrics.PDPRequestsTotal.MustCurryWith(prometheus.Labels{"scope": scope}),
		mCache:    metrics.PDPCacheTotal.MustCurryWith(prometheus.Labels{"scope": scope}),
	}
}

// Authorize chains the PDP after the GrainFS service-account authorizer. The
// actor presented to the PDP is the named service account.
func (d *Decorator) Authorize(ctx context.Context, saID, bucket string, ctxReq policy.RequestContext) policy.EvalResult {
	inner := d.inner.Authorize(ctx, saID, bucket, ctxReq)
	// Disabled-path short-circuit BEFORE principal.ServiceAccount(saID): the actor
	// escapes to the heap inside chain (toWire→req→client.Authorize), so passing it
	// here would heap-allocate on every call, including the 0-alloc disabled hot path.
	// chain keeps its own gate for the AuthorizePrincipal entry.
	if cfg, ok, err := d.loadConfig(); !ok || err != nil || d.disabledForScope(cfg) {
		d.release()
		return inner
	}
	return d.chain(ctx, principal.ServiceAccount(saID), saID, inner, ctxReq)
}

// disabledForScope reports whether the PDP must be skipped for this decorator's
// scope: control-plane scopes gate on cfg.Enabled alone; the data_plane scope
// additionally requires cfg.DataPlane.Enabled.
func (d *Decorator) disabledForScope(cfg Config) bool {
	return !cfg.Enabled || (d.scope == scopeDataPlane && !cfg.DataPlane.Enabled)
}

// AuthorizePrincipal chains the PDP after the GrainFS principal authorizer. The
// actor presented to the PDP is the resolved principal.
func (d *Decorator) AuthorizePrincipal(ctx context.Context, p principal.Principal, bucket string, ctxReq policy.RequestContext) policy.EvalResult {
	inner := d.inner.AuthorizePrincipal(ctx, p, bucket, ctxReq)
	targetSA := ""
	if p.Kind == principal.KindServiceAccount {
		targetSA = p.ID
	}
	return d.chain(ctx, p, targetSA, inner, ctxReq)
}

// loadConfig returns the parsed iam.pdp config for the current raw value,
// re-parsing only when the raw string changed. Restore-coherent: raw comes from
// GetString (the live config store), so a snapshot install that swaps the stored
// value is picked up next request. ok=false ⇒ key unset (unconfigured pass-through).
func (d *Decorator) loadConfig() (Config, bool, error) {
	raw, ok := d.cfg.GetString(ConfigKey)
	if !ok {
		return Config{}, false, nil
	}
	if snap := d.parsed.Load(); snap != nil && snap.raw == raw {
		return snap.cfg, true, nil
	}
	cfg, err := ParseConfig([]byte(raw))
	if err != nil {
		return Config{}, true, err
	}
	d.parsed.Store(&parsedConfig{raw: raw, cfg: cfg})
	return cfg, true, nil
}

// chain applies the PDP consultation given the GrainFS (inner) result.
func (d *Decorator) chain(ctx context.Context, actor principal.Principal, targetSA string, inner policy.EvalResult, ctxReq policy.RequestContext) policy.EvalResult {
	cfg, ok, err := d.loadConfig()
	if !ok {
		d.release()  // unconfigured: free any client/cache left over from when it was enabled
		return inner // unconfigured: pure pass-through
	}
	if err != nil {
		log.Warn().Err(err).Str("event", "iam.pdp").Msg("iam.pdp: invalid config, treating as disabled")
		d.release()
		return inner
	}
	if d.disabledForScope(cfg) {
		d.release()
		return inner
	}

	// Deny-override: only consult the PDP when GrainFS already allowed.
	if inner.Decision != policy.DecisionAllow {
		return inner
	}

	protocol := "admin"
	am := string(actor.Kind)
	ts := targetSA
	if d.scope == scopeDataPlane {
		protocol = protocolFromAction(ctxReq.Action)
		am = authMethod(actor)
		ts = "" // data-plane op acts on an object, not an SA (spec D5)
	}
	req := Request{
		SchemaVersion: schemaVersion,
		RequestID:     newRequestID(),
		Principal:     toWire(actor),
		Action:        ctxReq.Action,
		Resource:      ctxReq.Resource,
		Protocol:      protocol,
		Context: map[string]string{
			"auth_method": am,
			"target_sa":   ts,
			"route":       ctxReq.Action,
		},
	}

	token, tokenGen := "", ""
	tokenBroken := false
	if d.tokens != nil {
		switch tk, g, st := d.tokens.CurrentToken(); st {
		case TokenReady:
			token, tokenGen = tk, g
		case TokenError:
			tokenBroken = true
		}
	}
	fp := string(cfg.FailurePolicy)
	client, cache := d.refresh(cfg, token, tokenGen)

	// Inbound-cancel takes precedence over ANY cached decision: an already-canceled
	// request must DENY "request canceled" before the cache lookup, so a fresh cached
	// allow can never resurrect an abandoned request (and emits no cache_total). This
	// covers the fresh-hit, grace, and miss paths uniformly since it runs before the
	// lookup. The post-PDP ctx.Err() check below catches cancellation DURING the call.
	if ctx.Err() != nil {
		errType := ErrTypeTransport
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			errType = ErrTypeTimeout
		}
		d.mRequests.WithLabelValues("error", errType, fp).Inc()
		d.audit(req, actor, ctxReq, "deny", errType, "request canceled")
		return policy.EvalResult{Decision: policy.DecisionDeny, Reason: "request canceled"}
	}

	// A configured-but-unusable bearer token (bad envelope / unseal failure /
	// encryptor not ready) HARD-DENIES — never silently call the PDP without
	// Authorization, or a fail-open policy could turn a corrupt/misconfigured token
	// into an allow. Exempt from failure_policy AND grace, like an SSRF block, and
	// checked before the cache so a stale allow cached under a once-good token is
	// not served while the token is broken.
	if tokenBroken {
		d.mRequests.WithLabelValues("error", ErrTypeTokenUnavailable, fp).Inc()
		log.Warn().Str("event", "iam.pdp").Str("error_type", ErrTypeTokenUnavailable).
			Msg("iam.pdp: configured bearer token is unusable (parse/unseal/encryptor) — hard deny")
		d.audit(req, actor, ctxReq, "deny", ErrTypeTokenUnavailable, "token_unavailable: configured token unusable")
		return policy.EvalResult{Decision: policy.DecisionDeny, Reason: layerFailClosed}
	}

	// Cache lookup slots between req-build and the PDP call. A fresh hit returns
	// without consulting the PDP and without an audit line; a stale entry is held
	// for a possible grace-serve in the failure branch below.
	var (
		entry cacheEntry
		state cacheState
	)
	// cacheKey is computed unconditionally (deterministic in req) so the
	// singleflight collapse key exists even when the cache is disabled; the
	// cache-nil branch just skips Lookup/Put.
	key := cacheKey(req)
	if cache != nil {
		// PDPCacheEntries is best-effort/approximate: it is updated on put, release,
		// and refresh, NOT on the lazy eviction that Lookup may perform here (Len()
		// locks every shard, so we do not call it per lookup).
		entry, state = cache.Lookup(key, cfg.Cache.GraceTTL, d.now())
		if state == cacheFresh {
			d.mCache.WithLabelValues("hit", entry.decision).Inc()
			if entry.decision == DecisionDeny {
				return policy.EvalResult{Decision: policy.DecisionDeny, Reason: genericDenyMsg}
			}
			return inner
		}
	}

	// sfKey includes configGen so a config flip cannot collapse across generations
	// (cacheKey omits failure_policy/endpoint/token-gen/timeout). The cache itself
	// still keys on plain cacheKey; only the collapse key is generation-scoped.
	// Grace coherence: leader computes grace from its entry/state; collapsed waiters
	// share this sfKey so they looked up the same key — at the grace boundary their
	// captured state may differ slightly, but the divergence is sub-ms and always
	// security-equivalent-or-more-restrictive. Cancel/audit delta: the detached flight
	// completes even if the sole waiter cancels, so a canceled request adds one extra
	// completion audit/mRequests row on top of its cancel row (observability-only).
	sfKey := key + "\x00" + configGen(cfg, tokenGen)
	ch := d.sf.DoChan(sfKey, func() (any, error) {
		return d.consult(req, cfg, fp, key, cache, entry, state, actor, ctxReq, client), nil
	})
	// canceledDeny restores master's invariant that a canceled request never adopts
	// an allow (incl. fail-open). The ctx.Done() arm catches a cancel that wins the
	// select race; the post-<-ch check is defense-in-depth for the case where the
	// flight result won the select but the inbound ctx is ALREADY canceled (a select
	// between two ready channels picks randomly). The invariant is tested at its two
	// black-box-reachable points — TestDecoratorCanceledCtxDeniesEvenFailOpen
	// (pre-call arm) and TestDecoratorSingleflightPerWaiterCancel (ctx.Done arm); the
	// post-<-ch arm is not black-box reachable today (the pre-call check absorbs early
	// cancels and a blocked flight lands on ctx.Done), so it is correct-by-inspection
	// defense-in-depth, NOT test-covered. S3 passes context.Background() (never
	// cancels), but the deferred "thread ctx through IAMChecker" follow-up would make
	// this arm reachable on S3 — close it now rather than ship a latent fail-open.
	canceledDeny := func() policy.EvalResult {
		errType := ErrTypeTransport
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			errType = ErrTypeTimeout
		}
		d.mRequests.WithLabelValues("error", errType, fp).Inc()
		d.audit(req, actor, ctxReq, "deny", errType, "request canceled")
		return policy.EvalResult{Decision: policy.DecisionDeny, Reason: "request canceled"}
	}
	var r consultResult
	select {
	case <-ctx.Done():
		return canceledDeny()
	case res := <-ch:
		if ctx.Err() != nil {
			return canceledDeny()
		}
		r = res.Val.(consultResult)
	}
	if r.useInner {
		out := inner
		if r.reason == layerFailOpen {
			out.Reason = annotate(out.Reason, layerFailOpen)
		}
		return out
	}
	return policy.EvalResult{Decision: r.decision, Reason: r.reason}
}

// consultResult is the outcome of one PDP consult (miss path). useInner=true means
// "return the GrainFS inner result" (allow / fail-open / grace-allow); otherwise
// Decision/Reason carry the PDP/grace/failure/cancel outcome.
type consultResult struct {
	useInner bool
	decision policy.Decision
	reason   string
}

// consult performs the PDP round trip + grace + failure policy + caching. It runs
// inside the singleflight flight, shared by collapsed waiters; callCtx is DETACHED
// from any single waiter's inbound ctx so one waiter's cancel cannot poison the
// shared round trip (per-waiter cancel is the DoChan select in chain). The decorator
// still owns the per-request deadline (cfg.Timeout) so a runtime iam.pdp.timeout
// change takes effect without rebuilding the cached client.
func (d *Decorator) consult(req Request, cfg Config, fp, key string,
	cache *decisionCache, entry cacheEntry, state cacheState,
	actor principal.Principal, ctxReq policy.RequestContext, client *Client) consultResult {

	// DETACHED: a single waiter's cancel must not poison the shared flight; per-waiter
	// cancel is the DoChan select in chain(). S3 passes context.Background() anyway.
	callCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	start := time.Now()
	_, errType, err := client.Authorize(callCtx, req)
	d.mDuration.Observe(time.Since(start).Seconds())
	// (4a post-call inbound ctx.Err() block REMOVED — the select owns per-waiter cancel.)

	var de *DenyError
	if errors.As(err, &de) {
		d.mRequests.WithLabelValues("deny", "", fp).Inc()
		if cache != nil {
			d.mCache.WithLabelValues("miss", "").Inc()
			if cfg.Cache.TTLDeny > 0 {
				cache.Put(key, DecisionDeny, de.Reason, cfg.Cache.TTLDeny, d.now())
				d.mGauge.Set(float64(cache.Len()))
			}
		}
		d.audit(req, actor, ctxReq, "deny", "", layerDeny+": "+de.Reason)
		return consultResult{decision: policy.DecisionDeny, reason: genericDenyMsg}
	}

	if errType != "" {
		d.mRequests.WithLabelValues("error", errType, fp).Inc()
		// SSRF-blocked is a HARD DENY independent of failure_policy: a dial rejected
		// by the egress filter must never fall through to fail-open (it would let an
		// attacker reach a forbidden target by tripping the filter). It also bypasses
		// grace — a blocked dial is not "PDP unavailable", it is a refused egress.
		if errType == ErrTypeSSRF {
			log.Warn().Str("event", "iam.pdp").Str("error_type", errType).
				Msg("iam.pdp: dial blocked by SSRF egress filter — hard deny")
			d.audit(req, actor, ctxReq, "deny", errType, "ssrf_blocked: egress filter")
			return consultResult{decision: policy.DecisionDeny, reason: layerFailClosed}
		}
		// Grace first: a PDP failure with a stale-but-within-grace cached decision
		// serves that decision rather than applying the failure policy.
		if cache != nil && cfg.Cache.GraceTTL > 0 && state == cacheStale {
			d.mCache.WithLabelValues("grace", entry.decision).Inc()
			d.audit(req, actor, ctxReq, entry.decision, errType, layerGraceServed)
			if entry.decision == DecisionDeny {
				return consultResult{decision: policy.DecisionDeny, reason: genericDenyMsg}
			}
			return consultResult{useInner: true}
		}
		// No grace served: this consult falls through to the failure policy, so
		// count exactly one miss (covers both fail-open and fail-closed).
		if cache != nil {
			d.mCache.WithLabelValues("miss", "").Inc()
		}
		if cfg.FailurePolicy == FailureOpen {
			d.audit(req, actor, ctxReq, "allow", errType, layerFailOpen)
			return consultResult{useInner: true, reason: layerFailOpen}
		}
		d.audit(req, actor, ctxReq, "deny", errType, layerFailClosed)
		return consultResult{decision: policy.DecisionDeny, reason: layerFailClosed}
	}

	d.mRequests.WithLabelValues("allow", "", fp).Inc()
	if cache != nil {
		d.mCache.WithLabelValues("miss", "").Inc()
		if cfg.Cache.TTLAllow > 0 {
			cache.Put(key, DecisionAllow, "", cfg.Cache.TTLAllow, d.now())
			d.mGauge.Set(float64(cache.Len()))
		}
	}
	d.audit(req, actor, ctxReq, "allow", "", layerAllow)
	return consultResult{useInner: true}
}

// refresh reconciles the cached client and decision cache against cfg under a
// single lock and returns a snapshot of both. The client is rebuilt ONLY when
// its identity changes (endpoint/scheme/TLS/SSRF or the bearer-token generation —
// FailurePolicy/timeout are read per request from cfg, not the cached client, so
// those flips take effect without a rebuild). The cache is rebuilt/dropped only
// when configGen changes — so an endpoint, failure_policy, cache-parameter, or
// token-generation change clears prior cached decisions (R-A). The returned
// *decisionCache is internally locked, safe to use after unlock. token is the
// snapshot taken once by the caller so client+cache reconcile against the same
// value; tokenGen feeds both client identity and configGen.
func (d *Decorator) refresh(cfg Config, token, tokenGen string) (*Client, *decisionCache) {
	d.mu.Lock()
	defer d.mu.Unlock()

	id := clientIdentity(cfg, tokenGen)
	if d.client == nil || d.clientID != id {
		if d.client != nil {
			// Identity changed: release the old client's idle keep-alive
			// connections so hot-reloading the endpoint/token doesn't leak FDs.
			d.client.Close()
		}
		d.client = NewClient(cfg, token)
		d.clientID = id
	}

	if gen := configGen(cfg, tokenGen); gen != d.cacheGen {
		if cfg.Cache.Active {
			d.cache = newDecisionCache(cfg.Cache.MaxEntries)
		} else {
			d.cache = nil
		}
		d.cacheGen = gen
		// A rebuilt cache is empty and a dropped cache holds nothing, so the live
		// entry count is 0 either way.
		d.mGauge.Set(0)
	}

	// (Re)build complete with a non-nil client: re-arm release()'s fast path so a
	// subsequent disable performs the cleanup once more instead of short-circuiting.
	d.released.Store(false)

	return d.client, d.cache
}

// clientIdentity hashes the inputs that determine the HTTP client's wiring:
// scheme, endpoint, bearer-token generation, CA bundle, TLS floor, and SSRF
// relaxation. A change in any of these forces a client rebuild (close + new),
// so a hot-reloaded endpoint or rotated token never reuses a stale transport.
func clientIdentity(cfg Config, tokenGen string) string {
	h := sha256.New()
	caHash := sha256.Sum256([]byte(cfg.TLS.CAPEM))
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%x\x00%d\x00%t",
		cfg.Scheme, cfg.RemoteURL, tokenGen, caHash, cfg.TLS.MinVersion, cfg.SSRF.AllowPrivate)
	return hex.EncodeToString(h.Sum(nil))
}

// release closes the cached client + drops the decision cache. The released atomic
// is a LOCK-FREE FAST PATH for the steady-state disabled hot path (Load()==true ->
// return, no d.mu). The cleanup body stays under d.mu and is idempotent. BENIGN
// WINDOW: a concurrent re-enable (refresh: Store(false)) can interleave so a disable
// request that saw the OLD released==true skips the lock, leaving an idle client live
// under a disabled config for one request cycle — healed by the next disabled request.
// This changes NO decision: the disabled gate returns inner and never reads d.client.
// cacheGen is reset so a re-enable with identical config rebuilds the cache.
func (d *Decorator) release() {
	if d.released.Load() {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client != nil {
		d.client.Close()
		d.client = nil
		d.clientID = ""
	}
	d.cache = nil
	d.cacheGen = ""
	d.mGauge.Set(0)
	d.released.Store(true)
}

// configGen returns a stable hash of the config inputs that, when changed, must
// invalidate the decision cache: scheme, endpoint, failure policy, token
// generation, TLS/SSRF wiring, and every cache knob. A change in any of these
// could alter a decision (or which PDP produced it), so prior cached decisions
// must not survive the change.
func configGen(cfg Config, tokenGen string) string {
	h := sha256.New()
	caHash := sha256.Sum256([]byte(cfg.TLS.CAPEM))
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%x\x00%d\x00%t\x00%t\x00%d\x00%d\x00%d\x00%d",
		cfg.Scheme, cfg.RemoteURL, cfg.FailurePolicy, tokenGen,
		caHash, cfg.TLS.MinVersion, cfg.SSRF.AllowPrivate,
		cfg.Cache.Active, int64(cfg.Cache.TTLAllow), int64(cfg.Cache.TTLDeny),
		cfg.Cache.MaxEntries, int64(cfg.Cache.GraceTTL),
	)
	return hex.EncodeToString(h.Sum(nil))
}

// audit emits the path-agnostic PDP decision audit line. Request fields are
// preserved on every outcome (including failures) by threading the built req.
func (d *Decorator) audit(req Request, actor principal.Principal, ctxReq policy.RequestContext, decision, errType, reason string) {
	log.Info().
		Str("event", "iam.pdp").
		Str("request_id", req.RequestID).
		Str("principal_kind", string(actor.Kind)).
		Str("principal_id", actor.ID).
		Str("action", ctxReq.Action).
		Str("resource", ctxReq.Resource).
		Str("decision", decision).
		Str("layer", "pdp").
		Str("reason", reason).
		Str("error_type", errType).
		Msg("iam.pdp.decision")
}

// toWire maps a GrainFS principal to the PDP wire shape.
func toWire(p principal.Principal) WirePrincipal {
	return WirePrincipal{
		Kind:         string(p.Kind),
		ID:           p.ID,
		Issuer:       p.Issuer,
		Subject:      p.Subject,
		Groups:       p.GroupNames(),
		CredentialID: p.CredentialID,
	}
}

// annotate appends a layer marker to an existing reason without losing it.
func annotate(reason, marker string) string {
	if reason == "" {
		return marker
	}
	return reason + "; " + marker
}

// protocolFromAction returns the namespace prefix of a policy action ("s3" for
// "s3:GetObject"). Data_plane scope only — control plane keeps literal "admin"
// (its actions are grainfs:*, whose prefix is the IAM action namespace, not a protocol).
func protocolFromAction(action string) string {
	if i := strings.IndexByte(action, ':'); i > 0 {
		return action[:i]
	}
	return "admin"
}

// authMethod maps the actor to the PDP auth_method; an empty-ID service account
// is the anonymous S3 actor (saID==""), which presents "anonymous".
func authMethod(actor principal.Principal) string {
	if actor.Kind == principal.KindServiceAccount && actor.ID == "" {
		return "anonymous"
	}
	return string(actor.Kind)
}

// newRequestID returns a time-ordered request id, falling back to an empty
// string only if the entropy source fails (never expected in practice).
func newRequestID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return ""
	}
	return id.String()
}
