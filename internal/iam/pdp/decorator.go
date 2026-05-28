package pdp

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/gritive/GrainFS/internal/metrics"
)

// ConfigKey is the config store key holding the iam.pdp JSON document.
const ConfigKey = "iam.pdp"

// Layer markers recorded in the audit log and surfaced in EvalResult.Reason.
const (
	layerDeny       = "pdp_deny"
	layerAllow      = "pdp_allow"
	layerFailOpen   = "pdp_skipped_fail_open"
	layerFailClosed = "pdp_unavailable"
)

// genericDenyMsg is the user-facing reason for a PDP deny. The PDP-supplied raw
// reason is recorded ONLY in the audit log, never returned to the caller.
const genericDenyMsg = "pdp_deny: denied by external policy"

const schemaVersion = 1

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

// Decorator chains an external PDP AFTER the GrainFS IAM authorizer using a
// deny-override rule: a request is allowed only if BOTH GrainFS and the PDP
// allow. It reads the iam.pdp config per request, so an operator can flip
// enabled/failure_policy/endpoint at runtime without a restart. When disabled
// (or the config is missing/invalid) it is a pure pass-through.
type Decorator struct {
	inner innerAuthorizer
	cfg   ConfigReader

	mu         sync.Mutex
	client     *Client
	clientSock string
}

// NewDecorator wraps inner with a per-request PDP chain driven by cfg.
func NewDecorator(inner innerAuthorizer, cfg ConfigReader) *Decorator {
	return &Decorator{inner: inner, cfg: cfg}
}

// Authorize chains the PDP after the GrainFS service-account authorizer. The
// actor presented to the PDP is the named service account.
func (d *Decorator) Authorize(ctx context.Context, saID, bucket string, ctxReq policy.RequestContext) policy.EvalResult {
	inner := d.inner.Authorize(ctx, saID, bucket, ctxReq)
	return d.chain(ctx, principal.ServiceAccount(saID), saID, inner, ctxReq)
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

// chain applies the PDP consultation given the GrainFS (inner) result.
func (d *Decorator) chain(ctx context.Context, actor principal.Principal, targetSA string, inner policy.EvalResult, ctxReq policy.RequestContext) policy.EvalResult {
	raw, ok := d.cfg.GetString(ConfigKey)
	if !ok {
		return inner // unconfigured: pure pass-through
	}
	cfg, err := ParseConfig([]byte(raw))
	if err != nil {
		log.Warn().Err(err).Str("event", "iam.pdp").Msg("iam.pdp: invalid config, treating as disabled")
		return inner
	}
	if !cfg.Enabled {
		return inner
	}

	// Deny-override: only consult the PDP when GrainFS already allowed.
	if inner.Decision != policy.DecisionAllow {
		return inner
	}

	req := Request{
		SchemaVersion: schemaVersion,
		RequestID:     newRequestID(),
		Principal:     toWire(actor),
		Action:        ctxReq.Action,
		Resource:      ctxReq.Resource,
		Protocol:      "admin",
		Context: map[string]string{
			"auth_method": string(actor.Kind),
			"target_sa":   targetSA,
			"route":       ctxReq.Action,
		},
	}

	fp := string(cfg.FailurePolicy)
	client := d.clientFor(cfg)

	// The decorator owns the per-request deadline so a runtime iam.pdp.timeout
	// change takes effect without rebuilding the cached client. Deriving callCtx
	// from the FRESH cfg here (not in the client) is what makes the timeout
	// track hot-reload.
	callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	start := time.Now()
	_, errType, err := client.Authorize(callCtx, req)
	metrics.PDPRequestDuration.Observe(time.Since(start).Seconds())

	// Caller-canceled takes precedence over the failure policy: a fail-open
	// config must not turn an abandoned request into an allow. We check the
	// ORIGINAL inbound ctx (not callCtx): a client-timeout fires callCtx while
	// the inbound ctx stays live, so it falls through to the failure policy
	// below; only a genuine inbound cancel/deadline lands here.
	if ctx.Err() != nil {
		cancelErrType := ErrTypeTransport
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			cancelErrType = ErrTypeTimeout
		}
		metrics.PDPRequestsTotal.WithLabelValues("error", cancelErrType, fp).Inc()
		d.audit(req, actor, ctxReq, "deny", cancelErrType, "request canceled")
		return policy.EvalResult{Decision: policy.DecisionDeny, Reason: "request canceled"}
	}

	var de *DenyError
	if errors.As(err, &de) {
		metrics.PDPRequestsTotal.WithLabelValues("deny", "", fp).Inc()
		d.audit(req, actor, ctxReq, "deny", "", layerDeny+": "+de.Reason)
		return policy.EvalResult{Decision: policy.DecisionDeny, Reason: genericDenyMsg}
	}

	if errType != "" {
		metrics.PDPRequestsTotal.WithLabelValues("error", errType, fp).Inc()
		if cfg.FailurePolicy == FailureOpen {
			d.audit(req, actor, ctxReq, "allow", errType, layerFailOpen)
			out := inner
			out.Reason = annotate(out.Reason, layerFailOpen)
			return out
		}
		d.audit(req, actor, ctxReq, "deny", errType, layerFailClosed)
		return policy.EvalResult{Decision: policy.DecisionDeny, Reason: layerFailClosed}
	}

	metrics.PDPRequestsTotal.WithLabelValues("allow", "", fp).Inc()
	d.audit(req, actor, ctxReq, "allow", "", layerAllow)
	return inner
}

// clientFor returns a cached *Client, rebuilding it only when the socket path
// changes. The FailurePolicy is read per request from cfg, not the cached
// client, so a runtime policy flip takes effect without a client rebuild.
func (d *Decorator) clientFor(cfg Config) *Client {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client == nil || d.clientSock != cfg.SocketPath {
		if d.client != nil {
			// Endpoint changed: release the old client's idle keep-alive
			// connections so hot-reloading the socket doesn't leak FDs.
			d.client.Close()
		}
		d.client = NewClient(cfg)
		d.clientSock = cfg.SocketPath
	}
	return d.client
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

// newRequestID returns a time-ordered request id, falling back to an empty
// string only if the entropy source fails (never expected in practice).
func newRequestID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return ""
	}
	return id.String()
}
