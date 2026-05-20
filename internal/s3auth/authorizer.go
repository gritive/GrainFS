package s3auth

import (
	"context"

	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/reservedname"
)

// Anon-allow reason tokens. Promoted to consts so the authorizer producer
// (this file) and the audit consumer (internal/server/server.go iamCheck
// closure) reference the SAME string — silently drifting the literal in
// one place would make every anonymous Allow re-classify as a regular
// authenticated allow on the audit.s3 row. T51' N1 review.
const (
	ReasonAnonEnabled               = "iam.anon-enabled=true"
	ReasonDefaultBucketImplicitAnon = "default bucket implicit anon (D#2)"
)

// adminUDSOnlyActions are unconditionally denied on the data plane (Decision #8).
var adminUDSOnlyActions = map[string]bool{
	"s3:CreateBucket":       true,
	"s3:DeleteBucket":       true,
	"s3:PutBucketPolicy":    true,
	"s3:DeleteBucketPolicy": true,
}

// Authorizer is the single authorization entry-point for the S3 data plane.
type Authorizer struct {
	resolver *policy.Resolver
	cfg      ConfigReader
}

// ConfigReader is the small slice of config store that Authorizer reads.
// Defined here so callers can pass a fake without dragging the FSM-backed store.
type ConfigReader interface {
	GetBool(key string) (value, ok bool)
}

// NewAuthorizer constructs an Authorizer backed by the given policy resolver and config.
func NewAuthorizer(r *policy.Resolver, c ConfigReader) *Authorizer {
	return &Authorizer{resolver: r, cfg: c}
}

// Authorize returns the policy.Evaluate result for (saID, bucket, action).
// saID == "" means anonymous.
//
//   - admin-UDS-only actions (D#8) are unconditionally denied on the data plane.
//   - Internal buckets (_grainfs/*) are admin-UDS-only on the data plane (spec
//     line 462 + F-A2). Both anonymous and authenticated SAs are denied. The
//     audit-internal SA's localhost read path bypasses Authorize entirely via
//     authenticateAuditInternalRequest (internal/server/authn_middleware.go),
//     so the audit reader is unaffected.
//   - When saID == "" and iam.anon-enabled=true, returns Allow without resolver
//     lookup (Phase 0 → Phase 1 progressive application).
//   - Otherwise runs full Evaluate with the SA's effective policies and the
//     bucket policy. AllowAnonBucket comes from iam.allow-anonymous-bucket-policy.
func (a *Authorizer) Authorize(ctx context.Context, saID, bucket string, ctxReq policy.RequestContext) policy.EvalResult {
	// Short-circuit paths below don't invoke policy.Evaluate, so they have
	// to attach the ConditionContext themselves; consumers (audit) rely on
	// the field being populated on every Allow/Deny outcome. T51' B2 review.
	cc := policy.ConditionContextFromRequest(ctxReq)
	if adminUDSOnlyActions[ctxReq.Action] {
		return policy.EvalResult{Decision: policy.DecisionDeny, Reason: "admin-UDS-only action (D#8)", ConditionContext: cc}
	}
	// F-A2 + spec line 462: internal buckets are admin-UDS-only on the data plane.
	// Denies BOTH anonymous and authenticated SAs. The audit-internal SA's read path
	// bypasses Authorize entirely (see internal/server auditInternalObjectReadAllowed),
	// so this does not break the audit reader.
	if reservedname.IsInternalBucket(bucket) {
		return policy.EvalResult{Decision: policy.DecisionDeny, Reason: "internal bucket deny (data plane is admin-UDS-only)", ConditionContext: cc}
	}
	// D#2: "default" bucket carries an implicit anon policy unless the operator
	// has attached an explicit bucket policy. Implicit policy survives Phase 0→2
	// transitions (i.e., it does NOT depend on iam.anon-enabled).
	if saID == "" && bucket == "default" {
		hasExplicit, err := a.resolver.HasBucketPolicy(ctx, "default")
		if err == nil && !hasExplicit {
			return policy.EvalResult{Decision: policy.DecisionAllow, Reason: ReasonDefaultBucketImplicitAnon, ConditionContext: cc}
		}
	}
	if saID == "" {
		if anon, ok := a.cfg.GetBool("iam.anon-enabled"); ok && anon {
			return policy.EvalResult{Decision: policy.DecisionAllow, Reason: ReasonAnonEnabled, ConditionContext: cc}
		}
		// Fall through: Principal:* on a bucket policy may still allow if iam.allow-anonymous-bucket-policy=true.
	}
	in, err := a.resolver.Effective(ctx, saID, bucket)
	if err != nil {
		return policy.EvalResult{Decision: policy.DecisionDeny, Reason: "resolver: " + err.Error(), ConditionContext: cc}
	}
	in.Ctx = ctxReq
	allowAnon, _ := a.cfg.GetBool("iam.allow-anonymous-bucket-policy")
	in.AllowAnonBucket = allowAnon
	return policy.Evaluate(in)
}
