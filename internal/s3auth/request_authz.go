package s3auth

import (
	"context"
	"time"
)

// Phase distinguishes pre-load and post-load authorization evaluation.
// PhasePreLoad evaluates Layer 1 (IAM grant) + Layer 2 (bucket policy); ObjectACL is ignored.
// PhasePostLoad additionally evaluates Layer 3 (object ACL) using PermCheckInput.ObjectACL.
type Phase uint8

const (
	PhasePreLoad Phase = iota
	PhasePostLoad
)

// Decision is the composed verdict returned by RequestAuthorizer.Decide.
// Layer names the producing layer ("iam_grant", "bucket_policy", "object_acl_*", "anonymous_pass").
// Reason is non-empty for deny ("no_grant", "policy_deny", "acl_anonymous_denied", "acl_private_denied").
type Decision struct {
	Allow  bool
	Layer  string
	Reason string
	// Detail surfaces the Layer 1 policy-decision metadata (matched policy
	// id, matched sid, anon-allow flag, condition context) so callers can
	// thread it into the audit envelope without a second policy evaluation.
	// T51' §6.
	Detail AuthzDetail
	// AuthzLatencyUS reports the wall-clock microseconds the authorizer
	// spent producing this decision. T51' §6.
	AuthzLatencyUS int32
}

// AuthzDetail carries policy decision metadata threaded out of the Layer 1
// check so the audit emitter can record matched_policy_id, matched_sid, and
// the evaluator's reason without depending on the iam/policy package
// directly. Zero values are valid and mean "no detail available". T51' §6.
type AuthzDetail struct {
	MatchedPolicyID  string
	MatchedSID       string
	Reason           string
	AnonAllow        bool
	ConditionContext map[string]string
}

// IAMStore is the subset of *iam.Store the authorizer depends on.
type IAMStore interface {
	AuthEnabled() bool
}

// IAMChecker is the Layer 1 policy gate. saID is "" for anonymous requests.
// bucket is the target bucket name; key is the object key (empty for
// bucket-level actions like ListBucket). Passing the key is required for
// object-scope Deny statements (Resource: arn:aws:s3:::bucket/path/*) to
// match at L1 — without it a paired "Allow Resource:*" silently bypasses
// the Deny.
//
// The second return value carries optional policy-decision metadata for
// audit (T51' §6). Callers may return a zero AuthzDetail; the authorizer
// treats it as "no detail available".
type IAMChecker func(saID, bucket, key string, action S3Action) (bool, AuthzDetail)

// PolicyChecker evaluates bucket policy. *policy.CompiledPolicyStore satisfies this.
type PolicyChecker interface {
	Allow(ctx context.Context, in PermCheckInput) bool
}

// AuditEmitter records authorization decisions. *iam.AuditLogger satisfies this.
type AuditEmitter interface {
	RecordAllow(ctx context.Context, saID, bucket, key string, action S3Action)
	RecordDeny(ctx context.Context, saID, bucket, key string, action S3Action, reason string)
}

// AuditEmitterDetailed is an optional extension of AuditEmitter that accepts
// policy decision metadata (matched policy/sid, evaluator latency, condition
// context, anon-allow flag). *iam.AuditLogger satisfies this. When an
// AuditEmitter does not also implement AuditEmitterDetailed, RequestAuthorizer
// falls back to the bool-only RecordAllow/RecordDeny path.
type AuditEmitterDetailed interface {
	RecordAllowDetailed(ctx context.Context, saID, bucket, key string, action S3Action, d AuditAllowDetails)
	RecordDenyDetailed(ctx context.Context, saID, bucket, key string, action S3Action, reason string, d AuditAllowDetails)
	RecordAnonAllow(ctx context.Context, bucket, key string, action S3Action, d AuditAllowDetails)
}

// AuditAllowDetails carries the policy decision metadata into the audit
// emitter. Mirrors iam.AuditDetails — defined here to keep s3auth free of
// an iam import. T51' §6.
type AuditAllowDetails struct {
	MatchedPolicyID  string
	MatchedSID       string
	AuthzLatencyUS   int32
	ConditionContext map[string]string
}

// PrincipalResolver returns the SA id stored in ctx. iam.PrincipalFromContext satisfies this.
type PrincipalResolver func(ctx context.Context) string

// RequestAuthorizer composes IAM grant, bucket policy, and object ACL into a
// single authz decision for one S3 request. See CONTEXT.md
// § "S3 Request Authorization Decision" for the full contract.
type RequestAuthorizer struct {
	iam              IAMStore
	iamCheck         IAMChecker
	policy           PolicyChecker
	audit            AuditEmitter
	principalFromCtx PrincipalResolver
}

// NewRequestAuthorizer wires the authorizer with its dependencies. Any nil
// dependency is tolerated:
//   - iam == nil treats auth as disabled.
//   - iamCheck == nil with auth enabled denies with "no_grant".
//   - policy == nil treats every bucket-policy check as allow.
//   - audit == nil silently drops audit records.
//   - principalFromCtx == nil resolves saID to "".
func NewRequestAuthorizer(
	iam IAMStore,
	iamCheck IAMChecker,
	policy PolicyChecker,
	audit AuditEmitter,
	principalFromCtx PrincipalResolver,
) *RequestAuthorizer {
	return &RequestAuthorizer{
		iam:              iam,
		iamCheck:         iamCheck,
		policy:           policy,
		audit:            audit,
		principalFromCtx: principalFromCtx,
	}
}

// Decide evaluates the request against IAM grant, bucket policy, and (when
// phase == PhasePostLoad) the loaded object's ACL. Audit records are emitted
// for every call when auth is enabled; anonymous mode (auth disabled) does
// not audit allows but still audits denies.
func (r *RequestAuthorizer) Decide(ctx context.Context, in PermCheckInput, phase Phase) Decision {
	saID := ""
	if r.principalFromCtx != nil {
		saID = r.principalFromCtx(ctx)
	}
	authEnabled := r.iam != nil && r.iam.AuthEnabled()

	// Latency clock starts here so the audit row's authz_latency_us reflects
	// the full Layer 1 + Layer 2 + Layer 3 evaluation as observed by the
	// authorizer (the part operators can influence via policy edits). T51' §6.
	t0 := time.Now()
	var detail AuthzDetail

	// Layer 1: IAM grant.
	if authEnabled {
		ok := false
		if r.iamCheck != nil {
			ok, detail = r.iamCheck(saID, in.Resource.Bucket, in.Resource.Key, in.Action)
		}
		if !ok {
			lat := elapsedUS(t0)
			r.recordDeny(ctx, saID, in, "no_grant", detail, lat)
			return Decision{Allow: false, Layer: "iam_grant", Reason: "no_grant", Detail: detail, AuthzLatencyUS: lat}
		}
	}

	// Layer 2: bucket policy. Skipped for *BucketPolicy CRUD to avoid
	// chicken-and-egg lockout; IAM (Layer 1) already gates these actions.
	if !isBucketPolicyAction(in.Action) && r.policy != nil {
		if !r.policy.Allow(ctx, in) {
			lat := elapsedUS(t0)
			r.recordDeny(ctx, saID, in, "policy_deny", detail, lat)
			return Decision{Allow: false, Layer: "bucket_policy", Reason: "policy_deny", Detail: detail, AuthzLatencyUS: lat}
		}
	}

	// Layer 3: object ACL (post-load only).
	if phase == PhasePostLoad {
		if !IsAuthorizedByACL(in.ObjectACL, in.Principal.AccessKey, in.Action) {
			reason := aclDenyReason(in.Principal.AccessKey)
			lat := elapsedUS(t0)
			r.recordDeny(ctx, saID, in, reason, detail, lat)
			return Decision{Allow: false, Layer: "object_acl", Reason: reason, Detail: detail, AuthzLatencyUS: lat}
		}
		layer := postLoadAllowLayer(in)
		lat := elapsedUS(t0)
		if authEnabled {
			r.recordAllow(ctx, saID, in, detail, lat)
		}
		return Decision{Allow: true, Layer: layer, Detail: detail, AuthzLatencyUS: lat}
	}

	lat := elapsedUS(t0)
	if authEnabled {
		r.recordAllow(ctx, saID, in, detail, lat)
		return Decision{Allow: true, Layer: "iam_grant", Detail: detail, AuthzLatencyUS: lat}
	}
	return Decision{Allow: true, Layer: "anonymous_pass", Detail: detail, AuthzLatencyUS: lat}
}

func elapsedUS(t0 time.Time) int32 {
	us := time.Since(t0).Microseconds()
	if us < 0 {
		return 0
	}
	if us > int64(int32(^uint32(0)>>1)) {
		return int32(^uint32(0) >> 1)
	}
	return int32(us)
}

func (r *RequestAuthorizer) recordAllow(ctx context.Context, saID string, in PermCheckInput, detail AuthzDetail, latencyUS int32) {
	if r.audit == nil {
		return
	}
	if detailed, ok := r.audit.(AuditEmitterDetailed); ok {
		d := AuditAllowDetails{
			MatchedPolicyID:  detail.MatchedPolicyID,
			MatchedSID:       detail.MatchedSID,
			AuthzLatencyUS:   latencyUS,
			ConditionContext: detail.ConditionContext,
		}
		if detail.AnonAllow {
			detailed.RecordAnonAllow(ctx, in.Resource.Bucket, in.Resource.Key, in.Action, d)
			return
		}
		detailed.RecordAllowDetailed(ctx, saID, in.Resource.Bucket, in.Resource.Key, in.Action, d)
		return
	}
	r.audit.RecordAllow(ctx, saID, in.Resource.Bucket, in.Resource.Key, in.Action)
}

func (r *RequestAuthorizer) recordDeny(ctx context.Context, saID string, in PermCheckInput, reason string, detail AuthzDetail, latencyUS int32) {
	if r.audit == nil {
		return
	}
	if detailed, ok := r.audit.(AuditEmitterDetailed); ok {
		detailed.RecordDenyDetailed(ctx, saID, in.Resource.Bucket, in.Resource.Key, in.Action, reason, AuditAllowDetails{
			MatchedPolicyID:  detail.MatchedPolicyID,
			MatchedSID:       detail.MatchedSID,
			AuthzLatencyUS:   latencyUS,
			ConditionContext: detail.ConditionContext,
		})
		return
	}
	r.audit.RecordDeny(ctx, saID, in.Resource.Bucket, in.Resource.Key, in.Action, reason)
}

func isBucketPolicyAction(a S3Action) bool {
	return a == GetBucketPolicy || a == PutBucketPolicy || a == DeleteBucketPolicy
}

func aclDenyReason(accessKey string) string {
	if accessKey == "" {
		return "acl_anonymous_denied"
	}
	return "acl_private_denied"
}

func postLoadAllowLayer(in PermCheckInput) string {
	if in.ObjectACL&ACLPublicReadWrite != 0 {
		return "object_acl_public_read_write"
	}
	if in.ObjectACL&ACLPublicRead != 0 && isReadAction(in.Action) {
		return "object_acl_public_read"
	}
	return "object_acl_authenticated"
}
