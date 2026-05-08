package s3auth

import "context"

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
}

// IAMStore is the subset of *iam.Store the authorizer depends on.
type IAMStore interface {
	AuthEnabled() bool
}

// IAMChecker checks whether a service-account ID has a grant for (bucket, action).
// iam.CheckAccess satisfies this signature when bound to a *iam.Store.
type IAMChecker func(saID, bucket string, action S3Action) bool

// PolicyChecker evaluates bucket policy. *policy.CompiledPolicyStore satisfies this.
type PolicyChecker interface {
	Allow(ctx context.Context, in PermCheckInput) bool
}

// AuditEmitter records authorization decisions. *iam.AuditLogger satisfies this.
type AuditEmitter interface {
	RecordAllow(ctx context.Context, saID, bucket, key string, action S3Action)
	RecordDeny(ctx context.Context, saID, bucket, key string, action S3Action, reason string)
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

	// Layer 1: IAM grant.
	if authEnabled {
		ok := r.iamCheck != nil && r.iamCheck(saID, in.Resource.Bucket, in.Action)
		if !ok {
			r.recordDeny(ctx, saID, in, "no_grant")
			return Decision{Allow: false, Layer: "iam_grant", Reason: "no_grant"}
		}
	}

	// Layer 2: bucket policy. Skipped for *BucketPolicy CRUD to avoid
	// chicken-and-egg lockout; IAM (Layer 1) already gates these actions.
	if !isBucketPolicyAction(in.Action) && r.policy != nil {
		if !r.policy.Allow(ctx, in) {
			r.recordDeny(ctx, saID, in, "policy_deny")
			return Decision{Allow: false, Layer: "bucket_policy", Reason: "policy_deny"}
		}
	}

	// Layer 3 is added in the next task.

	if authEnabled {
		r.recordAllow(ctx, saID, in)
		return Decision{Allow: true, Layer: "iam_grant"}
	}
	return Decision{Allow: true, Layer: "anonymous_pass"}
}

func (r *RequestAuthorizer) recordAllow(ctx context.Context, saID string, in PermCheckInput) {
	if r.audit == nil {
		return
	}
	r.audit.RecordAllow(ctx, saID, in.Resource.Bucket, in.Resource.Key, in.Action)
}

func (r *RequestAuthorizer) recordDeny(ctx context.Context, saID string, in PermCheckInput, reason string) {
	if r.audit == nil {
		return
	}
	r.audit.RecordDeny(ctx, saID, in.Resource.Bucket, in.Resource.Key, in.Action, reason)
}

func isBucketPolicyAction(a S3Action) bool {
	return a == GetBucketPolicy || a == PutBucketPolicy || a == DeleteBucketPolicy
}
