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
