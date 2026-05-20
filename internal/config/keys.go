package config

import "context"

// ReloadHooks carries optional subsystem callbacks that fire when a cluster-wide
// config key changes. Fields left nil are treated as no-ops.
//
// NOTE: OnDEKRotate and OnDEKVersionPrune are intentionally absent — those flows
// are handled by the FSM post-commit hook in Task 14, not by reload-hook closures.
type ReloadHooks struct {
	OnAnonEnabledChange     func(context.Context, bool) error
	OnAllowAnonBucketPolicy func(context.Context, bool) error
	OnTrustedProxyCIDR      func(context.Context, string) error
	OnJWTSigningKeyRotate   func(context.Context) error
	OnJWTSigningKeyPrune    func(context.Context) error
	OnClusterReadOnlyChange func(context.Context, bool) error
	OnAuditDenyOnly         func(context.Context, bool) error
}

// RegisterClusterKeys registers every cluster-wide config key into s.
// Subsystem reload-hook fields in h may be nil; absent hooks are no-ops.
func RegisterClusterKeys(s *Store, h ReloadHooks) {
	s.Register("iam.anon-enabled", BoolSpec{
		Default: true,
		Desc:    "Allow anonymous (unauthenticated) S3 access",
		OnReload: func(ctx context.Context, v bool) error {
			if h.OnAnonEnabledChange == nil {
				return nil
			}
			return h.OnAnonEnabledChange(ctx, v)
		},
	})

	s.Register("iam.allow-anonymous-bucket-policy", BoolSpec{
		Default: false,
		Desc:    "Allow anonymous access via bucket-level IAM policies",
		OnReload: func(ctx context.Context, v bool) error {
			if h.OnAllowAnonBucketPolicy == nil {
				return nil
			}
			return h.OnAllowAnonBucketPolicy(ctx, v)
		},
	})

	s.Register("trusted-proxy.cidr", StringSpec{
		Default: "",
		Desc:    "CIDR range of trusted reverse proxies (e.g. 10.0.0.0/8)",
		OnReload: func(ctx context.Context, v string) error {
			if h.OnTrustedProxyCIDR == nil {
				return nil
			}
			return h.OnTrustedProxyCIDR(ctx, v)
		},
	})

	s.Register("jwt.signing-key-rotate", TriggerSpec{
		Desc: "Rotate the JWT signing key (set to \"now\" to trigger)",
		OnTrigger: func(ctx context.Context) error {
			if h.OnJWTSigningKeyRotate == nil {
				return nil
			}
			return h.OnJWTSigningKeyRotate(ctx)
		},
	})

	s.Register("jwt.signing-key-prune", TriggerSpec{
		Desc: "Prune old JWT signing keys (set to \"now\" to trigger)",
		OnTrigger: func(ctx context.Context) error {
			if h.OnJWTSigningKeyPrune == nil {
				return nil
			}
			return h.OnJWTSigningKeyPrune(ctx)
		},
	})

	// encryption.rotate-dek: Task 14 FSM post-commit hook acts on this key.
	// The reload-hook closure is intentionally a no-op.
	s.Register("encryption.rotate-dek", TriggerSpec{
		Desc:      "Rotate the data-encryption key (set to \"now\" to trigger)",
		OnTrigger: func(_ context.Context) error { return nil },
	})

	// encryption.prune-dek-version: same — Task 14 handles the actual action.
	s.Register("encryption.prune-dek-version", Uint32Spec{
		Desc:     "Prune DEK versions older than this version number",
		OnReload: func(_ context.Context, _ uint32) error { return nil },
	})

	s.Register("cluster.read-only", BoolSpec{
		Default: false,
		Desc:    "Put the cluster in read-only mode (no writes accepted)",
		OnReload: func(ctx context.Context, v bool) error {
			if h.OnClusterReadOnlyChange == nil {
				return nil
			}
			return h.OnClusterReadOnlyChange(ctx, v)
		},
	})

	s.Register("audit.deny-only", BoolSpec{
		Default: false,
		Desc:    "When true, audit log records denials only (reduces log volume)",
		OnReload: func(ctx context.Context, v bool) error {
			if h.OnAuditDenyOnly == nil {
				return nil
			}
			return h.OnAuditDenyOnly(ctx, v)
		},
	})
}
