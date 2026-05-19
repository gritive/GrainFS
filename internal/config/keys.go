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

// RegisterClusterKeys registers the 9 cluster-wide config keys into s.
// Subsystem reload-hook fields in h may be nil; absent hooks are no-ops.
func RegisterClusterKeys(s *Store, h ReloadHooks) {
	s.Register("iam.anon-enabled", BoolSpec{
		Default: true,
		OnReload: func(ctx context.Context, v bool) error {
			if h.OnAnonEnabledChange == nil {
				return nil
			}
			return h.OnAnonEnabledChange(ctx, v)
		},
	})

	s.Register("iam.allow-anonymous-bucket-policy", BoolSpec{
		Default: false,
		OnReload: func(ctx context.Context, v bool) error {
			if h.OnAllowAnonBucketPolicy == nil {
				return nil
			}
			return h.OnAllowAnonBucketPolicy(ctx, v)
		},
	})

	s.Register("trusted-proxy.cidr", StringSpec{
		Default: "",
		OnReload: func(ctx context.Context, v string) error {
			if h.OnTrustedProxyCIDR == nil {
				return nil
			}
			return h.OnTrustedProxyCIDR(ctx, v)
		},
	})

	s.Register("jwt.signing-key-rotate", TriggerSpec{
		OnTrigger: func(ctx context.Context) error {
			if h.OnJWTSigningKeyRotate == nil {
				return nil
			}
			return h.OnJWTSigningKeyRotate(ctx)
		},
	})

	s.Register("jwt.signing-key-prune", TriggerSpec{
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
		OnTrigger: func(_ context.Context) error { return nil },
	})

	// encryption.prune-dek-version: same — Task 14 handles the actual action.
	s.Register("encryption.prune-dek-version", Uint32Spec{
		OnReload: func(_ context.Context, _ uint32) error { return nil },
	})

	s.Register("cluster.read-only", BoolSpec{
		Default: false,
		OnReload: func(ctx context.Context, v bool) error {
			if h.OnClusterReadOnlyChange == nil {
				return nil
			}
			return h.OnClusterReadOnlyChange(ctx, v)
		},
	})

	s.Register("audit.deny-only", BoolSpec{
		Default: false,
		OnReload: func(ctx context.Context, v bool) error {
			if h.OnAuditDenyOnly == nil {
				return nil
			}
			return h.OnAuditDenyOnly(ctx, v)
		},
	})
}
