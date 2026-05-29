package config

import (
	"context"

	iamoidc "github.com/gritive/GrainFS/internal/iam/oidc"
	iampdp "github.com/gritive/GrainFS/internal/iam/pdp"
)

// ReloadHooks carries optional subsystem callbacks that fire when a cluster-wide
// config key changes. Fields left nil are treated as no-ops.
//
// NOTE: OnDEKRotate and OnDEKVersionPrune are intentionally absent — those flows
// are handled by the FSM post-commit hook in Task 14, not by reload-hook closures.
type ReloadHooks struct {
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

	s.Register("iam.oidc.issuers", StringSpec{
		Default: "[]",
		Desc:    "OIDC issuer configuration JSON for federated IAM authentication",
		Validate: func(raw string) error {
			_, err := iamoidc.ParseIssuerConfigs([]byte(raw))
			return err
		},
	})

	s.Register("iam.pdp", StringSpec{
		Default: `{"enabled":false}`,
		Desc:    "External PDP adapter configuration JSON (disabled by default)",
		Validate: func(raw string) error {
			_, err := iampdp.ParseConfig([]byte(raw))
			return err
		},
	})

	s.Register("iam.pdp.token", StringSpec{
		Default: "",
		Desc:    "Sealed External PDP bearer-token envelope (managed by `grainfs iam pdp set-token`; do not edit directly)",
		Validate: func(raw string) error {
			if raw == "" {
				return nil
			}
			_, err := iampdp.ParseTokenEnvelope([]byte(raw))
			return err
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

	// encryption.rotate-dek: ENABLED (S5). All data lanes now carry gen framing
	// or self-roll on a gen advance (packblob per-entry gen S1; datawal/logical-WAL
	// rotation boundaries S2/S3; EC/object seal-at-pinned-gen S4; datawal Append
	// roll-then-retry S5), so advancing the active gen is safe. OnTrigger is a
	// no-op accept: it runs SYNCHRONOUSLY inside the raft apply loop
	// (applyConfigPut → cfgStore.Set → fireReload), where calling the blocking
	// ProposeDEKRotate would deadlock the apply loop. The actual rotation is
	// proposed by the post-commit dispatcher (serveruntime/dek_post_commit.go),
	// which runs after apply and escapes the apply goroutine — mirroring the
	// encryption.prune-dek-version path below. Leader-gating lives there.
	s.Register("encryption.rotate-dek", TriggerSpec{
		Desc:      "Rotate the data-encryption key (set to \"now\" to trigger)",
		OnTrigger: func(_ context.Context) error { return nil },
	})

	// encryption.prune-dek-version: same shape — the post-commit dispatcher
	// handles the actual action; this accept is a no-op at apply time.
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
