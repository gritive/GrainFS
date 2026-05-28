package config

import (
	"context"
	"fmt"

	iamoidc "github.com/gritive/GrainFS/internal/iam/oidc"
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

	// encryption.rotate-dek: GATED in R1. The key stays registered (catalog +
	// existing config tooling), but its trigger is rejected: connecting the
	// logical-WAL/packblob/PUT-pipeline sealers to the gen-aware DEK (R1
	// Commit 2) means a DEK rotation would advance the active gen, and those
	// append-only writers pin the gen at open — so a rotation would break them.
	// Proper data-DEK rotation with per-segment gen framing for every lane is a
	// future slice (spec decision #5). Rejecting here in OnTrigger makes
	// Store.Set roll back and surface the deferral error to the operator instead
	// of silently no-op'ing or reaching ProposeDEKRotate.
	s.Register("encryption.rotate-dek", TriggerSpec{
		Desc: "Rotate the data-encryption key (deferred — not supported in this release)",
		OnTrigger: func(_ context.Context) error {
			return fmt.Errorf("data-DEK rotation is deferred — not supported in this release; per-segment generation framing for all data lanes is a future slice")
		},
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
