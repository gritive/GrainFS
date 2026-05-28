package serveruntime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/iam/oidc"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/gritive/GrainFS/internal/server/admin"
)

type oidcIssuerConfigReader interface {
	GetString(key string) (string, bool)
}

type oidcActorAuthenticator struct {
	cfgStore oidcIssuerConfigReader
	keys     oidc.KeySource
	now      func() time.Time
}

func newOIDCActorAuthenticator(cfgStore oidcIssuerConfigReader) admin.ActorAuthenticator {
	if cfgStore == nil {
		return nil
	}
	return newOIDCActorAuthenticatorWithKeys(cfgStore, oidc.NewJWKSCache(http.DefaultClient, 5*time.Minute), time.Now)
}

func newOIDCActorAuthenticatorWithKeys(cfgStore oidcIssuerConfigReader, keys oidc.KeySource, now func() time.Time) admin.ActorAuthenticator {
	if cfgStore == nil || keys == nil {
		return nil
	}
	if now == nil {
		now = time.Now
	}
	return &oidcActorAuthenticator{cfgStore: cfgStore, keys: keys, now: now}
}

func (a *oidcActorAuthenticator) AuthenticateActor(ctx context.Context, bearerToken string) (principal.Principal, error) {
	if strings.TrimSpace(bearerToken) == "" {
		return principal.Principal{}, errors.New("bearer token required")
	}
	raw, ok := a.cfgStore.GetString("iam.oidc.issuers")
	if !ok || strings.TrimSpace(raw) == "" {
		return principal.Principal{}, errors.New("oidc issuers not configured")
	}
	issuers, err := oidc.ParseIssuerConfigs([]byte(raw))
	if err != nil {
		return principal.Principal{}, err
	}
	if len(issuers) == 0 {
		return principal.Principal{}, errors.New("oidc issuers not configured")
	}
	var lastErr error
	for _, issuer := range issuers {
		p, err := oidc.NewAuthenticator(issuer, a.keys).Authenticate(ctx, bearerToken, a.now())
		if err == nil {
			return p, nil
		}
		lastErr = err
	}
	return principal.Principal{}, fmt.Errorf("oidc authentication failed: %w", lastErr)
}
