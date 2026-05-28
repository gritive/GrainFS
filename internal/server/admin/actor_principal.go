package admin

import (
	"context"

	"github.com/gritive/GrainFS/internal/iam/principal"
)

type actorPrincipalContextKey struct{}

func WithActorPrincipal(ctx context.Context, p principal.Principal) context.Context {
	if p.Kind == "" || p.ID == "" {
		return ctx
	}
	return context.WithValue(ctx, actorPrincipalContextKey{}, p)
}

func ActorPrincipalFromContext(ctx context.Context) (principal.Principal, bool) {
	p, ok := ctx.Value(actorPrincipalContextKey{}).(principal.Principal)
	if !ok || p.Kind == "" || p.ID == "" {
		return principal.Principal{}, false
	}
	return p, true
}
