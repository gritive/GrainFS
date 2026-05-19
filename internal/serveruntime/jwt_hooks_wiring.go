package serveruntime

import (
	"context"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/config"
)

// jwtProposer is the subset of *cluster.MetaRaft used by the JWT reload hooks.
// Extracted as an interface so tests can substitute a fake without standing up
// a full raft fixture.
type jwtProposer interface {
	Propose(ctx context.Context, cmdType cluster.MetaCmdType, payload []byte) error
}

// wireJWTReloadHooks returns ReloadHooks that forward jwt.signing-key-rotate /
// jwt.signing-key-prune config triggers to the meta-raft proposer as
// MetaCmdTypeJWTSigningKeyRotate / MetaCmdTypeJWTSigningKeyPrune.
//
// The two cmds carry no payload (nil); the FSM applies them by rotating or
// pruning the JWT signing-key ring that was wired by T35.
func wireJWTReloadHooks(p jwtProposer) config.ReloadHooks {
	return config.ReloadHooks{
		OnJWTSigningKeyRotate: func(ctx context.Context) error {
			return p.Propose(ctx, cluster.MetaCmdTypeJWTSigningKeyRotate, nil)
		},
		OnJWTSigningKeyPrune: func(ctx context.Context) error {
			return p.Propose(ctx, cluster.MetaCmdTypeJWTSigningKeyPrune, nil)
		},
	}
}
