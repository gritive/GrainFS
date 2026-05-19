package serveruntime

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/encrypt"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
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
// All non-deterministic work (rand.Read, keeper.Seal, NewKid, time.Now) is done
// here on the proposer side. The payload that lands in the Raft log carries only
// deterministic bytes, so every replica's FSM.applyCmd produces the same result.
func wireJWTReloadHooks(p jwtProposer, keeper *encrypt.DEKKeeper) config.ReloadHooks {
	return config.ReloadHooks{
		OnJWTSigningKeyRotate: func(ctx context.Context) error {
			if keeper == nil {
				return fmt.Errorf("jwt rotate: DEK keeper not wired")
			}
			secret := make([]byte, 32)
			if _, err := rand.Read(secret); err != nil {
				return fmt.Errorf("jwt rotate: rand secret: %w", err)
			}
			wrapped, gen, err := keeper.Seal(secret)
			if err != nil {
				return fmt.Errorf("jwt rotate: seal: %w", err)
			}
			kid, err := iamjwt.NewKid()
			if err != nil {
				return fmt.Errorf("jwt rotate: new kid: %w", err)
			}
			payload := cluster.EncodeMetaJWTSigningKeyRotateCmd(kid, wrapped, gen, time.Now().Unix())
			return p.Propose(ctx, cluster.MetaCmdTypeJWTSigningKeyRotate, payload)
		},
		OnJWTSigningKeyPrune: func(ctx context.Context) error {
			payload := cluster.EncodeMetaJWTSigningKeyPruneCmd(time.Now().Unix())
			return p.Propose(ctx, cluster.MetaCmdTypeJWTSigningKeyPrune, payload)
		},
	}
}
