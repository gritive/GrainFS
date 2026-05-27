package serveruntime

import (
	"context"
	"strconv"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// DEKProposer is the subset of *cluster.MetaRaft needed by DEKPostCommitDispatcher.
// Defined here so MetaRaft satisfies it implicitly without import cycles.
type DEKProposer interface {
	ProposeDEKRotate(ctx context.Context) error
	ProposeDEKVersionPrune(ctx context.Context, gen uint32) error
}

// DEKPostCommitDispatcher handles post-commit hooks related to DEK lifecycle.
// It decodes ConfigPut payloads for DEK-related keys and dispatches proposals
// asynchronously (via goroutines) so the FSM apply loop is never blocked.
type DEKPostCommitDispatcher struct {
	proposer     DEKProposer
	isLeader     func() bool
	scrubberKick func(ctx context.Context, oldGen uint32)
}

// Handle implements cluster.PostCommitHook. It is called from the FSM apply
// goroutine after a command commits; it MUST NOT block. All side-effectful
// work (proposals, scrubber kicks) is dispatched in goroutines.
func (d *DEKPostCommitDispatcher) Handle(cmdType clusterpb.MetaCmdType, payload []byte) {
	switch cmdType {
	case clusterpb.MetaCmdTypeConfigPut:
		d.handleConfigPut(payload)
	case clusterpb.MetaCmdTypeDEKReplicatedRotate:
		d.handleDEKReplicatedRotate(payload)
	}
}

func (d *DEKPostCommitDispatcher) handleConfigPut(payload []byte) {
	// Leader-only gate: collapse N-node fan-out to a single proposal.
	// nil isLeader is treated as not-leader (fail-safe).
	if d.proposer == nil {
		return
	}
	if d.isLeader == nil || !d.isLeader() {
		return
	}
	key, value, err := cluster.DecodeConfigPutPayload(payload)
	if err != nil {
		log.Warn().Err(err).Msg("dek_post_commit: failed to decode ConfigPut payload")
		return
	}
	switch key {
	case "encryption.rotate-dek":
		if value == "now" {
			go func() {
				if err := d.proposer.ProposeDEKRotate(context.Background()); err != nil {
					log.Warn().Err(err).Msg("dek_post_commit: ProposeDEKRotate failed")
				}
			}()
		}
	case "encryption.prune-dek-version":
		gen64, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			log.Warn().Err(err).Str("value", value).Msg("dek_post_commit: invalid prune-dek-version value")
			return
		}
		gen := uint32(gen64)
		go func() {
			if err := d.proposer.ProposeDEKVersionPrune(context.Background(), gen); err != nil {
				log.Warn().Err(err).Uint32("gen", gen).Msg("dek_post_commit: ProposeDEKVersionPrune failed")
			}
		}()
	}
}

func (d *DEKPostCommitDispatcher) handleDEKReplicatedRotate(payload []byte) {
	if d.scrubberKick == nil {
		return
	}
	cmd, err := cluster.DecodeDEKReplicatedRotateCmd(payload)
	if err != nil {
		log.Warn().Err(err).Msg("dek_post_commit: failed to decode DEKReplicatedRotate payload")
		return
	}
	// Gen=0 is the bootstrap sentinel — there is no previous generation to rewrap.
	if cmd.Gen < 1 {
		return
	}
	oldGen := cmd.Gen - 1
	go d.scrubberKick(context.Background(), oldGen)
}

// WireDEKPostCommit constructs a DEKPostCommitDispatcher and registers it as a
// post-commit hook on fsm. The scrubberKick parameter may be nil (used in §1
// before the storage-layer adapter is implemented).
func WireDEKPostCommit(
	fsm *cluster.MetaFSM,
	proposer DEKProposer,
	isLeader func() bool,
	scrubberKick func(ctx context.Context, oldGen uint32),
) {
	d := &DEKPostCommitDispatcher{
		proposer:     proposer,
		isLeader:     isLeader,
		scrubberKick: scrubberKick,
	}
	fsm.RegisterPostCommit(d.Handle)
}
