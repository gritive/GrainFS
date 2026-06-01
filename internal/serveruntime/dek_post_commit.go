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
	ProposeDEKRewrapProgress(ctx context.Context, nodeID string, gen, epoch uint32) error
}

// DEKPostCommitDispatcher handles post-commit hooks related to DEK lifecycle.
// It decodes ConfigPut payloads for DEK-related keys and dispatches proposals
// asynchronously (via goroutines) so the FSM apply loop is never blocked.
type DEKPostCommitDispatcher struct {
	proposer       DEKProposer
	isLeader       func() bool
	scrubberKick   func(ctx context.Context, oldGen uint32)
	fsmValueRewrap func(ctx context.Context, activeGen uint32) // S7-1a: leader-only FSM-value drain
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
		// S5: data-DEK rotation is ENABLED. The config trigger accepts as a no-op
		// at apply time (config/keys.go); the actual rotation is proposed here,
		// after the ConfigPut commits, off the apply goroutine. ProposeDEKRotate is
		// leader-gated + single-flighted on its own; the go func ensures the
		// blocking propose never re-enters the apply loop.
		go func() {
			if err := d.proposer.ProposeDEKRotate(context.Background()); err != nil {
				log.Warn().Err(err).Msg("dek_post_commit: ProposeDEKRotate failed")
			}
		}()
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
	cmd, err := cluster.DecodeDEKReplicatedRotateCmd(payload)
	if err != nil {
		log.Warn().Err(err).Msg("dek_post_commit: failed to decode DEKReplicatedRotate payload")
		return
	}
	// Gen=0 is the bootstrap sentinel — there is no previous generation to rewrap.
	if cmd.Gen < 1 {
		return
	}
	// Fire EC-shard/packblob rewrap (every node) with the OLD gen.
	if d.scrubberKick != nil {
		oldGen := cmd.Gen - 1
		go d.scrubberKick(context.Background(), oldGen)
	}
	// S7-1a: Fire FSM-value drain (leader-only per group) with the NEW active gen
	// (cmd.Gen). Pass cmd.Gen directly — do NOT re-read the keeper to avoid
	// a stale-gen race if another rotation lands mid-drain.
	if d.fsmValueRewrap != nil {
		activeGen := cmd.Gen
		go d.fsmValueRewrap(context.Background(), activeGen)
	}
}

// WireDEKPostCommit constructs a DEKPostCommitDispatcher and registers it as a
// post-commit hook on fsm. The scrubberKick parameter may be nil (used in §1
// before the storage-layer adapter is implemented). fsmValueRewrap may be nil
// (epoch-neutral; set once wireDEKKeeper has wired dgMgr). S7-1a.
func WireDEKPostCommit(
	fsm *cluster.MetaFSM,
	proposer DEKProposer,
	isLeader func() bool,
	scrubberKick func(ctx context.Context, oldGen uint32),
	fsmValueRewrap func(ctx context.Context, activeGen uint32),
) {
	d := &DEKPostCommitDispatcher{
		proposer:       proposer,
		isLeader:       isLeader,
		scrubberKick:   scrubberKick,
		fsmValueRewrap: fsmValueRewrap,
	}
	fsm.RegisterPostCommit(d.Handle)
}
