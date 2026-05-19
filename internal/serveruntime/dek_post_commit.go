package serveruntime

import (
	"context"
	"strconv"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// dekProposer is the subset of *cluster.MetaRaft needed by dekPostCommitDispatcher.
// Defined here so MetaRaft satisfies it implicitly without import cycles.
type dekProposer interface {
	ProposeDEKRotate(ctx context.Context) error
	ProposeDEKVersionPrune(ctx context.Context, gen uint32) error
}

// dekPostCommitDispatcher handles post-commit hooks related to DEK lifecycle.
// It decodes ConfigPut payloads for DEK-related keys and dispatches proposals
// asynchronously (via goroutines) so the FSM apply loop is never blocked.
type dekPostCommitDispatcher struct {
	proposer     dekProposer
	keeper       *encrypt.DEKKeeper
	scrubberKick func(ctx context.Context, oldGen uint32)
}

// Handle implements cluster.PostCommitHook. It is called from the FSM apply
// goroutine after a command commits; it MUST NOT block. All side-effectful
// work (proposals, scrubber kicks) is dispatched in goroutines.
func (d *dekPostCommitDispatcher) Handle(cmdType clusterpb.MetaCmdType, payload []byte) {
	switch cmdType {
	case clusterpb.MetaCmdTypeConfigPut:
		d.handleConfigPut(payload)
	case clusterpb.MetaCmdTypeDEKRotate:
		d.handleDEKRotate()
	}
}

func (d *dekPostCommitDispatcher) handleConfigPut(payload []byte) {
	if d.proposer == nil {
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

func (d *dekPostCommitDispatcher) handleDEKRotate() {
	if d.scrubberKick == nil || d.keeper == nil {
		return
	}
	newGen, _ := d.keeper.Active()
	if newGen < 1 {
		return
	}
	oldGen := newGen - 1
	go d.scrubberKick(context.Background(), oldGen)
}

// wireDEKPostCommit constructs a dekPostCommitDispatcher and registers it as a
// post-commit hook on fsm. The scrubberKick parameter may be nil (used in §1
// before the storage-layer adapter is implemented).
//
// NOTE: This function exists for future use. It is NOT wired into the real
// server startup in §1; that wiring happens when the runtime is assembled in a
// later section.
func wireDEKPostCommit(
	fsm *cluster.MetaFSM,
	proposer dekProposer,
	keeper *encrypt.DEKKeeper,
	scrubberKick func(ctx context.Context, oldGen uint32),
) {
	d := &dekPostCommitDispatcher{
		proposer:     proposer,
		keeper:       keeper,
		scrubberKick: scrubberKick,
	}
	fsm.RegisterPostCommit(d.Handle)
}
