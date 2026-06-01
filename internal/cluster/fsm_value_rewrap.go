package cluster

import (
	"context"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
)

// fsmValueRewrapMaxBatch is the default maximum number of keys per
// CmdResealFSMValues command. Bounded to keep one apply txn small.
const fsmValueRewrapMaxBatch = 256

// fsmValueRewrapMaxBytes is the default maximum accumulated raw value bytes per
// CmdResealFSMValues batch. Object-meta values can be large; capping by bytes
// prevents a single batch from exceeding the badger txn budget. 4 MiB is well
// below badger's default limit and leaves ample room for the overhead per entry.
const fsmValueRewrapMaxBytes = 4 << 20 // 4 MiB

// fsmValueRewrapAfterProposeHook, set only by tests, fires after each batch is
// proposed inside DrainFSMValueRewrap. It lets a test advance the keeper's
// active gen MID-DRAIN (between iterations) to reproduce the back-to-back-
// rotation livelock. nil in production.
var fsmValueRewrapAfterProposeHook func()

// CollectStaleFSMValueKeys read-only-scans this group's policy: and obj: values
// and returns up to maxBatch FULL storage keys (and up to maxBytes of accumulated
// raw value bytes) whose DEK frame gen != activeGen.
//
// Multipart (mpu:) is intentionally excluded (D4 census-only). The keys are full
// storage keys (group-prefixed) so applyResealFSMValues can txn.Get them and the
// AAD matches what sealValue/openValue use.
//
// The scan stops at whichever bound is hit first: len(stale) >= maxBatch OR
// accumulated raw bytes >= maxBytes. This prevents a single CmdResealFSMValues
// from exceeding the badger apply-txn budget (ErrTxnTooBig).
func (b *DistributedBackend) CollectStaleFSMValueKeys(activeGen uint32, maxBatch int, maxBytes int) ([]string, error) {
	var stale []string
	var accBytes int
	err := b.db.View(func(txn *badger.Txn) error {
		scan := func(prefix string) error {
			return b.ks().scanGroupPrefix(txn, []byte(prefix), func(_ []byte, item *badger.Item) error {
				// Pre-check count cap before reading the value.
				if len(stale) >= maxBatch {
					return errStopScan
				}
				raw, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				gen, _, ok, derr := decodeFSMValueFrameV2(raw)
				if derr != nil {
					return derr
				}
				if ok && gen != activeGen {
					// Pre-check byte-budget: if adding this item would exceed the
					// budget, stop before adding it. This keeps the propose txn
					// strictly under the badger limit.
					if accBytes > 0 && accBytes+len(raw) > maxBytes {
						return errStopScan
					}
					stale = append(stale, string(item.KeyCopy(nil))) // FULL key (AAD-correct)
					accBytes += len(raw)
				}
				return nil
			})
		}
		if err := scan("policy:"); err != nil {
			return err
		}
		// Skip obj: scan if either cap already reached from policy: pass.
		if len(stale) >= maxBatch || accBytes >= maxBytes {
			return nil
		}
		return scan("obj:")
	})
	return stale, err
}

// DrainFSMValueRewrap, run on the group LEADER, reseals every stale policy:/obj:
// value in gb onto the KEEPER-CURRENT active gen by proposing batched
// CmdResealFSMValues until no stale key remains. It reads keeper-current at the
// TOP of each iteration and scans/proposes against that value — it does NOT pin
// to a fixed gen. This is what makes it converge under back-to-back rotations:
// a 2nd rotation landing mid-drain simply shifts the convergence target on the
// next iteration. Live writes (already at current) are never collected, so the
// stale set strictly shrinks toward "all-current" and the loop terminates.
//
// Re-scans each iteration (no cursor); propose waits for apply, so each scan
// observes the prior batch's reseals. Idempotent.
//
// maxBatch, when <= 0, uses fsmValueRewrapMaxBatch. The byte budget is always
// fsmValueRewrapMaxBytes (not exposed as a parameter; callers use the default).
func DrainFSMValueRewrap(ctx context.Context, gb *GroupBackend, maxBatch int) error {
	if maxBatch <= 0 {
		maxBatch = fsmValueRewrapMaxBatch
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Track keeper-current each iteration — never a fixed gen.
		current, ok := gb.fsm.activeDEKGen()
		if !ok {
			return nil // encryption disabled — nothing to reseal
		}
		keys, err := gb.CollectStaleFSMValueKeys(current, maxBatch, fsmValueRewrapMaxBytes)
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil // drained (all values at keeper-current gen)
		}
		if err := gb.ProposeResealFSMValues(ctx, keys, current); err != nil {
			log.Warn().Err(err).Uint32("active_gen", current).
				Msg("fsm-value rewrap: propose failed; will retry on next trigger")
			return err // surface error; the rotation post-commit will re-trigger
		}
		if fsmValueRewrapAfterProposeHook != nil {
			fsmValueRewrapAfterProposeHook()
		}
	}
}
