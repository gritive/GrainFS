package cluster

import (
	"context"
	"strconv"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

// fsmValueRewrapMaxBatch is the default maximum number of keys per local
// rewrite transaction. Bounded to keep one Badger write txn small.
const fsmValueRewrapMaxBatch = 256

// fsmValueRewrapMaxBytes is the default maximum accumulated raw value bytes per
// local rewrite batch. Object-meta values can be large; capping by bytes
// prevents a single batch from exceeding the badger txn budget. 4 MiB is well
// below badger's default limit and leaves ample room for the overhead per entry.
const fsmValueRewrapMaxBytes = 4 << 20 // 4 MiB

// fsmValueRewrapAfterProposeHook, set only by tests, fires after each local
// batch inside DrainFSMValueRewrap. It lets a test advance the keeper's
// active gen MID-DRAIN (between iterations) to reproduce the back-to-back-
// rotation livelock. The name is legacy from the retired raft-propose path.
// nil in production.
var fsmValueRewrapAfterProposeHook func()

// CollectStaleFSMValueKeys read-only-scans this group's policy: and obj: values
// and returns up to maxBatch FULL storage keys (and up to maxBytes of accumulated
// raw value bytes) whose DEK frame gen != activeGen.
//
// Multipart (mpu:) is intentionally excluded (D4 census-only). The keys are full
// storage keys (group-prefixed) so the local rewrite can txn.Get them and the
// AAD matches what sealValue/openValue use.
//
// The scan stops at whichever bound is hit first: len(stale) >= maxBatch OR
// accumulated raw bytes >= maxBytes. This prevents a single local batch from
// exceeding the badger write-txn budget (ErrTxnTooBig).
func (b *DistributedBackend) CollectStaleFSMValueKeys(activeGen uint32, maxBatch int, maxBytes int) ([]string, error) {
	var stale []string
	var accBytes int
	err := b.store.View(func(txn MetadataTxn) error {
		scan := func(prefix string) error {
			return b.ks().scanGroupPrefix(txn, []byte(prefix), func(_ []byte, item MetaItem) error {
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

// RewrapLocalFSMValues reseals up to maxBatch stale policy:/obj: FSM values in
// this node's local group store without proposing through data-group raft. It
// reuses CollectStaleFSMValueKeys for bounded scanning, then re-reads each
// collected key inside the write transaction so a live active-generation write
// that wins after the scan is never clobbered by stale scan-time data.
func RewrapLocalFSMValues(ctx context.Context, gb *GroupBackend, maxBatch int) (int, error) {
	if maxBatch <= 0 {
		maxBatch = fsmValueRewrapMaxBatch
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	current, ok := gb.fsm.activeDEKGen()
	if !ok {
		return 0, nil
	}
	keys, err := gb.CollectStaleFSMValueKeys(current, maxBatch, fsmValueRewrapMaxBytes)
	if err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, nil
	}
	return gb.rewrapLocalFSMValueKeys(keys)
}

func (b *DistributedBackend) rewrapLocalFSMValueKeys(keys []string) (int, error) {
	var rewrapped int
	err := b.store.Update(func(txn MetadataTxn) error {
		de := b.fsm.dataEncryptor()
		if de == nil {
			return nil
		}
		current, ok := b.fsm.activeDEKGen()
		if !ok {
			return nil
		}
		for _, k := range keys {
			changed, err := b.rewrapLocalFSMValueKey(txn, de, current, []byte(k))
			if err != nil {
				return err
			}
			if changed {
				rewrapped++
				RewrapFSMValuesTotal.WithLabelValues(strconv.FormatUint(uint64(current), 10)).Inc()
			}
		}
		return nil
	})
	return rewrapped, err
}

func (b *DistributedBackend) rewrapLocalFSMValueKey(txn MetadataTxn, de storage.DataEncryptor, current uint32, key []byte) (bool, error) {
	item, err := txn.Get(key)
	if err == ErrMetaKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	raw, err := item.ValueCopy(nil)
	if err != nil {
		return false, err
	}
	gen, ct, frameOK, derr := decodeFSMValueFrameV2(raw)
	if derr != nil {
		return false, derr
	}
	if !frameOK || gen == current {
		return false, nil
	}
	plain, err := de.Open(encrypt.DomainFSMValue, []encrypt.AADField{encrypt.FieldBytes(key)}, gen, ct)
	if err != nil {
		return false, err
	}
	if err := b.fsm.setValue(txn, key, plain); err != nil {
		return false, err
	}
	return true, nil
}

// DrainFSMValueRewrap reseals every stale policy:/obj: value in gb onto the
// KEEPER-CURRENT active gen via node-local transactions. It reads keeper-current
// at the top of each iteration and scans against that value — it does NOT pin to
// a fixed gen. This is what makes it converge under back-to-back rotations: a
// 2nd rotation landing mid-drain simply shifts the convergence target on the
// next iteration. Live writes (already at current) are never collected, so the
// stale set strictly shrinks toward "all-current" and the loop terminates.
//
// Re-scans each iteration (no cursor); each scan observes the prior batch's
// local reseals. Idempotent. The function name is retained for existing callers
// and tests, but it no longer proposes through data-group raft.
//
// maxBatch, when <= 0, uses fsmValueRewrapMaxBatch. The byte budget is always
// fsmValueRewrapMaxBytes (not exposed as a parameter; callers use the default).
func DrainFSMValueRewrap(ctx context.Context, gb *GroupBackend, maxBatch int) error {
	if maxBatch <= 0 {
		maxBatch = fsmValueRewrapMaxBatch
	}
	for {
		rewrapped, err := RewrapLocalFSMValues(ctx, gb, maxBatch)
		if err != nil {
			return err
		}
		if rewrapped == 0 {
			return nil // drained (all values at keeper-current gen)
		}
		if fsmValueRewrapAfterProposeHook != nil {
			fsmValueRewrapAfterProposeHook()
		}
	}
}
