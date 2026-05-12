package volume

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// blockIOPlanner computes a per-call write plan without performing any mutations.
// It performs read-only I/O (HeadObject, dedup.ReadBlock) to determine the
// action kind and key layout for each block, then validates pool quota.
type blockIOPlanner struct {
	objects blockObjectStore
	dedup   blockDedupIndex
}

// planWrite builds a []BlockAction for a WriteAt call.
// asyncEligible=true sets Async=true on ActionDirect actions (write-back NBD path).
// Returns ErrPoolQuotaExceeded when poolQuota > 0 and the projected allocation
// would exceed it. No object mutations are performed.
func (pl blockIOPlanner) planWrite(
	name string,
	vol *Volume,
	p []byte,
	off int64,
	liveMap map[int64]string,
	currentAllocatedBytes, poolQuota int64,
	asyncEligible bool,
) ([]BlockAction, error) {
	bs := int64(vol.BlockSize)
	useCow := pl.dedup == nil && vol.SnapshotCount > 0

	actions := make([]BlockAction, 0, (int64(len(p))+bs-1)/bs)
	newBlocks := int64(0)
	written := 0

	for written < len(p) && off+int64(written) < vol.Size {
		pos := off + int64(written)
		blkNum := pos / bs
		blkOff := pos % bs

		canWrite := int(bs - blkOff)
		if rem := len(p) - written; canWrite > rem {
			canWrite = rem
		}
		if end := off + int64(written) + int64(canWrite); end > vol.Size {
			canWrite = int(vol.Size - pos)
		}

		action := BlockAction{
			BlkNum:    blkNum,
			BlkOff:    blkOff,
			DataStart: written,
			CanWrite:  canWrite,
		}

		switch {
		case pl.dedup != nil:
			canonical, found, err := pl.dedup.ReadBlock(name, blkNum)
			if err != nil {
				return nil, fmt.Errorf("dedup read block %d: %w", blkNum, err)
			}
			action.Kind = ActionDedup
			action.Key = fmt.Sprintf("%s%s/blk_%012d_v%s", metaPrefix, name, blkNum, uuid.Must(uuid.NewV7()).String())
			if found {
				action.OldKey = canonical
			} else {
				action.IsNew = true
				newBlocks++
			}

		case useCow:
			oldKey := physicalKey(name, blkNum, liveMap)
			_, headErr := pl.objects.HeadObject(context.Background(), volumeBucketName, oldKey)
			action.Kind = ActionCow
			action.Key = cowBlockKey(name, blkNum)
			action.OldKey = oldKey
			action.IsNew = headErr != nil
			if action.IsNew {
				newBlocks++
			}

		default: // ActionDirect
			key := blockKey(name, blkNum)
			_, headErr := pl.objects.HeadObject(context.Background(), volumeBucketName, key)
			action.Kind = ActionDirect
			action.Key = key
			action.OldKey = key
			action.IsNew = headErr != nil
			action.Async = asyncEligible
			if action.IsNew {
				newBlocks++
			}
		}

		actions = append(actions, action)
		written += canWrite
	}

	if poolQuota > 0 && currentAllocatedBytes+newBlocks*bs > poolQuota {
		return nil, ErrPoolQuotaExceeded
	}
	return actions, nil
}
