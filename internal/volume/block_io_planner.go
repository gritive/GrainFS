package volume

import (
	"context"
)

// blockIOPlanner computes a per-call write plan without performing any mutations.
// It performs read-only I/O (HeadObject) to determine the key layout for each
// block, then validates pool quota.
type blockIOPlanner struct {
	objects blockObjectStore
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

		isFullBlk := blkOff == 0 && canWrite == int(bs)
		key := blockKey(name, blkNum)
		action := BlockAction{
			Kind:      ActionDirect,
			BlkNum:    blkNum,
			BlkOff:    blkOff,
			DataStart: written,
			CanWrite:  canWrite,
			Key:       key,
			OldKey:    key,
			Async:     asyncEligible,
		}
		if isFullBlk || poolQuota > 0 {
			_, headErr := pl.objects.HeadObject(context.Background(), volumeBucketName, key)
			action.IsNew = headErr != nil
			if action.IsNew {
				newBlocks++
			}
		}
		// partial without quota: executor determines IsNew via GetObject

		actions = append(actions, action)
		written += canWrite
	}

	if poolQuota > 0 && currentAllocatedBytes+newBlocks*bs > poolQuota {
		return nil, ErrPoolQuotaExceeded
	}
	return actions, nil
}
