package volume

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog/log"
)

// blockIOExecutor executes a []BlockAction produced by blockIOPlanner.
// It performs all object puts, deletes, cache invalidations, and CommitFn
// collection. It makes no routing decisions.
type blockIOExecutor struct {
	objects   blockObjectStore
	dedup     blockDedupIndex
	cache     blockCache
	deferred  blockDeferredWriter
	getBlkBuf func(int) []byte
	putBlkBuf func([]byte)
}

// executeWrite executes a []BlockAction and returns the mutation result.
// p is the original WriteAt data slice; the executor uses
// action.DataStart and action.CanWrite to slice p without copying.
func (ex blockIOExecutor) executeWrite(
	ctx context.Context,
	name string,
	vol *Volume,
	p []byte,
	liveMap map[int64]string,
	actions []BlockAction,
) (blockIOResult, error) {
	bs := int64(vol.BlockSize)
	var result blockIOResult
	var newBlocks int64

	for i := range actions {
		action := &actions[i]
		isFullBlock := action.BlkOff == 0 && action.CanWrite == int(bs)

		switch action.Kind {
		case ActionDedup:
			if volumeTraceEnabled {
				log.Debug().Int64("block", action.BlkNum).Msg("BlockIO action dedup")
			}
			if err := ex.executeDedupAction(ctx, name, vol, p, action, &result, &newBlocks); err != nil {
				return result, err
			}
		case ActionCow:
			if volumeTraceEnabled {
				log.Debug().Int64("block", action.BlkNum).Msg("BlockIO action cow")
			}
			if err := ex.executeCowAction(ctx, vol, p, action, liveMap, &result, &newBlocks); err != nil {
				return result, err
			}
		case ActionDirect:
			if volumeTraceEnabled {
				log.Debug().Int64("block", action.BlkNum).Bool("async", action.Async).Bool("full_block", isFullBlock).Msg("BlockIO action direct")
			}
			if err := ex.executeDirectAction(ctx, vol, p, action, isFullBlock, &result, &newBlocks); err != nil {
				return result, err
			}
		}

		result.Bytes += action.CanWrite
	}

	ex.invalidateAll(result.InvalidatedKeys)
	result.AllocationBytesDelta = newBlocks * bs
	return result, nil
}

func (ex blockIOExecutor) executeDedupAction(
	ctx context.Context,
	name string,
	vol *Volume,
	p []byte,
	action *BlockAction,
	result *blockIOResult,
	newBlocks *int64,
) error {
	blkData := ex.getBlkBuf(vol.BlockSize)
	defer ex.putBlkBuf(blkData)

	if action.OldKey != "" {
		rc, _, readErr := ex.objects.GetObject(ctx, volumeBucketName, action.OldKey)
		if readErr == nil {
			if _, err := io.ReadFull(rc, blkData); err != nil {
				_ = rc.Close()
				return fmt.Errorf("read block %d: %w", action.BlkNum, err)
			}
			if err := rc.Close(); err != nil {
				return fmt.Errorf("close block %d: %w", action.BlkNum, err)
			}
		}
		result.InvalidatedKeys = append(result.InvalidatedKeys, action.OldKey)
	}

	copy(blkData[action.BlkOff:int(action.BlkOff)+action.CanWrite],
		p[action.DataStart:action.DataStart+action.CanWrite])

	hash := sha256.Sum256(blkData)
	res, err := ex.dedup.WriteBlock(name, action.BlkNum, hash, action.Key)
	if err != nil {
		return fmt.Errorf("dedup block %d: %w", action.BlkNum, err)
	}
	if res.IsNew {
		if _, err := ex.objects.PutObject(ctx, volumeBucketName, res.Canonical,
			bytes.NewReader(blkData), "application/octet-stream"); err != nil {
			return fmt.Errorf("write block %d: %w", action.BlkNum, err)
		}
		*newBlocks++
	}
	if res.ToDelete != "" {
		ex.objects.DeleteObject(ctx, volumeBucketName, res.ToDelete) //nolint:errcheck
		*newBlocks--
	}
	result.InvalidatedKeys = append(result.InvalidatedKeys, res.Canonical)
	return nil
}

func (ex blockIOExecutor) executeCowAction(
	ctx context.Context,
	vol *Volume,
	p []byte,
	action *BlockAction,
	liveMap map[int64]string,
	result *blockIOResult,
	newBlocks *int64,
) error {
	blkData := ex.getBlkBuf(vol.BlockSize)
	defer ex.putBlkBuf(blkData)

	blockFound := false
	if !action.IsNew {
		rc, _, readErr := ex.objects.GetObject(ctx, volumeBucketName, action.OldKey)
		if readErr == nil {
			blockFound = true
			if _, err := io.ReadFull(rc, blkData); err != nil {
				_ = rc.Close()
				return fmt.Errorf("read block %d: %w", action.BlkNum, err)
			}
			if err := rc.Close(); err != nil {
				return fmt.Errorf("close block %d: %w", action.BlkNum, err)
			}
		}
	}

	copy(blkData[action.BlkOff:int(action.BlkOff)+action.CanWrite],
		p[action.DataStart:action.DataStart+action.CanWrite])

	if _, err := ex.objects.PutObject(ctx, volumeBucketName, action.Key,
		bytes.NewReader(blkData), "application/octet-stream"); err != nil {
		return fmt.Errorf("write block %d: %w", action.BlkNum, err)
	}
	isNew := !blockFound // true when planner said IsNew=true (blockFound=false) or GetObject failed
	if action.OldKey != action.Key && !isNew {
		ex.objects.DeleteObject(ctx, volumeBucketName, action.OldKey) //nolint:errcheck
	}
	liveMap[action.BlkNum] = action.Key
	result.LiveMapDirty = true
	if !isNew {
		result.InvalidatedKeys = append(result.InvalidatedKeys, action.OldKey)
	}
	result.InvalidatedKeys = append(result.InvalidatedKeys, action.Key)
	if isNew {
		*newBlocks++
	}
	return nil
}

func (ex blockIOExecutor) executeDirectAction(
	ctx context.Context,
	vol *Volume,
	p []byte,
	action *BlockAction,
	isFullBlock bool,
	result *blockIOResult,
	newBlocks *int64,
) error {
	result.InvalidatedKeys = append(result.InvalidatedKeys, action.Key)

	if action.Async && ex.deferred != nil {
		return ex.executeDirectAsync(ctx, vol, p, action, isFullBlock, result, newBlocks)
	}

	data := p[action.DataStart : action.DataStart+action.CanWrite]

	if isFullBlock {
		preferWriteAt := ex.objects.PreferWriteAt(volumeBucketName)
		if volumeTraceEnabled {
			log.Debug().Bool("prefer_writeat", preferWriteAt).Msg("BlockIO direct full-block write")
		}
		if preferWriteAt {
			tStart := time.Now()
			if _, ok, err := ex.objects.WriteAt(ctx, volumeBucketName, action.Key, 0, data); ok {
				if volumeTraceEnabled {
					log.Debug().Bool("ok", ok).Dur("total", time.Since(tStart)).Msg("BlockIO direct WriteAt")
				}
				if err != nil {
					return fmt.Errorf("write block %d: %w", action.BlkNum, err)
				}
				if action.IsNew {
					*newBlocks++
				}
				return nil
			}
		}
		tStart := time.Now()
		if _, err := ex.objects.PutObject(ctx, volumeBucketName, action.Key,
			bytes.NewReader(data), "application/octet-stream"); err != nil {
			return fmt.Errorf("write block %d: %w", action.BlkNum, err)
		}
		if volumeTraceEnabled {
			log.Debug().Dur("total", time.Since(tStart)).Msg("BlockIO direct PutObject")
		}
	} else {
		blkData := ex.getBlkBuf(vol.BlockSize)
		defer ex.putBlkBuf(blkData)
		rc, _, readErr := ex.objects.GetObject(ctx, volumeBucketName, action.OldKey)
		if readErr == nil {
			if _, err := io.ReadFull(rc, blkData); err != nil {
				_ = rc.Close()
				return fmt.Errorf("read block %d: %w", action.BlkNum, err)
			}
			if err := rc.Close(); err != nil {
				return fmt.Errorf("close block %d: %w", action.BlkNum, err)
			}
		}
		copy(blkData[action.BlkOff:int(action.BlkOff)+action.CanWrite], data)
		if _, err := ex.objects.PutObject(ctx, volumeBucketName, action.Key,
			bytes.NewReader(blkData), "application/octet-stream"); err != nil {
			return fmt.Errorf("write block %d: %w", action.BlkNum, err)
		}
		if readErr != nil {
			*newBlocks++
		}
		return nil
	}
	if action.IsNew {
		*newBlocks++
	}
	return nil
}

func (ex blockIOExecutor) executeDirectAsync(
	ctx context.Context,
	vol *Volume,
	p []byte,
	action *BlockAction,
	isFullBlock bool,
	result *blockIOResult,
	newBlocks *int64,
) error {
	data := p[action.DataStart : action.DataStart+action.CanWrite]

	if isFullBlock {
		preferWriteAt := ex.objects.PreferWriteAt(volumeBucketName)
		if volumeTraceEnabled {
			log.Debug().Bool("prefer_writeat", preferWriteAt).Msg("BlockIO async full-block write")
		}
		if preferWriteAt {
			tStart := time.Now()
			if _, ok, err := ex.objects.WriteAt(ctx, volumeBucketName, action.Key, 0, data); ok {
				if volumeTraceEnabled {
					log.Debug().Bool("ok", ok).Dur("total", time.Since(tStart)).Msg("BlockIO async WriteAt")
				}
				if err != nil {
					return fmt.Errorf("write block %d: %w", action.BlkNum, err)
				}
				if action.IsNew {
					*newBlocks++
				}
				return nil
			}
		}
		_, commitFn, err := ex.deferred.PutObjectAsync(ctx, volumeBucketName, action.Key,
			bytes.NewReader(data), "application/octet-stream")
		if err != nil {
			return fmt.Errorf("write block %d: %w", action.BlkNum, err)
		}
		result.CommitFns = append(result.CommitFns, commitFn)
	} else {
		blkData := ex.getBlkBuf(vol.BlockSize)
		rc, _, readErr := ex.objects.GetObject(ctx, volumeBucketName, action.OldKey)
		if readErr == nil {
			if _, err := io.ReadFull(rc, blkData); err != nil {
				ex.putBlkBuf(blkData)
				_ = rc.Close()
				return fmt.Errorf("read block %d: %w", action.BlkNum, err)
			}
			if err := rc.Close(); err != nil {
				ex.putBlkBuf(blkData)
				return fmt.Errorf("close block %d: %w", action.BlkNum, err)
			}
		}
		copy(blkData[action.BlkOff:int(action.BlkOff)+action.CanWrite], data)
		_, commitFn, err := ex.deferred.PutObjectAsync(ctx, volumeBucketName, action.Key,
			bytes.NewReader(blkData), "application/octet-stream")
		ex.putBlkBuf(blkData)
		if err != nil {
			return fmt.Errorf("write block %d: %w", action.BlkNum, err)
		}
		result.CommitFns = append(result.CommitFns, commitFn)
		if readErr != nil {
			*newBlocks++
		}
		return nil
	}
	if action.IsNew {
		*newBlocks++
	}
	return nil
}

func (ex blockIOExecutor) invalidateAll(keys []string) {
	if ex.cache == nil {
		return
	}
	for _, k := range keys {
		ex.cache.Invalidate(k)
	}
}
