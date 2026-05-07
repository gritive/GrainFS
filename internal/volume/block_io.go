package volume

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/metrics/readamp"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume/dedup"
)

type blockObjectStore interface {
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error)
	PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error)
	PreferReadAt(bucket string) bool
	PreferWriteAt(bucket string) bool
	ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, bool)
	WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*storage.Object, bool, error)
}

type backendBlockObjectStore struct {
	backend storage.Backend
}

func (s backendBlockObjectStore) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	return s.backend.GetObject(ctx, bucket, key)
}

func (s backendBlockObjectStore) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	return s.backend.PutObject(ctx, bucket, key, r, contentType)
}

func (s backendBlockObjectStore) DeleteObject(ctx context.Context, bucket, key string) error {
	return s.backend.DeleteObject(ctx, bucket, key)
}

func (s backendBlockObjectStore) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	return s.backend.HeadObject(ctx, bucket, key)
}

func (s backendBlockObjectStore) PreferReadAt(bucket string) bool {
	return backendPrefersReadAt(s.backend, bucket)
}

func (s backendBlockObjectStore) PreferWriteAt(bucket string) bool {
	return backendPrefersWriteAt(s.backend, bucket)
}

func (s backendBlockObjectStore) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, bool) {
	partial, ok := s.backend.(storage.PartialIO)
	if !ok {
		return 0, false
	}
	n, _ := partial.ReadAt(ctx, bucket, key, offset, buf)
	return n, true
}

func (s backendBlockObjectStore) WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*storage.Object, bool, error) {
	partial, ok := s.backend.(storage.PartialIO)
	if !ok {
		return nil, false, nil
	}
	obj, err := partial.WriteAt(ctx, bucket, key, offset, data)
	return obj, true, err
}

type blockDedupIndex interface {
	WriteBlock(vol string, blkNum int64, hash [32]byte, newKey string) (dedup.WriteResult, error)
	ReadBlock(vol string, blkNum int64) (canonical string, found bool, err error)
	FreeBlock(vol string, blkNum int64) (objectKey string, shouldDelete bool, err error)
}

type blockCache interface {
	Get(key string) ([]byte, bool)
	Put(key string, data []byte)
	Invalidate(key string)
}

type blockReadMeter interface {
	RecordVolumeBlock(key string)
}

type defaultBlockReadMeter struct{}

func (defaultBlockReadMeter) RecordVolumeBlock(key string) {
	readamp.RecordVolumeBlock(key)
}

type blockDeferredWriter interface {
	PutObjectAsync(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, func() error, error)
}

type blockIOEngine struct {
	objects   blockObjectStore
	dedup     blockDedupIndex
	cache     blockCache
	meter     blockReadMeter
	deferred  blockDeferredWriter
	getBlkBuf func(int) []byte
	putBlkBuf func([]byte)
}

type blockIOResult struct {
	Bytes                int
	AllocationBytesDelta int64
	InvalidatedKeys      []string
	LiveMapDirty         bool
	CommitFns            []func() error
}

func (e blockIOEngine) read(name string, vol *Volume, p []byte, off int64, liveMap map[int64]string) (blockIOResult, error) {
	bs := int64(vol.BlockSize)
	var result blockIOResult

	for result.Bytes < len(p) && off+int64(result.Bytes) < vol.Size {
		pos := off + int64(result.Bytes)
		blkNum := pos / bs
		blkOff := pos % bs

		var phyKey string
		if e.dedup != nil {
			var err error
			phyKey, _, err = e.dedup.ReadBlock(name, blkNum)
			if err != nil {
				return result, fmt.Errorf("dedup read block %d: %w", blkNum, err)
			}
		} else {
			phyKey = physicalKey(name, blkNum, liveMap)
		}

		e.meter.RecordVolumeBlock(phyKey)

		canRead := int(bs - blkOff)
		remaining := len(p) - result.Bytes
		if canRead > remaining {
			canRead = remaining
		}
		if off+int64(result.Bytes)+int64(canRead) > vol.Size {
			canRead = int(vol.Size - pos)
		}

		if e.cache != nil {
			if cached, ok := e.cache.Get(phyKey); ok {
				src := cached[blkOff:]
				if len(src) < canRead {
					copy(p[result.Bytes:result.Bytes+len(src)], src)
					clear(p[result.Bytes+len(src) : result.Bytes+canRead])
				} else {
					copy(p[result.Bytes:result.Bytes+canRead], src[:canRead])
				}
				result.Bytes += canRead
				continue
			}
		}

		if e.dedup == nil && e.cache == nil && e.objects.PreferReadAt(volumeBucketName) {
			dst := p[result.Bytes : result.Bytes+canRead]
			if n, ok := e.objects.ReadAt(context.Background(), volumeBucketName, phyKey, blkOff, dst); ok {
				if n < canRead {
					clear(dst[n:])
				}
				result.Bytes += canRead
				continue
			}
		}

		blkData := e.getBlkBuf(vol.BlockSize)
		rc, _, err := e.objects.GetObject(context.Background(), volumeBucketName, phyKey)
		if err == nil {
			n, _ := io.ReadFull(rc, blkData)
			rc.Close()
			if n < vol.BlockSize {
				clear(blkData[n:])
			}
			if e.cache != nil {
				e.cache.Put(phyKey, blkData)
			}
		}
		copy(p[result.Bytes:result.Bytes+canRead], blkData[blkOff:int(blkOff)+canRead])
		e.putBlkBuf(blkData)
		result.Bytes += canRead
	}

	return result, nil
}

func (e blockIOEngine) write(name string, vol *Volume, p []byte, off int64, liveMap map[int64]string, currentAllocatedBytes, poolQuota int64) (blockIOResult, error) {
	bs := int64(vol.BlockSize)
	useCow := e.dedup == nil && vol.SnapshotCount > 0

	if poolQuota > 0 {
		newBlocksNeeded := int64(0)
		firstBlk := off / bs
		lastBlk := (off + int64(len(p)) - 1) / bs
		if lastBlk >= vol.Size/bs {
			lastBlk = vol.Size/bs - 1
		}
		for blkNum := firstBlk; blkNum <= lastBlk; blkNum++ {
			var existErr error
			if e.dedup != nil {
				_, found, err := e.dedup.ReadBlock(name, blkNum)
				if err != nil {
					existErr = err
				} else if !found {
					existErr = fmt.Errorf("not found")
				}
			} else {
				_, existErr = e.objects.HeadObject(context.Background(), volumeBucketName, physicalKey(name, blkNum, liveMap))
			}
			if existErr != nil {
				newBlocksNeeded++
			}
		}

		if currentAllocatedBytes+newBlocksNeeded*bs > poolQuota {
			return blockIOResult{}, ErrPoolQuotaExceeded
		}
	}

	var result blockIOResult
	var newBlocks int64

	for result.Bytes < len(p) && off+int64(result.Bytes) < vol.Size {
		pos := off + int64(result.Bytes)
		blkNum := pos / bs
		blkOff := pos % bs

		canWrite := int(bs - blkOff)
		remaining := len(p) - result.Bytes
		if canWrite > remaining {
			canWrite = remaining
		}
		endPos := off + int64(result.Bytes) + int64(canWrite)
		if endPos > vol.Size {
			canWrite = int(vol.Size - pos)
		}

		isFullBlock := e.dedup == nil && !useCow && blkOff == 0 && canWrite == int(bs)

		var blkData []byte
		if !isFullBlock {
			blkData = e.getBlkBuf(vol.BlockSize)
		}
		var isNew bool
		var oldKey string
		if e.dedup != nil {
			canonical, found, rErr := e.dedup.ReadBlock(name, blkNum)
			if rErr != nil {
				e.putBlkBuf(blkData)
				return result, fmt.Errorf("read dedup block %d: %w", blkNum, rErr)
			}
			if found {
				rc, _, readErr := e.objects.GetObject(context.Background(), volumeBucketName, canonical)
				if readErr == nil {
					if _, err := io.ReadFull(rc, blkData); err != nil {
						_ = rc.Close()
						e.putBlkBuf(blkData)
						return result, fmt.Errorf("read block %d: %w", blkNum, err)
					}
					if err := rc.Close(); err != nil {
						e.putBlkBuf(blkData)
						return result, fmt.Errorf("close block %d: %w", blkNum, err)
					}
				}
				result.InvalidatedKeys = append(result.InvalidatedKeys, canonical)
			} else {
				isNew = true
			}
		} else {
			oldKey = physicalKey(name, blkNum, liveMap)
			if isFullBlock {
				_, headErr := e.objects.HeadObject(context.Background(), volumeBucketName, oldKey)
				isNew = headErr != nil
			} else {
				rc, _, readErr := e.objects.GetObject(context.Background(), volumeBucketName, oldKey)
				isNew = readErr != nil
				if !isNew {
					if _, err := io.ReadFull(rc, blkData); err != nil {
						_ = rc.Close()
						e.putBlkBuf(blkData)
						return result, fmt.Errorf("read block %d: %w", blkNum, err)
					}
					if err := rc.Close(); err != nil {
						e.putBlkBuf(blkData)
						return result, fmt.Errorf("close block %d: %w", blkNum, err)
					}
				}
			}
			result.InvalidatedKeys = append(result.InvalidatedKeys, oldKey)
		}

		if !isFullBlock {
			copy(blkData[blkOff:blkOff+int64(canWrite)], p[result.Bytes:result.Bytes+canWrite])
		}

		var targetKey string
		if e.dedup != nil {
			newKey := fmt.Sprintf("%s%s/blk_%012d_v%s", metaPrefix, name, blkNum, uuid.Must(uuid.NewV7()).String())
			hash := sha256.Sum256(blkData)
			res, dedupErr := e.dedup.WriteBlock(name, blkNum, hash, newKey)
			if dedupErr != nil {
				e.putBlkBuf(blkData)
				return result, fmt.Errorf("dedup block %d: %w", blkNum, dedupErr)
			}
			targetKey = res.Canonical
			if res.IsNew {
				if _, err := e.objects.PutObject(context.Background(), volumeBucketName, targetKey,
					bytes.NewReader(blkData), "application/octet-stream"); err != nil {
					e.putBlkBuf(blkData)
					return result, fmt.Errorf("write block %d: %w", blkNum, err)
				}
			}
			if res.ToDelete != "" {
				e.objects.DeleteObject(context.Background(), volumeBucketName, res.ToDelete) //nolint:errcheck
				newBlocks--
			}
			if res.IsNew {
				newBlocks++
			}
		} else if useCow {
			targetKey = cowBlockKey(name, blkNum)
			if _, err := e.objects.PutObject(context.Background(), volumeBucketName, targetKey,
				bytes.NewReader(blkData), "application/octet-stream"); err != nil {
				e.putBlkBuf(blkData)
				return result, fmt.Errorf("write block %d: %w", blkNum, err)
			}
			if oldKey != targetKey {
				if !isNew {
					e.objects.DeleteObject(context.Background(), volumeBucketName, oldKey) //nolint:errcheck
				}
				liveMap[blkNum] = targetKey
				result.LiveMapDirty = true
			}
		} else {
			targetKey = blockKey(name, blkNum)
			if isFullBlock && e.objects.PreferWriteAt(volumeBucketName) {
				if _, ok, err := e.objects.WriteAt(context.Background(), volumeBucketName, targetKey, 0, p[result.Bytes:result.Bytes+canWrite]); ok {
					if err != nil {
						e.putBlkBuf(blkData)
						return result, fmt.Errorf("write block %d: %w", blkNum, err)
					}
				} else if _, err := e.objects.PutObject(context.Background(), volumeBucketName, targetKey, bytes.NewReader(p[result.Bytes:result.Bytes+canWrite]), "application/octet-stream"); err != nil {
					e.putBlkBuf(blkData)
					return result, fmt.Errorf("write block %d: %w", blkNum, err)
				}
			} else {
				src := bytes.NewReader(blkData)
				if isFullBlock {
					src = bytes.NewReader(p[result.Bytes : result.Bytes+canWrite])
				}
				if _, err := e.objects.PutObject(context.Background(), volumeBucketName, targetKey, src, "application/octet-stream"); err != nil {
					e.putBlkBuf(blkData)
					return result, fmt.Errorf("write block %d: %w", blkNum, err)
				}
			}
		}

		if e.dedup == nil && isNew {
			newBlocks++
		}
		if targetKey != "" {
			result.InvalidatedKeys = append(result.InvalidatedKeys, targetKey)
		}
		e.putBlkBuf(blkData)
		result.Bytes += canWrite
	}

	e.invalidate(result.InvalidatedKeys)
	result.AllocationBytesDelta = newBlocks * bs
	return result, nil
}

func (e blockIOEngine) writeDeferred(name string, vol *Volume, p []byte, off int64, liveMap map[int64]string) (blockIOResult, error) {
	bs := int64(vol.BlockSize)
	var result blockIOResult
	var newBlocks int64

	for result.Bytes < len(p) && off+int64(result.Bytes) < vol.Size {
		pos := off + int64(result.Bytes)
		blkNum := pos / bs
		blkOff := pos % bs

		canWrite := int(bs - blkOff)
		if rem := len(p) - result.Bytes; canWrite > rem {
			canWrite = rem
		}
		if end := off + int64(result.Bytes) + int64(canWrite); end > vol.Size {
			canWrite = int(vol.Size - pos)
		}

		isFullBlock := blkOff == 0 && canWrite == int(bs)
		oldKey := physicalKey(name, blkNum, liveMap)
		targetKey := blockKey(name, blkNum)
		result.InvalidatedKeys = append(result.InvalidatedKeys, oldKey, targetKey)

		var blkSrc io.Reader
		if isFullBlock {
			_, headErr := e.objects.HeadObject(context.Background(), volumeBucketName, oldKey)
			if headErr != nil {
				newBlocks++
			}
			blkSrc = bytes.NewReader(p[result.Bytes : result.Bytes+canWrite])
		} else {
			blkData := e.getBlkBuf(vol.BlockSize)
			rc, _, readErr := e.objects.GetObject(context.Background(), volumeBucketName, oldKey)
			if readErr != nil {
				newBlocks++
			} else {
				io.ReadFull(rc, blkData) //nolint:errcheck
				rc.Close()
			}
			copy(blkData[blkOff:blkOff+int64(canWrite)], p[result.Bytes:result.Bytes+canWrite])
			blkSrc = bytes.NewReader(blkData)
			_, commitFn, putErr := e.deferred.PutObjectAsync(context.Background(), volumeBucketName, targetKey, blkSrc, "application/octet-stream")
			e.putBlkBuf(blkData)
			if putErr != nil {
				return result, fmt.Errorf("write block %d: %w", blkNum, putErr)
			}
			result.CommitFns = append(result.CommitFns, commitFn)
			result.Bytes += canWrite
			continue
		}

		if isFullBlock && e.objects.PreferWriteAt(volumeBucketName) {
			if _, ok, err := e.objects.WriteAt(context.Background(), volumeBucketName, targetKey, 0, p[result.Bytes:result.Bytes+canWrite]); ok {
				if err != nil {
					return result, fmt.Errorf("write block %d: %w", blkNum, err)
				}
			} else {
				_, commitFn, putErr := e.deferred.PutObjectAsync(context.Background(), volumeBucketName, targetKey, bytes.NewReader(p[result.Bytes:result.Bytes+canWrite]), "application/octet-stream")
				if putErr != nil {
					return result, fmt.Errorf("write block %d: %w", blkNum, putErr)
				}
				result.CommitFns = append(result.CommitFns, commitFn)
			}
			result.Bytes += canWrite
			continue
		}

		_, commitFn, putErr := e.deferred.PutObjectAsync(context.Background(), volumeBucketName, targetKey, blkSrc, "application/octet-stream")
		if putErr != nil {
			return result, fmt.Errorf("write block %d: %w", blkNum, putErr)
		}
		result.CommitFns = append(result.CommitFns, commitFn)
		result.Bytes += canWrite
	}

	e.invalidate(result.InvalidatedKeys)
	result.AllocationBytesDelta = newBlocks * bs
	return result, nil
}

func (e blockIOEngine) discard(name string, vol *Volume, off, length int64, liveMap map[int64]string) (blockIOResult, error) {
	bs := int64(vol.BlockSize)
	firstBlock := (off + bs - 1) / bs
	lastBlock := (off+length)/bs - 1

	if lastBlock < firstBlock {
		return blockIOResult{}, nil
	}

	var result blockIOResult
	var freed int64
	for blkNum := firstBlock; blkNum <= lastBlock; blkNum++ {
		if e.dedup != nil {
			objectKey, shouldDelete, freeErr := e.dedup.FreeBlock(name, blkNum)
			if freeErr != nil {
				return result, fmt.Errorf("dedup free block %d: %w", blkNum, freeErr)
			}
			result.InvalidatedKeys = append(result.InvalidatedKeys, objectKey)
			if shouldDelete {
				freed++
				e.objects.DeleteObject(context.Background(), volumeBucketName, objectKey) //nolint:errcheck
			}
		} else {
			key := physicalKey(name, blkNum, liveMap)
			result.InvalidatedKeys = append(result.InvalidatedKeys, key)
			if err := e.objects.DeleteObject(context.Background(), volumeBucketName, key); err == nil {
				freed++
				if liveMap != nil {
					delete(liveMap, blkNum)
					result.LiveMapDirty = true
				}
			}
		}
	}

	e.invalidate(result.InvalidatedKeys)
	result.AllocationBytesDelta = -freed * bs
	return result, nil
}

func (e blockIOEngine) invalidate(keys []string) {
	if e.cache == nil {
		return
	}
	for _, k := range keys {
		e.cache.Invalidate(k)
	}
}
