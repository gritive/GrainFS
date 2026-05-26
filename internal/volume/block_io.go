package volume

import (
	"context"
	"io"

	"github.com/gritive/GrainFS/internal/metrics/readamp"
	"github.com/gritive/GrainFS/internal/storage"
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
	n, err := partial.ReadAt(ctx, bucket, key, offset, buf)
	if err != nil {
		return n, false
	}
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

		phyKey := physicalKey(name, blkNum, liveMap)

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

		if e.cache == nil && e.objects.PreferReadAt(volumeBucketName) {
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
	pl := blockIOPlanner{objects: e.objects}
	ex := blockIOExecutor{objects: e.objects, cache: e.cache, getBlkBuf: e.getBlkBuf, putBlkBuf: e.putBlkBuf}
	actions, err := pl.planWrite(name, vol, p, off, liveMap, currentAllocatedBytes, poolQuota, false)
	if err != nil {
		return blockIOResult{}, err
	}
	return ex.executeWrite(context.Background(), name, vol, p, liveMap, actions)
}

func (e blockIOEngine) writeDeferred(name string, vol *Volume, p []byte, off int64, liveMap map[int64]string) (blockIOResult, error) {
	pl := blockIOPlanner{objects: e.objects}
	ex := blockIOExecutor{objects: e.objects, cache: e.cache, deferred: e.deferred, getBlkBuf: e.getBlkBuf, putBlkBuf: e.putBlkBuf}
	actions, err := pl.planWrite(name, vol, p, off, liveMap, 0, 0, true)
	if err != nil {
		return blockIOResult{}, err
	}
	return ex.executeWrite(context.Background(), name, vol, p, liveMap, actions)
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
