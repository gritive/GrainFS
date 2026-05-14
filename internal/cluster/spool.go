package cluster

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

const spoolCopyBufferSize = 1 << 20

var spoolCopyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, spoolCopyBufferSize)
		return &buf
	},
}

type spooledObject struct {
	Path string
	Size int64
	ETag string
}

func spoolObject(ctx context.Context, dir string, r io.Reader, bucket string) (*spooledObject, error) {
	stageStart := time.Now()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create spool dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".put-spool-*")
	if err != nil {
		return nil, fmt.Errorf("create spool file: %w", err)
	}
	observePutStage("spool_object", "create_temp", stageStart)
	path := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(path)
	}

	var (
		size int64
		etag string
	)
	reader := readerWithContext{ctx: ctx, r: r}
	bufp := spoolCopyBufferPool.Get().(*[]byte)
	defer func() {
		clear(*bufp)
		spoolCopyBufferPool.Put(bufp)
	}()
	stageStart = time.Now()
	if bucket != "" {
		if storage.IsInternalBucket(bucket) {
			xh := storage.GetXXH3Hasher()
			size, err = io.CopyBuffer(tmp, io.TeeReader(reader, xh), *bufp)
			if err == nil {
				var buf [8]byte
				binary.BigEndian.PutUint64(buf[:], xh.Sum64())
				etag = hex.EncodeToString(buf[:])
			}
			storage.PutXXH3Hasher(xh)
		} else {
			h := md5.New()
			size, err = io.CopyBuffer(tmp, io.TeeReader(reader, h), *bufp)
			if err == nil {
				etag = hex.EncodeToString(h.Sum(nil))
			}
		}
	} else {
		size, err = io.CopyBuffer(tmp, reader, *bufp)
	}
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("spool object: %w", err)
	}
	observePutStage("spool_object", "copy_hash", stageStart)
	stageStart = time.Now()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("close spool file: %w", err)
	}
	observePutStage("spool_object", "close", stageStart)
	return &spooledObject{Path: path, Size: size, ETag: etag}, nil
}

func (s *spooledObject) Open() (*os.File, error) {
	return os.Open(s.Path)
}

func (s *spooledObject) Cleanup() {
	_ = os.Remove(s.Path)
}

func writeFileAtomicFromReader(path string, r io.Reader) error {
	stageStart := time.Now()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create object dir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".object-*")
	if err != nil {
		return fmt.Errorf("create tmp object: %w", err)
	}
	observePutStage("write_file_atomic", "create_temp", stageStart)
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}
	stageStart = time.Now()
	bufp := spoolCopyBufferPool.Get().(*[]byte)
	_, err = io.CopyBuffer(tmp, r, *bufp)
	clear(*bufp)
	spoolCopyBufferPool.Put(bufp)
	if err != nil {
		cleanup()
		return fmt.Errorf("write tmp object: %w", err)
	}
	observePutStage("write_file_atomic", "copy", stageStart)
	stageStart = time.Now()
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync tmp object: %w", err)
	}
	observePutStage("write_file_atomic", "sync", stageStart)
	stageStart = time.Now()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close tmp object: %w", err)
	}
	observePutStage("write_file_atomic", "close", stageStart)
	stageStart = time.Now()
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename object: %w", err)
	}
	observePutStage("write_file_atomic", "rename", stageStart)
	return nil
}

func (b *DistributedBackend) spoolDir() string {
	return filepath.Join(b.root, "tmp", "put-spool")
}

type readerWithContext struct {
	ctx context.Context
	r   io.Reader
}

func (r readerWithContext) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.r.Read(p)
	}
}
