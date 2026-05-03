package cluster

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/gritive/GrainFS/internal/storage"
)

type spooledObject struct {
	Path string
	Size int64
	ETag string
}

func spoolObject(ctx context.Context, dir string, r io.Reader, hash bool) (*spooledObject, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create spool dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".put-spool-*")
	if err != nil {
		return nil, fmt.Errorf("create spool file: %w", err)
	}
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
	if hash {
		h := md5.New()
		size, err = io.Copy(tmp, io.TeeReader(reader, h))
		etag = hex.EncodeToString(h.Sum(nil))
	} else {
		size, err = io.Copy(tmp, reader)
	}
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("spool object: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("close spool file: %w", err)
	}
	return &spooledObject{Path: path, Size: size, ETag: etag}, nil
}

func (s *spooledObject) Open() (*os.File, error) {
	return os.Open(s.Path)
}

func (s *spooledObject) Cleanup() {
	_ = os.Remove(s.Path)
}

func writeFileAtomicFromReader(path string, r io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create object dir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".object-*")
	if err != nil {
		return fmt.Errorf("create tmp object: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}
	if _, err := io.Copy(tmp, r); err != nil {
		cleanup()
		return fmt.Errorf("write tmp object: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync tmp object: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close tmp object: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename object: %w", err)
	}
	return nil
}

func (b *DistributedBackend) spoolDir() string {
	return filepath.Join(b.root, "tmp", "put-spool")
}

func shouldHashBucket(bucket string) bool {
	return !storage.IsInternalBucket(bucket)
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
