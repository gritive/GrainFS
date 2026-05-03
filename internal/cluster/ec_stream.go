package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/reedsolomon"
)

type spooledECShards struct {
	paths    []string
	origSize int64
}

func spoolECShards(ctx context.Context, cfg ECConfig, dir string, sp *spooledObject) (*spooledECShards, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create ec spool dir: %w", err)
	}
	enc, err := reedsolomon.NewStream(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("ec stream encoder: %w", err)
	}
	out := &spooledECShards{
		paths:    make([]string, cfg.NumShards()),
		origSize: sp.Size,
	}
	cleanup := func() {
		out.Cleanup()
	}
	if sp.Size == 0 {
		for i := range out.paths {
			f, err := os.CreateTemp(dir, fmt.Sprintf(".ec-empty-%d-*", i))
			if err != nil {
				cleanup()
				return nil, fmt.Errorf("create empty ec shard: %w", err)
			}
			out.paths[i] = f.Name()
			if err := f.Close(); err != nil {
				cleanup()
				return nil, fmt.Errorf("close empty ec shard: %w", err)
			}
		}
		return out, nil
	}

	dataFiles := make([]*os.File, cfg.DataShards)
	dataWriters := make([]io.Writer, cfg.DataShards)
	for i := range dataFiles {
		f, err := os.CreateTemp(dir, fmt.Sprintf(".ec-data-%d-*", i))
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("create ec data shard: %w", err)
		}
		out.paths[i] = f.Name()
		dataFiles[i] = f
		dataWriters[i] = f
	}

	src, err := sp.Open()
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("open spooled object: %w", err)
	}
	if err := enc.Split(readerWithContext{ctx: ctx, r: src}, dataWriters, sp.Size); err != nil {
		_ = src.Close()
		cleanup()
		return nil, fmt.Errorf("ec split stream: %w", err)
	}
	if err := src.Close(); err != nil {
		cleanup()
		return nil, fmt.Errorf("close spooled object: %w", err)
	}
	for _, f := range dataFiles {
		if err := f.Close(); err != nil {
			cleanup()
			return nil, fmt.Errorf("close ec data shard: %w", err)
		}
	}

	dataReaders := make([]io.Reader, cfg.DataShards)
	dataReadFiles := make([]*os.File, cfg.DataShards)
	for i, path := range out.paths[:cfg.DataShards] {
		f, err := os.Open(path)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("open ec data shard: %w", err)
		}
		dataReadFiles[i] = f
		dataReaders[i] = f
	}
	defer func() {
		for _, f := range dataReadFiles {
			_ = f.Close()
		}
	}()

	parityFiles := make([]*os.File, cfg.ParityShards)
	parityWriters := make([]io.Writer, cfg.ParityShards)
	for i := range parityWriters {
		idx := cfg.DataShards + i
		f, err := os.CreateTemp(dir, fmt.Sprintf(".ec-parity-%d-*", i))
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("create ec parity shard: %w", err)
		}
		out.paths[idx] = f.Name()
		parityFiles[i] = f
		parityWriters[i] = f
	}
	if err := enc.Encode(dataReaders, parityWriters); err != nil {
		cleanup()
		return nil, fmt.Errorf("ec encode stream: %w", err)
	}
	for _, f := range parityFiles {
		if err := f.Close(); err != nil {
			cleanup()
			return nil, fmt.Errorf("close ec parity shard: %w", err)
		}
	}
	return out, nil
}

func (s *spooledECShards) OpenShard(idx int) (io.ReadCloser, error) {
	f, err := os.Open(s.paths[idx])
	if err != nil {
		return nil, err
	}
	header := encodeShardHeader(s.origSize)
	return &multiReadCloser{
		Reader: io.MultiReader(bytes.NewReader(header[:]), f),
		close:  f.Close,
	}, nil
}

func (s *spooledECShards) Cleanup() {
	for _, path := range s.paths {
		if path != "" {
			_ = os.Remove(path)
		}
	}
}

func (b *DistributedBackend) ecSpoolDir() string {
	return filepath.Join(b.root, "tmp", "ec-spool")
}

type multiReadCloser struct {
	io.Reader
	close func() error
}

func (r *multiReadCloser) Close() error {
	return r.close()
}
