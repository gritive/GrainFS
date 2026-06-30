package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	defaultECStreamBlockSize = 1 << 20
	minECStreamBlockSize     = 64 << 10
)

type spooledECShards struct {
	paths    []string
	sizes    []int64
	origSize int64
}

// spoolECShardsStream is the source-agnostic EC shard spooler: it reads exactly
// `size` bytes from `src` once (via reedsolomon Split into the data-shard temp
// files) and then encodes parity from those temp files. It never closes `src`;
// the caller owns the reader lifecycle. This lets maintenance paths (relocate,
// coalesce) feed a plain reader + known size directly instead of round-tripping
// through a disk temp-file spool.
func spoolECShardsStream(ctx context.Context, cfg ECConfig, dir string, src io.Reader, size int64) (*spooledECShards, error) {
	stageStart := time.Now()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create ec spool dir: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("set ec spool dir perms: %w", err)
	}
	enc, err := getStreamEncoder(cfg, ecStreamBlockSize(cfg, size))
	if err != nil {
		return nil, fmt.Errorf("ec stream encoder: %w", err)
	}
	out := &spooledECShards{
		paths:    make([]string, cfg.NumShards()),
		sizes:    make([]int64, cfg.NumShards()),
		origSize: size,
	}
	cleanup := func() {
		out.Cleanup()
	}
	if size == 0 {
		for i := range out.paths {
			f, err := os.CreateTemp(dir, fmt.Sprintf("ec-empty-%d-*.tmp", i))
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
	parityFiles := make([]*os.File, cfg.ParityShards)
	defer func() {
		for _, f := range dataFiles {
			if f != nil {
				_ = f.Close()
			}
		}
		for _, f := range parityFiles {
			if f != nil {
				_ = f.Close()
			}
		}
	}()
	dataWriters := make([]io.Writer, cfg.DataShards)
	for i := range dataFiles {
		f, err := os.CreateTemp(dir, fmt.Sprintf("ec-data-%d-*.tmp", i))
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("create ec data shard: %w", err)
		}
		out.paths[i] = f.Name()
		dataFiles[i] = f
		dataWriters[i] = &countingWriter{w: f, n: &out.sizes[i]}
	}
	observePutStage("ec_spool_shards", "create_data_files", stageStart)

	stageStart = time.Now()
	if err := enc.Split(readerWithContext{ctx: ctx, r: src}, dataWriters, size); err != nil {
		cleanup()
		return nil, fmt.Errorf("ec split stream: %w", err)
	}
	observePutStage("ec_spool_shards", "split", stageStart)
	stageStart = time.Now()
	for _, f := range dataFiles {
		if err := f.Close(); err != nil {
			cleanup()
			return nil, fmt.Errorf("close ec data shard: %w", err)
		}
	}
	observePutStage("ec_spool_shards", "close_data_files", stageStart)

	stageStart = time.Now()
	dataReaders := make([]io.Reader, cfg.DataShards)
	dataReadClosers := make([]io.Closer, cfg.DataShards)
	for i := range out.paths[:cfg.DataShards] {
		rc, err := out.openPayload(i)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("open ec data shard: %w", err)
		}
		dataReadClosers[i] = rc
		dataReaders[i] = rc
	}
	defer func() {
		for _, rc := range dataReadClosers {
			if rc != nil {
				_ = rc.Close()
			}
		}
	}()

	parityWriters := make([]io.Writer, cfg.ParityShards)
	for i := range parityWriters {
		idx := cfg.DataShards + i
		f, err := os.CreateTemp(dir, fmt.Sprintf("ec-parity-%d-*.tmp", i))
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("create ec parity shard: %w", err)
		}
		out.paths[idx] = f.Name()
		parityFiles[i] = f
		parityWriters[i] = &countingWriter{w: f, n: &out.sizes[idx]}
	}
	observePutStage("ec_spool_shards", "open_data_create_parity", stageStart)
	stageStart = time.Now()
	if err := enc.Encode(dataReaders, parityWriters); err != nil {
		cleanup()
		return nil, fmt.Errorf("ec encode stream: %w", err)
	}
	observePutStage("ec_spool_shards", "encode", stageStart)
	stageStart = time.Now()
	for _, f := range parityFiles {
		if err := f.Close(); err != nil {
			cleanup()
			return nil, fmt.Errorf("close ec parity shard: %w", err)
		}
	}
	observePutStage("ec_spool_shards", "close_parity_files", stageStart)
	return out, nil
}

func ecStreamBlockSize(cfg ECConfig, objectSize int64) int {
	if objectSize <= 0 || cfg.DataShards <= 0 {
		return minECStreamBlockSize
	}
	perDataShard := (objectSize + int64(cfg.DataShards) - 1) / int64(cfg.DataShards)
	if perDataShard < minECStreamBlockSize {
		return minECStreamBlockSize
	}
	if perDataShard > defaultECStreamBlockSize {
		return defaultECStreamBlockSize
	}
	return int(perDataShard)
}

func (s *spooledECShards) OpenShard(idx int) (io.ReadCloser, error) {
	payload, err := s.openPayload(idx)
	if err != nil {
		return nil, err
	}
	header := encodeShardHeader(s.origSize)
	return &multiReadCloser{
		Reader: io.MultiReader(bytes.NewReader(header[:]), payload),
		close:  payload.Close,
	}, nil
}

func (s *spooledECShards) ShardSize(idx int) (int64, error) {
	if s.sizes != nil {
		return int64(shardHeaderSize) + s.sizes[idx], nil
	}
	info, err := os.Stat(s.paths[idx])
	if err != nil {
		return 0, err
	}
	return int64(shardHeaderSize) + info.Size(), nil
}

func (s *spooledECShards) Cleanup() {
	for _, path := range s.paths {
		if path != "" {
			_ = os.Remove(path)
		}
	}
}

func (s *spooledECShards) openPayload(idx int) (io.ReadCloser, error) {
	return os.Open(s.paths[idx])
}

type countingWriter struct {
	w io.Writer
	n *int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	*w.n += int64(n)
	return n, err
}

type multiReadCloser struct {
	io.Reader
	close func() error
}

func (r *multiReadCloser) Close() error {
	return r.close()
}
