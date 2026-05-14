package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/klauspost/reedsolomon"
)

const (
	defaultECStreamBlockSize = 1 << 20
	minECStreamBlockSize     = 64 << 10
)

type spooledECShards struct {
	paths     []string
	sizes     []int64
	origSize  int64
	encrypted bool
	encryptor *encrypt.Encryptor
	domains   []string
}

func spoolECShards(ctx context.Context, cfg ECConfig, dir string, sp *spooledObject) (*spooledECShards, error) {
	stageStart := time.Now()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create ec spool dir: %w", err)
	}
	enc, err := reedsolomon.NewStream(
		cfg.DataShards,
		cfg.ParityShards,
		reedsolomon.WithStreamBlockSize(ecStreamBlockSize(cfg, sp.Size)),
	)
	if err != nil {
		return nil, fmt.Errorf("ec stream encoder: %w", err)
	}
	out := &spooledECShards{
		paths:     make([]string, cfg.NumShards()),
		sizes:     make([]int64, cfg.NumShards()),
		origSize:  sp.Size,
		encrypted: sp.encrypted,
		encryptor: sp.encryptor,
		domains:   make([]string, cfg.NumShards()),
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
			out.domains[i] = ecSpoolShardDomain(i)
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
		out.domains[i] = ecSpoolShardDomain(i)
		dataFiles[i] = f
		var writer io.Writer = f
		if out.encrypted {
			writer = &encryptedSpoolRecordWriter{w: f, enc: out.encryptor, domain: out.domains[i]}
		}
		dataWriters[i] = &countingWriter{w: writer, n: &out.sizes[i]}
	}
	observePutStage("ec_spool_shards", "create_data_files", stageStart)

	stageStart = time.Now()
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
			_ = rc.Close()
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
		out.domains[idx] = ecSpoolShardDomain(idx)
		parityFiles[i] = f
		var writer io.Writer = f
		if out.encrypted {
			writer = &encryptedSpoolRecordWriter{w: f, enc: out.encryptor, domain: out.domains[idx]}
		}
		parityWriters[i] = &countingWriter{w: writer, n: &out.sizes[idx]}
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
	if s.encrypted {
		return openSpoolEncryptedRecordFile(s.paths[idx], s.encryptor, s.domains[idx])
	}
	return os.Open(s.paths[idx])
}

func ecSpoolShardDomain(idx int) string {
	return fmt.Sprintf("cluster-ec-spool:%d:%d", time.Now().UnixNano(), idx)
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
