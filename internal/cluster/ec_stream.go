package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/directio"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
	"github.com/klauspost/reedsolomon"
	"golang.org/x/sync/errgroup"
)

var ecSpoolCounter uint64

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
	// finalFormat is true when paths point at files whose on-disk bytes are
	// byte-identical to the final shard format (GFSENC2 with final AAD).
	// writeShardReadersWithSize uses this to choose rename-vs-copy for local
	// shards.
	finalFormat bool
	aadBases    [][]byte
	// extraCleanupDirs are spool subdirs that should be removed on Cleanup
	// after the files inside are gone. Only populated when finalFormat is
	// true (per-drive tmp subdir per object).
	extraCleanupDirs []string
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

// spoolECShardsToFinalInMemoryCap is the upper bound for the in-memory EC
// split path. Objects at or below this size run Split + Encode in memory,
// which avoids the streaming spool's data-shard re-read + re-decrypt pass
// during parity computation. Memory cost is bounded by source size +
// (k+m)/k × source size; at 32 MiB the worst case is ~96 MiB per object,
// well below single-node RSS targets at concurrent = 32.
const spoolECShardsToFinalInMemoryCap = 32 << 20

// spoolECShardsToFinal is the C optimization: per-drive spool paths sealed
// with the final shard AAD (bucket/shardKey/idx), so writeShardReadersWithSize
// can rename each spool file straight into its final shard slot without
// decrypting and re-encoding. Requires an encryptor and a non-empty dataDirs
// list. Plaintext shards still take the legacy path (rename optimisation is
// possible but the unencrypted shard format adds a CRC footer that the
// streaming writer doesn't yet emit; this commit only optimises the
// at-rest-encrypted default).
//
// For payloads at or below spoolECShardsToFinalInMemoryCap the function
// dispatches to spoolECShardsToFinalMemory which performs Split + Encode in
// memory (one encrypt pass per shard, no disk re-read for parity). Larger
// payloads fall through to the streaming path below.
func spoolECShardsToFinal(
	ctx context.Context,
	cfg ECConfig,
	dataDirs []string,
	bucket, shardKey string,
	sp *spooledObject,
) (*spooledECShards, error) {
	if sp == nil || sp.encryptor == nil {
		return nil, fmt.Errorf("spoolECShardsToFinal: nil encryptor")
	}
	if len(dataDirs) == 0 {
		return nil, fmt.Errorf("spoolECShardsToFinal: empty dataDirs")
	}
	if sp.Size > 0 && sp.Size <= spoolECShardsToFinalInMemoryCap {
		return spoolECShardsToFinalMemory(ctx, cfg, dataDirs, bucket, shardKey, sp)
	}
	stageStart := time.Now()
	enc, err := reedsolomon.NewStream(
		cfg.DataShards,
		cfg.ParityShards,
		reedsolomon.WithStreamBlockSize(ecStreamBlockSize(cfg, sp.Size)),
	)
	if err != nil {
		return nil, fmt.Errorf("ec stream encoder: %w", err)
	}
	out := &spooledECShards{
		paths:       make([]string, cfg.NumShards()),
		sizes:       make([]int64, cfg.NumShards()),
		origSize:    sp.Size,
		encrypted:   true,
		encryptor:   sp.encryptor,
		domains:     make([]string, cfg.NumShards()),
		finalFormat: true,
		aadBases:    make([][]byte, cfg.NumShards()),
	}
	// Per-object spool subdir on each drive: dataDirs[i%len(dataDirs)]/tmp/.ec-spool-<stamp>
	stamp := time.Now().UnixNano()
	cnt := atomic.AddUint64(&ecSpoolCounter, 1)
	subdir := fmt.Sprintf(".ec-spool-%d-%d", stamp, cnt)
	allocSpool := func(idx int) (path string, err error) {
		drive := dataDirs[idx%len(dataDirs)]
		dir := filepath.Join(drive, "tmp", subdir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("ec spool mkdir %s: %w", dir, err)
		}
		out.extraCleanupDirs = append(out.extraCleanupDirs, dir)
		return filepath.Join(dir, fmt.Sprintf("shard_%d", idx)), nil
	}

	cleanup := func() {
		out.Cleanup()
	}

	// All-empty object: still emit the GFSENC2 header so consumers can detect
	// the format. Close on an empty writer writes the header alone.
	if sp.Size == 0 {
		for i := 0; i < cfg.NumShards(); i++ {
			path, err := allocSpool(i)
			if err != nil {
				cleanup()
				return nil, err
			}
			out.paths[i] = path
			out.aadBases[i] = []byte(bucket + "/" + shardKey + "/" + strconv.Itoa(i))
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				cleanup()
				return nil, fmt.Errorf("create empty ec shard: %w", err)
			}
			w, err := eccodec.NewEncryptedShardChunkedWriter(f, sp.encryptor, out.aadBases[i], eccodec.DefaultEncryptedChunkSize)
			if err != nil {
				_ = f.Close()
				cleanup()
				return nil, fmt.Errorf("create empty shard writer: %w", err)
			}
			if err := w.Close(); err != nil {
				_ = f.Close()
				cleanup()
				return nil, fmt.Errorf("close empty shard writer: %w", err)
			}
			if err := f.Close(); err != nil {
				cleanup()
				return nil, fmt.Errorf("close empty shard file: %w", err)
			}
		}
		return out, nil
	}

	// Final shard format includes an 8-byte origSize header at the front
	// of each shard's plaintext (see encodeShardHeader). writeLocalShard's
	// legacy path stamps this header via readers + io.MultiReader; on the
	// rename-direct path we have to prepend it explicitly so the byte stream
	// the chunked writer sees matches what EncodeEncryptedShard would have
	// produced for the same shard.
	shardHeader := encodeShardHeader(sp.Size)
	dataFiles := make([]*os.File, cfg.DataShards)
	dataChunked := make([]*eccodec.EncryptedShardChunkedWriter, cfg.DataShards)
	dataWriters := make([]io.Writer, cfg.DataShards)
	for i := range dataFiles {
		path, err := allocSpool(i)
		if err != nil {
			cleanup()
			return nil, err
		}
		out.paths[i] = path
		out.aadBases[i] = []byte(bucket + "/" + shardKey + "/" + strconv.Itoa(i))
		f, ferr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if ferr != nil {
			cleanup()
			return nil, fmt.Errorf("create ec data shard %d: %w", i, ferr)
		}
		dataFiles[i] = f
		cw, werr := eccodec.NewEncryptedShardChunkedWriter(f, sp.encryptor, out.aadBases[i], eccodec.DefaultEncryptedChunkSize)
		if werr != nil {
			_ = f.Close()
			cleanup()
			return nil, fmt.Errorf("create ec data shard writer %d: %w", i, werr)
		}
		if _, err := cw.Write(shardHeader[:]); err != nil {
			_ = cw.Close()
			_ = f.Close()
			cleanup()
			return nil, fmt.Errorf("write ec data shard header %d: %w", i, err)
		}
		dataChunked[i] = cw
		dataWriters[i] = &countingWriter{w: cw, n: &out.sizes[i]}
	}
	observePutStage("ec_spool_shards_final", "create_data_files", stageStart)

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
	observePutStage("ec_spool_shards_final", "split", stageStart)

	// Close chunked writers (flushes partial last chunk into encrypted shard)
	// and the underlying files. Stage ordering: chunked-writer Close must
	// happen before the os.File Close so the trailing chunk lands in the file.
	stageStart = time.Now()
	for i, cw := range dataChunked {
		if err := cw.Close(); err != nil {
			cleanup()
			return nil, fmt.Errorf("close ec data shard writer %d: %w", i, err)
		}
		if err := dataFiles[i].Close(); err != nil {
			cleanup()
			return nil, fmt.Errorf("close ec data shard %d: %w", i, err)
		}
		// Update size to reflect the encrypted file on disk (final shard
		// header + chunk frames), which the downstream Import path needs to
		// know in order to surface it to placement metadata.
		if info, statErr := os.Stat(out.paths[i]); statErr == nil {
			out.sizes[i] = info.Size()
		}
	}
	observePutStage("ec_spool_shards_final", "close_data_files", stageStart)

	// Re-open data shards in DECRYPTED form so reedsolomon.Encode sees the
	// same plaintext data bytes it produced during Split. Each shard's
	// decrypted plaintext is (8-byte origSize header || EC-split bytes); the
	// legacy Encode path expects PURE EC-split bytes (no header) so we
	// skip the header on read. Parity will be computed over the data bytes
	// only, identical to legacy behaviour.
	stageStart = time.Now()
	dataReaders := make([]io.Reader, cfg.DataShards)
	dataReadClosers := make([]io.Closer, cfg.DataShards)
	closeAllOnErr := func() {
		for _, rc := range dataReadClosers {
			if rc != nil {
				_ = rc.Close()
			}
		}
	}
	for i := 0; i < cfg.DataShards; i++ {
		f, err := os.Open(out.paths[i])
		if err != nil {
			closeAllOnErr()
			cleanup()
			return nil, fmt.Errorf("open ec data shard for encode: %w", err)
		}
		dr, err := eccodec.NewEncryptedShardReader(f, sp.encryptor, out.aadBases[i])
		if err != nil {
			_ = f.Close()
			closeAllOnErr()
			cleanup()
			return nil, fmt.Errorf("decrypt ec data shard for encode: %w", err)
		}
		// Skip the 8-byte shard header so the EC encoder sees pure data.
		var hdr [shardHeaderSize]byte
		if _, err := io.ReadFull(dr, hdr[:]); err != nil {
			_ = f.Close()
			closeAllOnErr()
			cleanup()
			return nil, fmt.Errorf("read ec data shard header: %w", err)
		}
		dataReadClosers[i] = f
		dataReaders[i] = dr
	}
	defer closeAllOnErr()

	parityFiles := make([]*os.File, cfg.ParityShards)
	parityChunked := make([]*eccodec.EncryptedShardChunkedWriter, cfg.ParityShards)
	parityWriters := make([]io.Writer, cfg.ParityShards)
	for i := range parityWriters {
		idx := cfg.DataShards + i
		path, err := allocSpool(idx)
		if err != nil {
			cleanup()
			return nil, err
		}
		out.paths[idx] = path
		out.aadBases[idx] = []byte(bucket + "/" + shardKey + "/" + strconv.Itoa(idx))
		f, ferr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if ferr != nil {
			cleanup()
			return nil, fmt.Errorf("create ec parity shard %d: %w", i, ferr)
		}
		parityFiles[i] = f
		cw, werr := eccodec.NewEncryptedShardChunkedWriter(f, sp.encryptor, out.aadBases[idx], eccodec.DefaultEncryptedChunkSize)
		if werr != nil {
			_ = f.Close()
			cleanup()
			return nil, fmt.Errorf("create ec parity shard writer %d: %w", i, werr)
		}
		// Match data-shard plaintext layout: 8-byte origSize header || parity bytes.
		// This keeps the final on-disk format consistent across all shards so the
		// reader can use a single decode path.
		if _, err := cw.Write(shardHeader[:]); err != nil {
			_ = cw.Close()
			_ = f.Close()
			cleanup()
			return nil, fmt.Errorf("write ec parity shard header %d: %w", i, err)
		}
		parityChunked[i] = cw
		parityWriters[i] = &countingWriter{w: cw, n: &out.sizes[idx]}
	}
	observePutStage("ec_spool_shards_final", "open_data_create_parity", stageStart)

	stageStart = time.Now()
	if err := enc.Encode(dataReaders, parityWriters); err != nil {
		cleanup()
		return nil, fmt.Errorf("ec encode stream: %w", err)
	}
	observePutStage("ec_spool_shards_final", "encode", stageStart)

	stageStart = time.Now()
	for i, cw := range parityChunked {
		if err := cw.Close(); err != nil {
			cleanup()
			return nil, fmt.Errorf("close ec parity shard writer %d: %w", i, err)
		}
		if err := parityFiles[i].Close(); err != nil {
			cleanup()
			return nil, fmt.Errorf("close ec parity shard %d: %w", i, err)
		}
		idx := cfg.DataShards + i
		if info, statErr := os.Stat(out.paths[idx]); statErr == nil {
			out.sizes[idx] = info.Size()
		}
	}
	observePutStage("ec_spool_shards_final", "close_parity_files", stageStart)
	return out, nil
}

// spoolECShardsToFinalMemory is the in-memory variant of the C-path. It
// reads the entire spooled source into RAM, calls ecSplitBodies to compute
// both data and parity shards in one pass (no disk re-read), then writes
// each shard's plaintext (8-byte shardHeader || shard bytes) through the
// encrypted chunked writer into its per-drive spool path. The result is
// identical to spoolECShardsToFinal's streaming output: paths in final
// shard format, sealed with final AAD, finalFormat = true.
//
// Saves one full encrypt-pass and one disk-read-pass per data shard vs the
// streaming path. At 5 MiB / EC 2+2 that's 2 × 1.25 MiB AES-GCM + ~5 MiB
// read avoided per object. Memory cost is bounded by spoolECShardsToFinalInMemoryCap.
func spoolECShardsToFinalMemory(
	ctx context.Context,
	cfg ECConfig,
	dataDirs []string,
	bucket, shardKey string,
	sp *spooledObject,
) (*spooledECShards, error) {
	stageStart := time.Now()
	src, err := sp.Open()
	if err != nil {
		return nil, fmt.Errorf("open spooled object: %w", err)
	}
	plaintext := make([]byte, sp.Size)
	if _, err := io.ReadFull(src, plaintext); err != nil {
		_ = src.Close()
		return nil, fmt.Errorf("read spooled object: %w", err)
	}
	if err := src.Close(); err != nil {
		return nil, fmt.Errorf("close spooled object: %w", err)
	}
	observePutStage("ec_spool_shards_final_mem", "read_source", stageStart)

	stageStart = time.Now()
	shards, err := ecSplitBodies(cfg, plaintext)
	if err != nil {
		clear(plaintext)
		return nil, fmt.Errorf("ec in-memory split+encode: %w", err)
	}
	// IMPORTANT: reedsolomon.Encoder.Split aliases the input buffer for the
	// data shards (shards[0..k-1] are sub-slices of plaintext), so the
	// plaintext buffer MUST stay alive until we've written every shard
	// payload to disk below. Zero it out at the end of the function via a
	// defer instead of here.
	defer clear(plaintext)
	observePutStage("ec_spool_shards_final_mem", "split_encode", stageStart)

	out := &spooledECShards{
		paths:       make([]string, cfg.NumShards()),
		sizes:       make([]int64, cfg.NumShards()),
		origSize:    sp.Size,
		encrypted:   true,
		encryptor:   sp.encryptor,
		domains:     make([]string, cfg.NumShards()),
		finalFormat: true,
		aadBases:    make([][]byte, cfg.NumShards()),
	}
	stamp := time.Now().UnixNano()
	cnt := atomic.AddUint64(&ecSpoolCounter, 1)
	subdir := fmt.Sprintf(".ec-spool-%d-%d", stamp, cnt)
	cleanup := func() {
		out.Cleanup()
	}

	// Pre-create unique spool directories to optimize os.MkdirAll on APFS
	uniqueDirs := make(map[string]bool)
	for i := 0; i < cfg.NumShards(); i++ {
		drive := dataDirs[i%len(dataDirs)]
		dir := filepath.Join(drive, "tmp", subdir)
		uniqueDirs[dir] = true
	}
	for dir := range uniqueDirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			cleanup()
			return nil, fmt.Errorf("ec spool mkdir %s: %w", dir, err)
		}
		out.extraCleanupDirs = append(out.extraCleanupDirs, dir)
	}

	shardHeader := encodeShardHeader(sp.Size)
	stageStart = time.Now()

	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < cfg.NumShards(); i++ {
		i := i
		g.Go(func() error {
			drive := dataDirs[i%len(dataDirs)]
			dir := filepath.Join(drive, "tmp", subdir)
			path := filepath.Join(dir, fmt.Sprintf("shard_%d", i))
			out.paths[i] = path
			out.aadBases[i] = []byte(bucket + "/" + shardKey + "/" + strconv.Itoa(i))

			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				return fmt.Errorf("create ec shard %d: %w", i, err)
			}
			// macOS F_NOCACHE / Linux no-op. Bypasses the unified buffer
			// cache so the chunked encrypted writer's many small
			// fs-cached writes don't fight the page reclaimer for the
			// shard bytes — fsync then has nothing to flush. Linux falls
			// through (O_DIRECT would EINVAL on the chunked writer's
			// unaligned writes); a Linux-only optimisation would need
			// to consolidate the chunks into one aligned buffer first.
			_ = directio.ApplyNoCacheHint(f)
			cw, err := eccodec.NewEncryptedShardChunkedWriter(f, sp.encryptor, out.aadBases[i], eccodec.DefaultEncryptedChunkSize)
			if err != nil {
				_ = f.Close()
				return fmt.Errorf("create chunked writer %d: %w", i, err)
			}
			if _, err := cw.Write(shardHeader[:]); err != nil {
				_ = cw.Close()
				_ = f.Close()
				return fmt.Errorf("write shard header %d: %w", i, err)
			}
			if _, err := cw.Write(shards[i]); err != nil {
				_ = cw.Close()
				_ = f.Close()
				return fmt.Errorf("write shard body %d: %w", i, err)
			}
			if err := cw.Close(); err != nil {
				_ = f.Close()
				return fmt.Errorf("close chunked writer %d: %w", i, err)
			}
			if err := f.Close(); err != nil {
				return fmt.Errorf("close shard file %d: %w", i, err)
			}
			clear(shards[i])
			if info, statErr := os.Stat(path); statErr == nil {
				out.sizes[i] = info.Size()
			}
			return nil
		})
		_ = gctx // keep compiler happy if unused
	}

	if err := g.Wait(); err != nil {
		cleanup()
		return nil, err
	}
	observePutStage("ec_spool_shards_final_mem", "write_shards", stageStart)
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
	if s.finalFormat {
		// finalFormat spool files are GFSENC2 encryption of (8-byte header ||
		// EC bytes) under the final AAD. Decrypting returns the same
		// plaintext byte stream the legacy path produced (header || payload)
		// so remote writeLocalShard can re-encrypt for the receiving node.
		f, err := os.Open(s.paths[idx])
		if err != nil {
			return nil, err
		}
		dr, err := eccodec.NewEncryptedShardReader(f, s.encryptor, s.aadBases[idx])
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		return &multiReadCloser{Reader: dr, close: f.Close}, nil
	}
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
	// Per-drive spool subdirs created by spoolECShardsToFinal. Best-effort:
	// rmdir succeeds only when the dir is empty (all shard files already
	// removed above OR renamed away by importLocalShardFromPath).
	for _, dir := range s.extraCleanupDirs {
		_ = os.Remove(dir)
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
