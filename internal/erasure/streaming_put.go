package erasure

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/klauspost/reedsolomon"

	"github.com/gritive/GrainFS/internal/storage"
)

// spoolToTemp streams r to a temporary file, computing md5 in one pass.
// Caller must close and remove the returned file when done.
func spoolToTemp(r io.Reader) (f *os.File, size int64, etag string, err error) {
	f, err = os.CreateTemp("", "grainfs-spool-*")
	if err != nil {
		return nil, 0, "", fmt.Errorf("create spool temp: %w", err)
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(f.Name())
			f = nil
		}
	}()

	h := md5.New()
	w := io.MultiWriter(f, h)
	size, err = io.Copy(w, r)
	if err != nil {
		return nil, 0, "", fmt.Errorf("spool body: %w", err)
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, "", fmt.Errorf("seek spool: %w", err)
	}
	return f, size, hex.EncodeToString(h.Sum(nil)), nil
}

// putObjectDataStreaming encodes src (of known size and precomputed etag) using
// streaming Reed-Solomon, keeping heap bounded to ~one shard at a time.
func (b *ECBackend) putObjectDataStreaming(bucket, key string, src io.ReadSeeker, size int64, etag, contentType string) (*storage.Object, error) {
	if size < int64(b.codec.DataShards) {
		// too small for EC — read into memory and use plain path
		data, err := io.ReadAll(src)
		if err != nil {
			return nil, err
		}
		return b.putObjectPlain(bucket, key, data, contentType)
	}

	shardDir := b.ShardDir(bucket, key)
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		return nil, fmt.Errorf("create shard dir: %w", err)
	}

	enc, err := reedsolomon.NewStream(b.codec.DataShards, b.codec.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("create stream encoder: %w", err)
	}

	// Pass 2a: split input into data shard temp files
	dataFiles, err := splitToTempFiles(enc, src, size, b.codec.DataShards, shardDir)
	if err != nil {
		os.RemoveAll(shardDir)
		return nil, err
	}

	// Pass 2b: compute parity shard temp files
	parityFiles, err := computeParityTempFiles(enc, dataFiles, b.codec.ParityShards, shardDir)
	if err != nil {
		for _, f := range dataFiles {
			f.Close()
			os.Remove(f.Name())
		}
		os.RemoveAll(shardDir)
		return nil, err
	}

	// Pass 3: CRC-wrap + atomic-write one shard at a time (bounds heap).
	// Non-encrypted: stream via streamWriteShardCRC (~32KB peak).
	// Encrypted: ReadAll+Encrypt+write (~2×shardSize peak, unavoidable for block cipher).
	allFiles := append(dataFiles, parityFiles...)
	for i, f := range allFiles {
		if _, seekErr := f.Seek(0, io.SeekStart); seekErr != nil {
			f.Close()
			for _, rem := range allFiles[i+1:] {
				rem.Close()
				os.Remove(rem.Name())
			}
			os.RemoveAll(shardDir)
			return nil, fmt.Errorf("seek shard temp %d: %w", i, seekErr)
		}

		path := b.shardPath(bucket, key, i)

		if b.encryptor == nil {
			writeErr := streamWriteShardCRC(path, f)
			f.Close()
			os.Remove(f.Name())
			if writeErr != nil {
				for _, rem := range allFiles[i+1:] {
					rem.Close()
					os.Remove(rem.Name())
				}
				os.RemoveAll(shardDir)
				return nil, fmt.Errorf("write shard %d: %w", i, writeErr)
			}
		} else {
			shardData, readErr := io.ReadAll(f)
			f.Close()
			os.Remove(f.Name())
			if readErr != nil {
				for _, rem := range allFiles[i+1:] {
					rem.Close()
					os.Remove(rem.Name())
				}
				os.RemoveAll(shardDir)
				return nil, fmt.Errorf("read shard temp %d: %w", i, readErr)
			}
			toWrite, encErr := b.encryptor.Encrypt(shardWithCRC(shardData))
			if encErr != nil {
				for _, rem := range allFiles[i+1:] {
					rem.Close()
					os.Remove(rem.Name())
				}
				os.RemoveAll(shardDir)
				return nil, fmt.Errorf("encrypt shard %d: %w", i, encErr)
			}
			if writeErr := atomicWriteShardFile(path, toWrite); writeErr != nil {
				for _, rem := range allFiles[i+1:] {
					rem.Close()
					os.Remove(rem.Name())
				}
				os.RemoveAll(shardDir)
				return nil, fmt.Errorf("write shard %d: %w", i, writeErr)
			}
		}
	}

	shardSz := int((size + int64(b.codec.DataShards) - 1) / int64(b.codec.DataShards))
	now := time.Now().Unix()
	meta := ecObjectMeta{
		Key:          key,
		Size:         size,
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
		DataShards:   b.codec.DataShards,
		ParityShards: b.codec.ParityShards,
		ShardSize:    shardSz,
	}

	metaBytes, err := marshalECObjectMeta(&meta)
	if err != nil {
		return nil, fmt.Errorf("marshal meta: %w", err)
	}
	if err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(objectMetaKey(bucket, key), metaBytes)
	}); err != nil {
		os.RemoveAll(shardDir)
		return nil, err
	}
	return &storage.Object{
		Key:          key,
		Size:         size,
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
	}, nil
}

// splitToTempFiles splits src (size bytes) into dataShards temp files using StreamEncoder.
func splitToTempFiles(enc reedsolomon.StreamEncoder, src io.Reader, size int64, dataShards int, dir string) ([]*os.File, error) {
	files := make([]*os.File, dataShards)
	writers := make([]io.Writer, dataShards)
	for i := range dataShards {
		f, err := os.CreateTemp(dir, fmt.Sprintf(".dshard-%02d-*", i))
		if err != nil {
			for j := range i {
				files[j].Close()
				os.Remove(files[j].Name())
			}
			return nil, fmt.Errorf("create data shard temp %d: %w", i, err)
		}
		files[i] = f
		writers[i] = f
	}
	if err := enc.Split(src, writers, size); err != nil {
		for _, f := range files {
			f.Close()
			os.Remove(f.Name())
		}
		return nil, fmt.Errorf("stream split: %w", err)
	}
	return files, nil
}

// computeParityTempFiles computes parityShards parity files from data shard files.
func computeParityTempFiles(enc reedsolomon.StreamEncoder, dataFiles []*os.File, parityShards int, dir string) ([]*os.File, error) {
	readers := make([]io.Reader, len(dataFiles))
	for i, f := range dataFiles {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek data shard %d: %w", i, err)
		}
		readers[i] = f
	}

	files := make([]*os.File, parityShards)
	writers := make([]io.Writer, parityShards)
	for i := range parityShards {
		f, err := os.CreateTemp(dir, fmt.Sprintf(".pshard-%02d-*", i))
		if err != nil {
			for j := range i {
				files[j].Close()
				os.Remove(files[j].Name())
			}
			return nil, fmt.Errorf("create parity shard temp %d: %w", i, err)
		}
		files[i] = f
		writers[i] = f
	}
	if err := enc.Encode(readers, writers); err != nil {
		for _, f := range files {
			f.Close()
			os.Remove(f.Name())
		}
		return nil, fmt.Errorf("stream encode parity: %w", err)
	}
	return files, nil
}

// streamWriteShardCRC streams src to dst atomically, appending a 4-byte CRC32
// footer (crash-safe: tmp+fsync+rename+dir-fsync). Peak memory: ~32KB io.Copy buf.
func streamWriteShardCRC(dst string, src io.Reader) error {
	dir := filepath.Dir(dst)
	tmp := dst + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	h := crc32.NewIEEE()
	if _, err := io.Copy(io.MultiWriter(f, h), src); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("stream shard data: %w", err)
	}
	var footer [4]byte
	binary.LittleEndian.PutUint32(footer[:], h.Sum32())
	if _, err := f.Write(footer[:]); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write crc footer: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	f.Close()
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return err
	}
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		df.Close()
	}
	return nil
}

// atomicWriteShardFile writes data to path via tmp+fsync+rename (crash-safe).
func atomicWriteShardFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	f.Close()
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		df.Close()
	}
	return nil
}
