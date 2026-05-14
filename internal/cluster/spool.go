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

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

const spoolCopyBufferSize = 1 << 20
const maxEncryptedSpoolBlobBytes = 2 * spoolCopyBufferSize

var spoolCopyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, spoolCopyBufferSize)
		return &buf
	},
}

type spooledObject struct {
	Path      string
	Size      int64
	ETag      string
	encrypted bool
	encryptor *encrypt.Encryptor
	domain    string
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

func spoolObjectEncrypted(ctx context.Context, dir string, r io.Reader, bucket string, enc *encrypt.Encryptor, domain string) (*spooledObject, error) {
	if enc == nil {
		return nil, fmt.Errorf("encrypt spool object: nil encryptor")
	}
	if domain == "" {
		return nil, fmt.Errorf("encrypt spool object: empty domain")
	}
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
	writer := &encryptedSpoolRecordWriter{w: tmp, enc: enc, domain: domain}
	hashWriter, etagFunc, releaseHash := spoolHashForBucket(bucket)
	defer releaseHash()
	stageStart = time.Now()
	for {
		n, readErr := reader.Read(*bufp)
		if n > 0 {
			plain := (*bufp)[:n]
			if hashWriter != nil {
				_, _ = hashWriter.Write(plain)
			}
			if _, err := writer.Write(plain); err != nil {
				cleanup()
				return nil, fmt.Errorf("spool object: %w", err)
			}
			size += int64(n)
			clear(plain)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			cleanup()
			return nil, fmt.Errorf("spool object: %w", readErr)
		}
	}
	if etagFunc != nil {
		etag = etagFunc()
	}
	observePutStage("spool_object", "copy_hash", stageStart)
	stageStart = time.Now()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("close spool file: %w", err)
	}
	observePutStage("spool_object", "close", stageStart)
	return &spooledObject{
		Path:      path,
		Size:      size,
		ETag:      etag,
		encrypted: true,
		encryptor: enc,
		domain:    domain,
	}, nil
}

func (s *spooledObject) Open() (io.ReadCloser, error) {
	if s.encrypted {
		return openSpoolEncryptedRecordFile(s.Path, s.encryptor, s.domain)
	}
	return os.Open(s.Path)
}

func (s *spooledObject) Cleanup() {
	_ = os.Remove(s.Path)
}

func (b *DistributedBackend) spoolPutObject(ctx context.Context, bucket string, r io.Reader) (*spooledObject, error) {
	if b.shardSvc != nil && b.shardSvc.encryptor != nil {
		domain := fmt.Sprintf("cluster-spool:%d", time.Now().UnixNano())
		return spoolObjectEncrypted(ctx, b.spoolDir(), r, bucket, b.shardSvc.encryptor, domain)
	}
	return spoolObject(ctx, b.spoolDir(), r, bucket)
}

func spoolHashForBucket(bucket string) (io.Writer, func() string, func()) {
	if bucket == "" {
		return nil, nil, func() {}
	}
	if storage.IsInternalBucket(bucket) {
		xh := storage.GetXXH3Hasher()
		return xh, func() string {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], xh.Sum64())
			return hex.EncodeToString(buf[:])
		}, func() { storage.PutXXH3Hasher(xh) }
	}
	h := md5.New()
	return h, func() string { return hex.EncodeToString(h.Sum(nil)) }, func() {}
}

type encryptedSpoolRecordWriter struct {
	w      io.Writer
	enc    *encrypt.Encryptor
	domain string
	record uint64
}

func (w *encryptedSpoolRecordWriter) Write(p []byte) (int, error) {
	if uint64(len(p)) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("encrypted spool record too large: %d", len(p))
	}
	blob, err := w.enc.SealValue(spoolEncryptedRecordAAD(w.domain, w.record), p)
	if err != nil {
		return 0, err
	}
	if uint64(len(blob)) > uint64(^uint32(0)) {
		clear(blob)
		return 0, fmt.Errorf("encrypted spool blob too large: %d", len(blob))
	}
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(p)))
	binary.BigEndian.PutUint32(header[4:], uint32(len(blob)))
	if _, err := w.w.Write(header[:]); err != nil {
		clear(blob)
		return 0, err
	}
	if _, err := w.w.Write(blob); err != nil {
		clear(blob)
		return 0, err
	}
	clear(blob)
	w.record++
	return len(p), nil
}

func openSpoolEncryptedRecordFile(path string, enc *encrypt.Encryptor, domain string) (io.ReadCloser, error) {
	if enc == nil {
		return nil, fmt.Errorf("open encrypted spool: nil encryptor")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &encryptedSpoolRecordReader{
		f:      f,
		enc:    enc,
		domain: domain,
	}, nil
}

type encryptedSpoolRecordReader struct {
	f      *os.File
	enc    *encrypt.Encryptor
	domain string
	record uint64
	buf    []byte
	err    error
}

func (r *encryptedSpoolRecordReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.buf) == 0 && r.err == nil {
		r.err = r.loadNext()
	}
	if len(r.buf) == 0 {
		return 0, r.err
	}
	n := copy(p, r.buf)
	clear(r.buf[:n])
	r.buf = r.buf[n:]
	return n, nil
}

func (r *encryptedSpoolRecordReader) Close() error {
	if len(r.buf) > 0 {
		clear(r.buf)
	}
	return r.f.Close()
}

func (r *encryptedSpoolRecordReader) loadNext() error {
	plain, done, err := readSpoolEncryptedRecord(r.f, r.enc, r.domain, r.record)
	if err != nil {
		return err
	}
	if done {
		return io.EOF
	}
	r.record++
	r.buf = plain
	return nil
}

func readSpoolEncryptedRecord(r io.Reader, enc *encrypt.Encryptor, domain string, record uint64) ([]byte, bool, error) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if err == io.EOF {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("read encrypted spool header: %w", err)
	}
	plainLen := binary.BigEndian.Uint32(header[:4])
	blobLen := binary.BigEndian.Uint32(header[4:])
	if blobLen == 0 {
		return nil, false, fmt.Errorf("read encrypted spool record: empty blob")
	}
	if blobLen > maxEncryptedSpoolBlobBytes {
		return nil, false, fmt.Errorf("read encrypted spool record: blob too large: %d", blobLen)
	}
	blob := make([]byte, blobLen)
	if _, err := io.ReadFull(r, blob); err != nil {
		return nil, false, fmt.Errorf("read encrypted spool blob: %w", err)
	}
	plain, err := enc.OpenValue(spoolEncryptedRecordAAD(domain, record), blob)
	clear(blob)
	if err != nil {
		return nil, false, fmt.Errorf("open encrypted spool record: %w", err)
	}
	if len(plain) != int(plainLen) {
		clear(plain)
		return nil, false, fmt.Errorf("open encrypted spool record: plaintext size mismatch")
	}
	return plain, false, nil
}

func spoolEncryptedRecordAAD(domain string, record uint64) string {
	return fmt.Sprintf("%s:%d", domain, record)
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
