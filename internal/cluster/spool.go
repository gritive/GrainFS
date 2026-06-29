package cluster

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

var md5Pool = sync.Pool{
	New: func() any {
		return md5.New()
	},
}

const spoolCopyBufferSize = 1 << 20
const maxEncryptedSpoolBlobBytes = 2 * spoolCopyBufferSize
const encryptedSpoolCipherBufferSize = spoolCopyBufferSize + 64

var spoolCopyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, spoolCopyBufferSize)
		return &buf
	},
}

var encryptedSpoolCipherBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, encryptedSpoolCipherBufferSize)
		return &buf
	},
}

// copyToSpoolChunked copies src to dst while forcing chunked Writes no
// larger than spoolCopyBufferSize. Callers writing into an encrypted
// spool record stream must use this helper so the receiver-side
// maxEncryptedSpoolBlobBytes invariant cannot be tripped by readers
// that implement WriteTo (e.g. *bytes.Reader) or by upstream HTTP
// frameworks that hand the body in 5 MiB+ slabs.
func copyToSpoolChunked(dst io.Writer, src io.Reader) (int64, error) {
	bp := spoolCopyBufferPool.Get().(*[]byte)
	defer spoolCopyBufferPool.Put(bp)
	type readerOnly struct{ io.Reader }
	return io.CopyBuffer(dst, readerOnly{src}, *bp)
}

type spooledObject struct {
	Path      string
	Size      int64
	ETag      string
	encrypted bool
	seam      storage.DataEncryptor
	domain    string
}

func spoolObject(ctx context.Context, dir string, r io.Reader, bucket string, needsMD5 bool) (*spooledObject, error) {
	stageStart := time.Now()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create spool dir: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("set spool dir perms: %w", err)
	}
	tmp, err := os.CreateTemp(dir, "put-spool-*.tmp")
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
		} else if needsMD5 {
			h := md5Pool.Get().(hash.Hash)
			h.Reset()
			size, err = io.CopyBuffer(tmp, io.TeeReader(reader, h), *bufp)
			if err == nil {
				etag = hex.EncodeToString(h.Sum(nil))
			}
			md5Pool.Put(h)
		} else {
			size, err = io.CopyBuffer(tmp, reader, *bufp)
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

func spoolObjectEncrypted(ctx context.Context, dir string, r io.Reader, bucket string, seam storage.DataEncryptor, domain string, needsMD5 bool) (*spooledObject, error) {
	if seam == nil {
		return nil, fmt.Errorf("encrypt spool object: nil seam")
	}
	if domain == "" {
		return nil, fmt.Errorf("encrypt spool object: empty domain")
	}
	stageStart := time.Now()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create spool dir: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("set spool dir perms: %w", err)
	}
	tmp, err := os.CreateTemp(dir, "put-spool-*.tmp")
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
	writer := &encryptedSpoolRecordWriter{w: tmp, seam: seam, domain: domain}
	hashWriter, etagFunc, releaseHash := spoolHashForBucket(bucket, needsMD5)
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
		seam:      seam,
		domain:    domain,
	}, nil
}

func (s *spooledObject) Open() (io.ReadCloser, error) {
	if s.encrypted {
		return openSpoolEncryptedRecordFile(s.Path, s.seam, s.domain)
	}
	return os.Open(s.Path)
}

func (s *spooledObject) Cleanup() {
	_ = os.Remove(s.Path)
}

func (b *DistributedBackend) spoolPutObject(ctx context.Context, bucket string, r io.Reader, needsMD5 bool) (*spooledObject, error) {
	return spoolObject(ctx, b.spoolDir(), r, bucket, needsMD5)
}

// spoolHashForBucket returns a hash writer, ETag getter, and release func.
// Internal buckets always use XXH3 (ignoring needsMD5). User buckets use MD5
// only when needsMD5 is true (Content-MD5 validation or legacy ETag path).
// Empty bucket and needsMD5=false both skip hashing.
func spoolHashForBucket(bucket string, needsMD5 bool) (io.Writer, func() string, func()) {
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
	if !needsMD5 {
		return nil, nil, func() {}
	}
	h := md5Pool.Get().(hash.Hash)
	h.Reset()
	return h, func() string { return hex.EncodeToString(h.Sum(nil)) }, func() {
		md5Pool.Put(h)
	}
}

type encryptedSpoolRecordWriter struct {
	w         io.Writer
	seam      storage.DataEncryptor
	domain    string
	record    uint64
	cipherBuf []byte // writer-owned ciphertext buffer, reused across records
	aadFields []encrypt.AADField
}

func (w *encryptedSpoolRecordWriter) Write(p []byte) (int, error) {
	if uint64(len(p)) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("encrypted spool record too large: %d", len(p))
	}
	w.aadFields = spoolRecordAADFieldsInto(w.aadFields, w.domain, w.record)
	blob, gen, err := w.seam.SealTo(w.cipherBuf[:0], encrypt.DomainSpool, w.aadFields, p)
	if err != nil {
		return 0, err
	}
	if uint64(len(blob)) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("encrypted spool blob too large: %d", len(blob))
	}
	var header [12]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(p)))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(blob)))
	binary.BigEndian.PutUint32(header[8:], gen)
	if _, err := w.w.Write(header[:]); err != nil {
		clear(blob)
		return 0, err
	}
	if _, err := w.w.Write(blob); err != nil {
		clear(blob)
		return 0, err
	}
	clear(blob)
	w.cipherBuf = blob
	w.record++
	return len(p), nil
}

func openSpoolEncryptedRecordFile(path string, seam storage.DataEncryptor, domain string) (io.ReadCloser, error) {
	if seam == nil {
		return nil, fmt.Errorf("open encrypted spool: nil seam")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	plainRef := spoolCopyBufferPool.Get().(*[]byte)
	cipherRef := encryptedSpoolCipherBufferPool.Get().(*[]byte)
	return &encryptedSpoolRecordReader{
		f:         f,
		seam:      seam,
		domain:    domain,
		plain:     (*plainRef)[:0],
		cipherBuf: (*cipherRef)[:0],
		plainRef:  plainRef,
		cipherRef: cipherRef,
	}, nil
}

type encryptedSpoolRecordReader struct {
	f         *os.File
	seam      storage.DataEncryptor
	domain    string
	record    uint64
	buf       []byte
	plain     []byte // reader-owned plaintext buffer, reused across records
	cipherBuf []byte // reader-owned ciphertext read buffer, reused across records
	plainRef  *[]byte
	cipherRef *[]byte
	aadFields []encrypt.AADField
	err       error
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
	if len(r.plain) > 0 {
		clear(r.plain)
	}
	if len(r.cipherBuf) > 0 {
		clear(r.cipherBuf)
	}
	if r.plainRef != nil {
		clear((*r.plainRef)[:cap(*r.plainRef)])
		spoolCopyBufferPool.Put(r.plainRef)
		r.plainRef = nil
	}
	if r.cipherRef != nil {
		clear((*r.cipherRef)[:cap(*r.cipherRef)])
		encryptedSpoolCipherBufferPool.Put(r.cipherRef)
		r.cipherRef = nil
	}
	return r.f.Close()
}

func (r *encryptedSpoolRecordReader) loadNext() error {
	// Reuse the reader-owned plaintext/ciphertext buffers. Safe because the
	// previous record is fully drained before loadNext is called (Read loops
	// while len(r.buf)==0) and the plaintext only ever exits via Read's copy.
	plain, cipher, aadFields, done, err := readSpoolEncryptedRecord(r.f, r.seam, r.domain, r.record, r.plain[:0], r.cipherBuf[:0], r.aadFields)
	if err != nil {
		return err
	}
	if done {
		return io.EOF
	}
	r.record++
	r.plain = plain
	r.cipherBuf = cipher
	r.aadFields = aadFields
	r.buf = plain
	return nil
}

func readSpoolEncryptedRecord(r io.Reader, seam storage.DataEncryptor, domain string, record uint64, plainDst, cipherDst []byte, aadDst []encrypt.AADField) (plain, cipher []byte, aadFields []encrypt.AADField, done bool, err error) {
	var header [12]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if err == io.EOF {
			return nil, cipherDst, aadDst, true, nil
		}
		return nil, cipherDst, aadDst, false, fmt.Errorf("read encrypted spool header: %w", err)
	}
	plainLen := binary.BigEndian.Uint32(header[:4])
	blobLen := binary.BigEndian.Uint32(header[4:8])
	gen := binary.BigEndian.Uint32(header[8:])
	if blobLen == 0 {
		return nil, cipherDst, aadDst, false, fmt.Errorf("read encrypted spool record: empty blob")
	}
	if blobLen > maxEncryptedSpoolBlobBytes {
		return nil, cipherDst, aadDst, false, fmt.Errorf("read encrypted spool record: blob too large: %d", blobLen)
	}
	if cap(cipherDst) < int(blobLen) {
		cipherDst = make([]byte, blobLen)
	}
	blob := cipherDst[:blobLen]
	if _, err := io.ReadFull(r, blob); err != nil {
		return nil, blob, aadDst, false, fmt.Errorf("read encrypted spool blob: %w", err)
	}
	aadFields = spoolRecordAADFieldsInto(aadDst, domain, record)
	out, err := seam.OpenTo(plainDst[:0], encrypt.DomainSpool, aadFields, gen, blob)
	clear(blob) // zeroize ciphertext after decrypt; buffer is reused next record
	if err != nil {
		// cipher.AEAD.Open may overwrite dst up to capacity even on failure.
		// plainDst is the reader-owned reusable buffer, so wipe its full
		// capacity to leave no unauthenticated plaintext residue (defense in
		// depth: Go's GCM already zeroes on auth failure, but the AEAD
		// contract does not guarantee it).
		clear(plainDst[:cap(plainDst)])
		return nil, blob, aadFields, false, fmt.Errorf("open encrypted spool record: %w", err)
	}
	if len(out) != int(plainLen) {
		clear(out[:cap(out)])
		return nil, blob, aadFields, false, fmt.Errorf("open encrypted spool record: plaintext size mismatch")
	}
	return out, blob, aadFields, false, nil
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
