// Package eccodec provides reusable shard I/O primitives shared by
// every erasure-coded backend. The current on-disk format is:
//
//	GFSCRC1\0 <payload> <4-byte little-endian CRC32-IEEE footer>
//
// The magic prefix lets cluster-mode readers distinguish a new checksummed
// shard from legacy raw shard bytes during rolling upgrades.
//
// Slice 2 (refactor/unify-storage-paths): eccodec is introduced so Slice 8
// can drop internal/erasure/ while keeping the footer layout consistent. The
// current cluster ShardService writes raw bytes without a footer, so CRC
// verification is wired in through Scrubbable only when upstream switches
// to the eccodec-backed path.
package eccodec

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/metrics"
)

// ErrCRCMismatch is returned when the CRC footer does not match the payload,
// or when a shard is shorter than the 4-byte footer.
var ErrCRCMismatch = errors.New("eccodec: CRC mismatch (bit-rot detected)")

// footerLen is the size of the CRC32 footer appended to every shard.
const footerLen = 4

var shardMagic = []byte("GFSCRC1\x00")
var encryptedShardMagic = []byte("GFSENC2\x00")

var (
	encryptedPlainChunkPool = sync.Pool{New: func() any {
		b := make([]byte, DefaultEncryptedChunkSize)
		return &b
	}}
	encryptedCipherChunkPool = sync.Pool{New: func() any {
		b := make([]byte, 0, DefaultEncryptedChunkSize+32)
		return &b
	}}
)

const (
	DefaultEncryptedChunkSize = 1 << 20
	maxEncryptedChunkSize     = DefaultEncryptedChunkSize
	encryptedNoncePrefixLen   = 8
	encryptedNonceLen         = 12
	encryptedHeaderLen        = 8 + 4 + encryptedNoncePrefixLen
	encryptedChunkHeaderLen   = 8
)

// IsEncodedShard reports whether raw bytes carry the current eccodec magic.
func IsEncodedShard(raw []byte) bool {
	if len(raw) < len(shardMagic) {
		return false
	}
	for i := range shardMagic {
		if raw[i] != shardMagic[i] {
			return false
		}
	}
	return true
}

func IsEncryptedShard(raw []byte) bool {
	if len(raw) < len(encryptedShardMagic) {
		return false
	}
	for i := range encryptedShardMagic {
		if raw[i] != encryptedShardMagic[i] {
			return false
		}
	}
	return true
}

func EncodeEncryptedShard(w io.Writer, r io.Reader, enc *encrypt.Encryptor, aadBase []byte, chunkSize int) error {
	if enc == nil {
		return fmt.Errorf("encrypted shard encode requires encryptor")
	}
	if chunkSize <= 0 {
		chunkSize = DefaultEncryptedChunkSize
	}
	if chunkSize > maxEncryptedChunkSize {
		return fmt.Errorf("encrypted shard chunk size too large: %d", chunkSize)
	}

	var noncePrefix [encryptedNoncePrefixLen]byte
	if _, err := io.ReadFull(rand.Reader, noncePrefix[:]); err != nil {
		return fmt.Errorf("generate nonce prefix: %w", err)
	}

	var header [encryptedHeaderLen]byte
	copy(header[:], encryptedShardMagic)
	binary.LittleEndian.PutUint32(header[len(encryptedShardMagic):], uint32(chunkSize))
	copy(header[len(encryptedShardMagic)+4:], noncePrefix[:])
	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("write encrypted shard header: %w", err)
	}

	plainPtr := encryptedPlainChunkPool.Get().(*[]byte)
	plain := *plainPtr
	if cap(plain) < chunkSize {
		plain = make([]byte, chunkSize)
	}
	plain = plain[:chunkSize]
	defer func() {
		clear(plain)
		*plainPtr = plain
		encryptedPlainChunkPool.Put(plainPtr)
	}()

	cipherPtr := encryptedCipherChunkPool.Get().(*[]byte)
	cipherBuf := *cipherPtr
	if cap(cipherBuf) < chunkSize+enc.AEADOverhead() {
		cipherBuf = make([]byte, 0, chunkSize+enc.AEADOverhead())
	}
	defer func() {
		*cipherPtr = cipherBuf[:0]
		encryptedCipherChunkPool.Put(cipherPtr)
	}()

	chunkIdx := uint32(0)
	for {
		stageStart := time.Now()
		n, readErr := io.ReadFull(r, plain)
		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) && !errors.Is(readErr, io.EOF) {
			return fmt.Errorf("read shard chunk: %w", readErr)
		}
		if n == 0 && errors.Is(readErr, io.EOF) {
			return nil
		}
		observeEncryptedShardStage("read_chunk", stageStart)

		nonce := encryptedChunkNonce(noncePrefix, chunkIdx)
		aad := encryptedChunkAAD(aadBase, chunkIdx)
		stageStart = time.Now()
		ciphertext, err := enc.SealWithNonceAAD(cipherBuf[:0], nonce[:], plain[:n], aad)
		if err != nil {
			return fmt.Errorf("encrypt shard chunk %d: %w", chunkIdx, err)
		}
		observeEncryptedShardStage("seal_chunk", stageStart)

		var chunkHeader [encryptedChunkHeaderLen]byte
		binary.LittleEndian.PutUint32(chunkHeader[0:4], uint32(n))
		binary.LittleEndian.PutUint32(chunkHeader[4:8], uint32(len(ciphertext)))
		stageStart = time.Now()
		if _, err := w.Write(chunkHeader[:]); err != nil {
			return fmt.Errorf("write shard chunk header: %w", err)
		}
		if _, err := w.Write(ciphertext); err != nil {
			return fmt.Errorf("write shard chunk: %w", err)
		}
		observeEncryptedShardStage("write_chunk", stageStart)

		chunkIdx++
		if chunkIdx == 0 {
			return fmt.Errorf("encrypted shard has too many chunks")
		}
		if errors.Is(readErr, io.ErrUnexpectedEOF) || errors.Is(readErr, io.EOF) {
			return nil
		}
	}
}

func DecodeEncryptedShard(w io.Writer, r io.Reader, enc *encrypt.Encryptor, aadBase []byte) error {
	er, err := NewEncryptedShardReader(r, enc, aadBase)
	if err != nil {
		return err
	}
	if closer, ok := er.(io.Closer); ok {
		defer closer.Close()
	}
	if _, err := io.Copy(w, er); err != nil {
		return fmt.Errorf("write decrypted shard chunk: %w", err)
	}
	return nil
}

// NewEncryptedShardReader returns a reader that decrypts a GFSENC2 shard one
// chunk at a time. The encrypted header is consumed before the reader is
// returned; chunk authentication failures are reported by Read.
func NewEncryptedShardReader(r io.Reader, enc *encrypt.Encryptor, aadBase []byte) (io.Reader, error) {
	if enc == nil {
		return nil, fmt.Errorf("encrypted shard decode requires encryptor")
	}
	var header [encryptedHeaderLen]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("read encrypted shard header: %w", err)
	}
	if !IsEncryptedShard(header[:]) {
		return nil, fmt.Errorf("not an encrypted shard")
	}
	chunkSize := binary.LittleEndian.Uint32(header[len(encryptedShardMagic):])
	if chunkSize == 0 || chunkSize > maxEncryptedChunkSize {
		return nil, fmt.Errorf("invalid encrypted shard chunk size: %d", chunkSize)
	}
	var noncePrefix [encryptedNoncePrefixLen]byte
	copy(noncePrefix[:], header[len(encryptedShardMagic)+4:])

	return &encryptedShardReader{
		r:           r,
		enc:         enc,
		aadBase:     aadBase,
		chunkSize:   chunkSize,
		noncePrefix: noncePrefix,
	}, nil
}

// NewEncryptedShardRangeReader returns a plaintext reader for [offset,
// offset+length) without decrypting earlier chunks. It requires the GFSENC2
// fixed chunk layout produced by EncodeEncryptedShard.
func NewEncryptedShardRangeReader(r io.ReaderAt, enc *encrypt.Encryptor, aadBase []byte, offset, length int64) (io.Reader, error) {
	if enc == nil {
		return nil, fmt.Errorf("encrypted shard decode requires encryptor")
	}
	if offset < 0 {
		return nil, fmt.Errorf("negative encrypted shard offset %d", offset)
	}
	if length < 0 {
		return nil, fmt.Errorf("negative encrypted shard length %d", length)
	}
	var header [encryptedHeaderLen]byte
	if _, err := r.ReadAt(header[:], 0); err != nil {
		return nil, fmt.Errorf("read encrypted shard header: %w", err)
	}
	if !IsEncryptedShard(header[:]) {
		return nil, fmt.Errorf("not an encrypted shard")
	}
	chunkSize := binary.LittleEndian.Uint32(header[len(encryptedShardMagic):])
	if chunkSize == 0 || chunkSize > maxEncryptedChunkSize {
		return nil, fmt.Errorf("invalid encrypted shard chunk size: %d", chunkSize)
	}
	var noncePrefix [encryptedNoncePrefixLen]byte
	copy(noncePrefix[:], header[len(encryptedShardMagic)+4:])

	return &encryptedShardRangeReader{
		r:           r,
		enc:         enc,
		aadBase:     aadBase,
		chunkSize:   chunkSize,
		noncePrefix: noncePrefix,
		pos:         offset,
		remaining:   length,
	}, nil
}

// ReadEncryptedShardRangeAt decrypts plaintext bytes from an encrypted shard
// directly into dst. It still authenticates full encrypted chunks, but reuses
// pooled chunk buffers to avoid per-ReadAt MiB-scale allocation churn.
func ReadEncryptedShardRangeAt(r io.ReaderAt, enc *encrypt.Encryptor, aadBase []byte, offset int64, dst []byte) (int, error) {
	if enc == nil {
		return 0, fmt.Errorf("encrypted shard decode requires encryptor")
	}
	if offset < 0 {
		return 0, fmt.Errorf("negative encrypted shard offset %d", offset)
	}
	if len(dst) == 0 {
		return 0, nil
	}
	var header [encryptedHeaderLen]byte
	if _, err := r.ReadAt(header[:], 0); err != nil {
		return 0, fmt.Errorf("read encrypted shard header: %w", err)
	}
	if !IsEncryptedShard(header[:]) {
		return 0, fmt.Errorf("not an encrypted shard")
	}
	chunkSize := binary.LittleEndian.Uint32(header[len(encryptedShardMagic):])
	if chunkSize == 0 || chunkSize > maxEncryptedChunkSize {
		return 0, fmt.Errorf("invalid encrypted shard chunk size: %d", chunkSize)
	}
	var noncePrefix [encryptedNoncePrefixLen]byte
	copy(noncePrefix[:], header[len(encryptedShardMagic)+4:])

	plainPtr := encryptedPlainChunkPool.Get().(*[]byte)
	cipherPtr := encryptedCipherChunkPool.Get().(*[]byte)
	plainBuf := *plainPtr
	cipherBuf := *cipherPtr
	defer func() {
		if cap(plainBuf) > 0 {
			clear(plainBuf[:cap(plainBuf)])
		}
		*plainPtr = plainBuf[:0]
		encryptedPlainChunkPool.Put(plainPtr)
		*cipherPtr = cipherBuf[:0]
		encryptedCipherChunkPool.Put(cipherPtr)
	}()

	done := 0
	pos := offset
	for done < len(dst) {
		n, err := readEncryptedShardChunkAt(r, enc, aadBase, noncePrefix, chunkSize, pos, dst[done:], &plainBuf, &cipherBuf)
		done += n
		pos += int64(n)
		if err != nil {
			if done > 0 && errors.Is(err, io.EOF) {
				return done, io.ErrUnexpectedEOF
			}
			return done, err
		}
	}
	return done, nil
}

func readEncryptedShardChunkAt(r io.ReaderAt, enc *encrypt.Encryptor, aadBase []byte, noncePrefix [encryptedNoncePrefixLen]byte, chunkSize uint32, pos int64, dst []byte, plainBuf, cipherBuf *[]byte) (int, error) {
	chunkSize64 := int64(chunkSize)
	chunkIdx64 := pos / chunkSize64
	if chunkIdx64 > int64(^uint32(0)) {
		return 0, fmt.Errorf("encrypted shard chunk index too large: %d", chunkIdx64)
	}
	chunkIdx := uint32(chunkIdx64)
	inChunk := int(pos % chunkSize64)
	fullCipherLen := int64(chunkSize) + int64(enc.AEADOverhead())
	chunkFileOffset := int64(encryptedHeaderLen) + chunkIdx64*(int64(encryptedChunkHeaderLen)+fullCipherLen)

	var chunkHeader [encryptedChunkHeaderLen]byte
	if _, err := r.ReadAt(chunkHeader[:], chunkFileOffset); err != nil {
		return 0, fmt.Errorf("read encrypted shard chunk header: %w", err)
	}
	plainLen := binary.LittleEndian.Uint32(chunkHeader[0:4])
	cipherLen := binary.LittleEndian.Uint32(chunkHeader[4:8])
	if plainLen > chunkSize {
		return 0, fmt.Errorf("encrypted shard chunk %d plaintext length %d exceeds chunk size %d", chunkIdx, plainLen, chunkSize)
	}
	if cipherLen < plainLen || cipherLen > plainLen+uint32(enc.AEADOverhead()) {
		return 0, fmt.Errorf("invalid encrypted shard chunk %d ciphertext length %d for plaintext length %d", chunkIdx, cipherLen, plainLen)
	}
	if inChunk >= int(plainLen) {
		return 0, io.EOF
	}

	if cap(*cipherBuf) < int(cipherLen) {
		*cipherBuf = make([]byte, cipherLen)
	}
	ciphertext := (*cipherBuf)[:cipherLen]
	if _, err := r.ReadAt(ciphertext, chunkFileOffset+encryptedChunkHeaderLen); err != nil {
		return 0, fmt.Errorf("read encrypted shard chunk: %w", err)
	}
	nonce := encryptedChunkNonce(noncePrefix, chunkIdx)
	aad := encryptedChunkAAD(aadBase, chunkIdx)
	if inChunk == 0 && len(dst) >= int(plainLen) {
		plaintext, err := enc.OpenWithNonceAAD(dst[:0], nonce[:], ciphertext, aad)
		if err != nil {
			return 0, fmt.Errorf("decrypt shard chunk %d: %w", chunkIdx, err)
		}
		if uint32(len(plaintext)) != plainLen {
			return 0, fmt.Errorf("encrypted shard chunk %d plaintext length mismatch: got %d, want %d", chunkIdx, len(plaintext), plainLen)
		}
		return len(plaintext), nil
	}
	if cap(*plainBuf) < int(plainLen) {
		*plainBuf = make([]byte, plainLen)
	}
	plaintext, err := enc.OpenWithNonceAAD((*plainBuf)[:0], nonce[:], ciphertext, aad)
	if err != nil {
		return 0, fmt.Errorf("decrypt shard chunk %d: %w", chunkIdx, err)
	}
	if uint32(len(plaintext)) != plainLen {
		return 0, fmt.Errorf("encrypted shard chunk %d plaintext length mismatch: got %d, want %d", chunkIdx, len(plaintext), plainLen)
	}
	*plainBuf = plaintext

	end := len(plaintext)
	if max := inChunk + len(dst); max < end {
		end = max
	}
	return copy(dst, plaintext[inChunk:end]), nil
}

type encryptedShardReader struct {
	r           io.Reader
	enc         *encrypt.Encryptor
	aadBase     []byte
	chunkSize   uint32
	noncePrefix [encryptedNoncePrefixLen]byte
	chunkIdx    uint32
	plain       []byte
	plainPtr    *[]byte
	cipherPtr   *[]byte
	plainBuf    []byte
	cipherBuf   []byte
	done        bool
	closed      bool
}

func (r *encryptedShardReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("encrypted shard reader is closed")
	}
	for len(r.plain) == 0 && !r.done {
		if err := r.loadChunk(); err != nil {
			return 0, err
		}
	}
	if len(r.plain) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.plain)
	r.plain = r.plain[n:]
	return n, nil
}

func (r *encryptedShardReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.plain = nil
	if r.plainPtr != nil {
		if cap(r.plainBuf) > 0 {
			clear(r.plainBuf[:cap(r.plainBuf)])
		}
		*r.plainPtr = r.plainBuf[:0]
		encryptedPlainChunkPool.Put(r.plainPtr)
		r.plainPtr = nil
		r.plainBuf = nil
	}
	if r.cipherPtr != nil {
		*r.cipherPtr = r.cipherBuf[:0]
		encryptedCipherChunkPool.Put(r.cipherPtr)
		r.cipherPtr = nil
		r.cipherBuf = nil
	}
	return nil
}

func (r *encryptedShardReader) loadChunk() error {
	var chunkHeader [encryptedChunkHeaderLen]byte
	if _, err := io.ReadFull(r.r, chunkHeader[:]); err != nil {
		if errors.Is(err, io.EOF) {
			r.done = true
			return nil
		}
		return fmt.Errorf("read encrypted shard chunk header: %w", err)
	}
	plainLen := binary.LittleEndian.Uint32(chunkHeader[0:4])
	cipherLen := binary.LittleEndian.Uint32(chunkHeader[4:8])
	if plainLen > r.chunkSize {
		return fmt.Errorf("encrypted shard chunk %d plaintext length %d exceeds chunk size %d", r.chunkIdx, plainLen, r.chunkSize)
	}
	if cipherLen < plainLen || cipherLen > plainLen+uint32(r.enc.AEADOverhead()) {
		return fmt.Errorf("invalid encrypted shard chunk %d ciphertext length %d for plaintext length %d", r.chunkIdx, cipherLen, plainLen)
	}

	if r.cipherPtr == nil {
		r.cipherPtr = encryptedCipherChunkPool.Get().(*[]byte)
		r.cipherBuf = *r.cipherPtr
	}
	if cap(r.cipherBuf) < int(cipherLen) {
		r.cipherBuf = make([]byte, cipherLen)
	}
	ciphertext := r.cipherBuf[:cipherLen]
	if _, err := io.ReadFull(r.r, ciphertext); err != nil {
		return fmt.Errorf("read encrypted shard chunk: %w", err)
	}
	nonce := encryptedChunkNonce(r.noncePrefix, r.chunkIdx)
	aad := encryptedChunkAAD(r.aadBase, r.chunkIdx)
	if r.plainPtr == nil {
		r.plainPtr = encryptedPlainChunkPool.Get().(*[]byte)
		r.plainBuf = *r.plainPtr
	}
	if cap(r.plainBuf) < int(plainLen) {
		r.plainBuf = make([]byte, plainLen)
	}
	plaintext, err := r.enc.OpenWithNonceAAD(r.plainBuf[:0], nonce[:], ciphertext, aad)
	if err != nil {
		return fmt.Errorf("decrypt shard chunk %d: %w", r.chunkIdx, err)
	}
	if uint32(len(plaintext)) != plainLen {
		return fmt.Errorf("encrypted shard chunk %d plaintext length mismatch: got %d, want %d", r.chunkIdx, len(plaintext), plainLen)
	}
	r.plainBuf = plaintext[:0]
	r.plain = plaintext
	r.chunkIdx++
	if r.chunkIdx == 0 {
		return fmt.Errorf("encrypted shard has too many chunks")
	}
	return nil
}

type encryptedShardRangeReader struct {
	r           io.ReaderAt
	enc         *encrypt.Encryptor
	aadBase     []byte
	chunkSize   uint32
	noncePrefix [encryptedNoncePrefixLen]byte
	pos         int64
	remaining   int64
	plain       []byte
	plainPtr    *[]byte
	cipherPtr   *[]byte
	plainBuf    []byte
	cipherBuf   []byte
	closed      bool
}

func (r *encryptedShardRangeReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("encrypted shard range reader is closed")
	}
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	for len(r.plain) == 0 {
		if err := r.loadChunk(); err != nil {
			return 0, err
		}
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n := copy(p, r.plain)
	r.plain = r.plain[n:]
	r.pos += int64(n)
	r.remaining -= int64(n)
	return n, nil
}

func (r *encryptedShardRangeReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.plain = nil
	if r.plainPtr != nil {
		if cap(r.plainBuf) > 0 {
			clear(r.plainBuf[:cap(r.plainBuf)])
		}
		*r.plainPtr = r.plainBuf[:0]
		encryptedPlainChunkPool.Put(r.plainPtr)
		r.plainPtr = nil
		r.plainBuf = nil
	}
	if r.cipherPtr != nil {
		*r.cipherPtr = r.cipherBuf[:0]
		encryptedCipherChunkPool.Put(r.cipherPtr)
		r.cipherPtr = nil
		r.cipherBuf = nil
	}
	return nil
}

func (r *encryptedShardRangeReader) loadChunk() error {
	chunkSize := int64(r.chunkSize)
	chunkIdx64 := r.pos / chunkSize
	if chunkIdx64 > int64(^uint32(0)) {
		return fmt.Errorf("encrypted shard chunk index too large: %d", chunkIdx64)
	}
	chunkIdx := uint32(chunkIdx64)
	inChunk := int(r.pos % chunkSize)
	fullCipherLen := int64(r.chunkSize) + int64(r.enc.AEADOverhead())
	chunkFileOffset := int64(encryptedHeaderLen) + chunkIdx64*(int64(encryptedChunkHeaderLen)+fullCipherLen)

	var chunkHeader [encryptedChunkHeaderLen]byte
	if _, err := r.r.ReadAt(chunkHeader[:], chunkFileOffset); err != nil {
		return fmt.Errorf("read encrypted shard chunk header: %w", err)
	}
	plainLen := binary.LittleEndian.Uint32(chunkHeader[0:4])
	cipherLen := binary.LittleEndian.Uint32(chunkHeader[4:8])
	if plainLen > r.chunkSize {
		return fmt.Errorf("encrypted shard chunk %d plaintext length %d exceeds chunk size %d", chunkIdx, plainLen, r.chunkSize)
	}
	if cipherLen < plainLen || cipherLen > plainLen+uint32(r.enc.AEADOverhead()) {
		return fmt.Errorf("invalid encrypted shard chunk %d ciphertext length %d for plaintext length %d", chunkIdx, cipherLen, plainLen)
	}
	if inChunk >= int(plainLen) {
		return io.EOF
	}

	if r.cipherPtr == nil {
		r.cipherPtr = encryptedCipherChunkPool.Get().(*[]byte)
		r.cipherBuf = *r.cipherPtr
	}
	if cap(r.cipherBuf) < int(cipherLen) {
		r.cipherBuf = make([]byte, cipherLen)
	}
	ciphertext := r.cipherBuf[:cipherLen]
	if _, err := r.r.ReadAt(ciphertext, chunkFileOffset+encryptedChunkHeaderLen); err != nil {
		return fmt.Errorf("read encrypted shard chunk: %w", err)
	}
	nonce := encryptedChunkNonce(r.noncePrefix, chunkIdx)
	aad := encryptedChunkAAD(r.aadBase, chunkIdx)
	if r.plainPtr == nil {
		r.plainPtr = encryptedPlainChunkPool.Get().(*[]byte)
		r.plainBuf = *r.plainPtr
	}
	if cap(r.plainBuf) < int(plainLen) {
		r.plainBuf = make([]byte, plainLen)
	}
	plaintext, err := r.enc.OpenWithNonceAAD(r.plainBuf[:0], nonce[:], ciphertext, aad)
	if err != nil {
		return fmt.Errorf("decrypt shard chunk %d: %w", chunkIdx, err)
	}
	if uint32(len(plaintext)) != plainLen {
		return fmt.Errorf("encrypted shard chunk %d plaintext length mismatch: got %d, want %d", chunkIdx, len(plaintext), plainLen)
	}

	end := len(plaintext)
	if max := inChunk + int(r.remaining); max < end {
		end = max
	}
	r.plainBuf = plaintext[:0]
	r.plain = plaintext[inChunk:end]
	return nil
}

// WriteEncryptedShardStreamAtomic writes a chunked encrypted shard from r
// using the same tmp + sync + rename recipe as WriteShardStreamAtomic.
func WriteEncryptedShardStreamAtomic(path string, r io.Reader, enc *encrypt.Encryptor, aadBase []byte, chunkSize int) error {
	return writeEncryptedShardStreamAtomic(path, r, enc, aadBase, chunkSize, true)
}

// WriteEncryptedShardStreamAtomicExistingDir is WriteEncryptedShardStreamAtomic
// for callers that already ensured filepath.Dir(path) exists.
func WriteEncryptedShardStreamAtomicExistingDir(path string, r io.Reader, enc *encrypt.Encryptor, aadBase []byte, chunkSize int) error {
	return writeEncryptedShardStreamAtomic(path, r, enc, aadBase, chunkSize, false)
}

func writeEncryptedShardStreamAtomic(path string, r io.Reader, enc *encrypt.Encryptor, aadBase []byte, chunkSize int, mkdir bool) error {
	stageStart := time.Now()
	if mkdir {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("mkdir shard dir: %w", err)
		}
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create tmp shard: %w", err)
	}
	observeEncryptedShardStage("open_tmp", stageStart)
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}
	stageStart = time.Now()
	if err := EncodeEncryptedShard(f, r, enc, aadBase, chunkSize); err != nil {
		cleanup()
		return err
	}
	observeEncryptedShardStage("encode_stream", stageStart)
	// Durability is owned by internal/storage/datawal. The tmp+rename below
	// provides atomic visibility of already-WAL-flushed bytes.
	stageStart = time.Now()
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close tmp shard: %w", err)
	}
	observeEncryptedShardStage("close_tmp", stageStart)
	stageStart = time.Now()
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename shard: %w", err)
	}
	observeEncryptedShardStage("rename", stageStart)
	return nil
}

func observeEncryptedShardStage(stage string, start time.Time) {
	metrics.ObjectPutStageDuration.WithLabelValues("encrypted_shard", stage).Observe(time.Since(start).Seconds())
}

func encryptedChunkNonce(prefix [encryptedNoncePrefixLen]byte, chunkIdx uint32) [encryptedNonceLen]byte {
	var nonce [encryptedNonceLen]byte
	copy(nonce[:], prefix[:])
	binary.BigEndian.PutUint32(nonce[encryptedNoncePrefixLen:], chunkIdx)
	return nonce
}

func encryptedChunkAAD(base []byte, chunkIdx uint32) []byte {
	size := len(encryptedShardMagic) + len(base) + 4
	var aad []byte
	var localAAD [256]byte
	if size <= len(localAAD) {
		aad = localAAD[:0]
	} else {
		aad = make([]byte, 0, size)
	}
	aad = append(aad, encryptedShardMagic...)
	aad = append(aad, base...)
	var idx [4]byte
	binary.BigEndian.PutUint32(idx[:], chunkIdx)
	aad = append(aad, idx[:]...)
	return aad
}

// EncodeShard appends the versioned CRC envelope around payload.
func EncodeShard(data []byte) []byte {
	out := make([]byte, len(shardMagic)+len(data)+footerLen)
	copy(out, shardMagic)
	copy(out[len(shardMagic):], data)
	binary.LittleEndian.PutUint32(out[len(shardMagic)+len(data):], crc32.ChecksumIEEE(data))
	return out
}

// DecodeShard verifies the envelope/footer and returns the payload slice.
// Returns ErrCRCMismatch if the shard is truncated or the checksum is wrong.
func DecodeShard(data []byte) ([]byte, error) {
	if IsEncodedShard(data) {
		if len(data) < len(shardMagic)+footerLen {
			return nil, fmt.Errorf("%w: shard too short (%d bytes)", ErrCRCMismatch, len(data))
		}
		payload := data[len(shardMagic) : len(data)-footerLen]
		stored := binary.LittleEndian.Uint32(data[len(data)-footerLen:])
		if crc32.ChecksumIEEE(payload) != stored {
			return nil, ErrCRCMismatch
		}
		return payload, nil
	}

	// Backward compatibility for the older eccodec test-only layout:
	// <payload><crc32>. Cluster-mode legacy raw fallback intentionally does
	// not call DecodeShard; it checks IsEncodedShard first and treats no-magic
	// bytes as legacy raw shards.
	if len(data) < footerLen {
		return nil, fmt.Errorf("%w: shard too short (%d bytes)", ErrCRCMismatch, len(data))
	}
	payload := data[:len(data)-footerLen]
	stored := binary.LittleEndian.Uint32(data[len(data)-footerLen:])
	if crc32.ChecksumIEEE(payload) != stored {
		return nil, ErrCRCMismatch
	}
	return payload, nil
}

// NewShardReader returns a reader for a GFSCRC1 shard payload. It streams the
// payload while retaining only the trailing CRC footer needed for verification.
func NewShardReader(r io.Reader) (io.Reader, error) {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return nil, fmt.Errorf("%w: shard too short", ErrCRCMismatch)
	}
	if !IsEncodedShard(magic[:]) {
		return nil, fmt.Errorf("not an encoded shard")
	}
	return &verifiedShardReader{
		r:    r,
		hash: crc32.NewIEEE(),
		buf:  make([]byte, 32<<10),
	}, nil
}

type verifiedShardReader struct {
	r       io.Reader
	hash    hash.Hash32
	buf     []byte
	pending []byte
	tail    []byte
	done    bool
}

func (r *verifiedShardReader) Read(p []byte) (int, error) {
	for len(r.pending) == 0 && !r.done {
		if err := r.readMore(); err != nil {
			return 0, err
		}
	}
	if len(r.pending) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func (r *verifiedShardReader) readMore() error {
	n, err := r.r.Read(r.buf)
	if n > 0 {
		r.tail = append(r.tail, r.buf[:n]...)
		if len(r.tail) > footerLen {
			payloadLen := len(r.tail) - footerLen
			r.pending = append(r.pending, r.tail[:payloadLen]...)
			_, _ = r.hash.Write(r.tail[:payloadLen])
			copy(r.tail, r.tail[payloadLen:])
			r.tail = r.tail[:footerLen]
		}
	}
	if err == nil {
		return nil
	}
	if !errors.Is(err, io.EOF) {
		return err
	}
	r.done = true
	if len(r.tail) < footerLen {
		return fmt.Errorf("%w: shard too short", ErrCRCMismatch)
	}
	stored := binary.LittleEndian.Uint32(r.tail)
	if r.hash.Sum32() != stored {
		return ErrCRCMismatch
	}
	return nil
}

// NewSizedShardReader returns a CRC-verifying payload reader for callers that
// have already consumed the GFSCRC1 magic and know the payload length.
func NewSizedShardReader(r io.Reader, payloadLen int64) io.Reader {
	return &sizedShardReader{
		r:         r,
		remaining: payloadLen,
		hash:      crc32.NewIEEE(),
	}
}

type sizedShardReader struct {
	r         io.Reader
	remaining int64
	hash      hash.Hash32
	verified  bool
}

func (r *sizedShardReader) Read(p []byte) (int, error) {
	if r.remaining > 0 {
		finalRead := int64(len(p)) >= r.remaining
		if int64(len(p)) > r.remaining {
			p = p[:r.remaining]
		}
		n, err := r.r.Read(p)
		if n > 0 {
			_, _ = r.hash.Write(p[:n])
			r.remaining -= int64(n)
		}
		if err != nil && (!errors.Is(err, io.EOF) || r.remaining > 0) {
			return n, err
		}
		if finalRead && r.remaining == 0 {
			if err := r.verifyFooter(); err != nil {
				return 0, err
			}
		}
		if n > 0 {
			return n, nil
		}
		return 0, io.ErrUnexpectedEOF
	}
	if r.verified {
		return 0, io.EOF
	}
	if err := r.verifyFooter(); err != nil {
		return 0, err
	}
	return 0, io.EOF
}

func (r *sizedShardReader) verifyFooter() error {
	if r.verified {
		return nil
	}
	var footer [footerLen]byte
	if _, err := io.ReadFull(r.r, footer[:]); err != nil {
		return fmt.Errorf("%w: shard too short", ErrCRCMismatch)
	}
	if r.hash.Sum32() != binary.LittleEndian.Uint32(footer[:]) {
		return ErrCRCMismatch
	}
	r.verified = true
	return nil
}

// WriteShardAtomic writes data (with CRC32 footer) to path using the
// tmp + rename recipe. Durability is owned by internal/storage/datawal;
// the tmp file here provides atomic visibility of already-WAL-flushed
// bytes, so a crash mid-write never exposes a torn shard at the
// destination.
func WriteShardAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir shard dir: %w", err)
	}
	payload := EncodeShard(data)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create tmp shard: %w", err)
	}
	if _, err := f.Write(payload); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write tmp shard: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close tmp shard: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename shard: %w", err)
	}
	return nil
}

// WriteShardStreamAtomic writes an encoded shard from r without buffering the
// full payload in memory.
func WriteShardStreamAtomic(path string, r io.Reader) error {
	return writeShardStreamAtomic(path, r, true)
}

// WriteShardStreamAtomicExistingDir is WriteShardStreamAtomic for callers that
// already ensured filepath.Dir(path) exists.
func WriteShardStreamAtomicExistingDir(path string, r io.Reader) error {
	return writeShardStreamAtomic(path, r, false)
}

func writeShardStreamAtomic(path string, r io.Reader, mkdir bool) error {
	if mkdir {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("mkdir shard dir: %w", err)
		}
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create tmp shard: %w", err)
	}
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}
	if _, err := f.Write(shardMagic); err != nil {
		cleanup()
		return fmt.Errorf("write shard magic: %w", err)
	}
	h := crc32.NewIEEE()
	if _, err := io.Copy(io.MultiWriter(f, h), r); err != nil {
		cleanup()
		return fmt.Errorf("write shard payload: %w", err)
	}
	var footer [footerLen]byte
	binary.LittleEndian.PutUint32(footer[:], h.Sum32())
	if _, err := f.Write(footer[:]); err != nil {
		cleanup()
		return fmt.Errorf("write shard footer: %w", err)
	}
	// Durability is owned by internal/storage/datawal. The tmp+rename
	// below provides atomic visibility of already-WAL-flushed bytes.
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close tmp shard: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename shard: %w", err)
	}
	return nil
}

// ReadShardVerified reads a shard from disk and returns its payload after
// verifying the CRC32 footer. Returns ErrCRCMismatch on corruption.
func ReadShardVerified(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return DecodeShard(raw)
}
