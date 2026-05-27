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

// ErrShardCorrupt marks errors that mean the bytes ON DISK are structurally
// bad (corruption), as opposed to a live I/O fault (EIO/EMFILE/EBUSY/perm)
// propagated from the underlying file. The placement monitor uses
// IsCorruption to decide whether to quarantine the parent object; transient
// faults must NOT be wrapped so they stay unwrapped and are skipped.
var ErrShardCorrupt = errors.New("eccodec: shard content corrupt")

// IsCorruption reports whether err represents confirmed on-disk shard
// corruption (CRC mismatch, structural decode failure, mid-structure
// truncation, or AEAD chunk-auth failure). It returns false for transient
// I/O errors, nil, and generic errors. errors.Is unwrapping means callers
// can pass an error wrapped with %w through several layers and still get a
// correct answer.
func IsCorruption(err error) bool {
	return errors.Is(err, ErrCRCMismatch) || errors.Is(err, ErrShardCorrupt)
}

// footerLen is the size of the CRC32 footer appended to every shard.
const footerLen = 4

var shardMagic = []byte("GFSCRC1\x00")
var encryptedShardMagic = []byte("GFSENC3\x00")

var encryptedPlainChunkPool = sync.Pool{New: func() any {
	b := make([]byte, DefaultEncryptedChunkSize)
	return &b
}}

const (
	DefaultEncryptedChunkSize = 1 << 20
	maxEncryptedChunkSize     = DefaultEncryptedChunkSize
	// GFSENC3 header: magic(8) + format_version(2) + dek_gen(4) + chunk_size(4) + chunk_overhead(2).
	encryptedShardFormatVersion = uint16(1)
	encryptedHeaderLen          = 8 + 2 + 4 + 4 + 2
	encryptedChunkHeaderLen     = 8
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

// EncodeEncryptedShard streams r into the GFSENC3 format, sealing each chunk
// via enc under DomainShard with baseFields plus the per-chunk ordinal. All
// chunks are sealed under one pinned generation (chunk 0's); a later chunk at a
// different gen fails the write so the header's dek_gen describes every chunk.
func EncodeEncryptedShard(w io.Writer, r io.Reader, enc ShardEncryptor, baseFields []encrypt.AADField, chunkSize int) error {
	if enc == nil {
		return fmt.Errorf("encrypted shard encode requires encryptor")
	}
	if chunkSize <= 0 {
		chunkSize = DefaultEncryptedChunkSize
	}
	if chunkSize > maxEncryptedChunkSize {
		return fmt.Errorf("encrypted shard chunk size too large: %d", chunkSize)
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

	var (
		chunkIdx      uint32
		pinnedGen     uint32
		chunkOverhead uint16
		headerWritten bool
	)
	for {
		n, readErr := io.ReadFull(r, plain)
		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) && !errors.Is(readErr, io.EOF) {
			return fmt.Errorf("read shard chunk: %w", readErr)
		}
		if n == 0 && errors.Is(readErr, io.EOF) {
			break
		}
		sealed, gen, err := enc.Seal(encrypt.DomainShard, chunkFields(baseFields, chunkIdx), plain[:n])
		if err != nil {
			return fmt.Errorf("encrypt shard chunk %d: %w", chunkIdx, err)
		}
		over := len(sealed) - n
		if over < 0 || over > int(^uint16(0)) {
			return fmt.Errorf("encrypt shard chunk %d: implausible overhead %d", chunkIdx, over)
		}
		if chunkIdx == 0 {
			pinnedGen = gen
			chunkOverhead = uint16(over)
			if err := writeEncryptedShardHeader(w, pinnedGen, uint32(chunkSize), chunkOverhead); err != nil {
				return err
			}
			headerWritten = true
		} else {
			if gen != pinnedGen {
				return fmt.Errorf("encrypt shard chunk %d sealed at gen %d, pinned %d", chunkIdx, gen, pinnedGen)
			}
			if uint16(over) != chunkOverhead {
				return fmt.Errorf("encrypt shard chunk %d overhead %d != pinned %d", chunkIdx, over, chunkOverhead)
			}
		}
		var chunkHeader [encryptedChunkHeaderLen]byte
		binary.LittleEndian.PutUint32(chunkHeader[0:4], uint32(n))
		binary.LittleEndian.PutUint32(chunkHeader[4:8], uint32(len(sealed)))
		if _, err := w.Write(chunkHeader[:]); err != nil {
			return fmt.Errorf("write shard chunk header: %w", err)
		}
		if _, err := w.Write(sealed); err != nil {
			return fmt.Errorf("write shard chunk: %w", err)
		}
		chunkIdx++
		if chunkIdx == 0 {
			return fmt.Errorf("encrypted shard has too many chunks")
		}
		if errors.Is(readErr, io.ErrUnexpectedEOF) || errors.Is(readErr, io.EOF) {
			break
		}
	}
	if !headerWritten {
		// Empty shard: still emit a valid header (gen 0, overhead 0) so decode succeeds.
		if err := writeEncryptedShardHeader(w, pinnedGen, uint32(chunkSize), chunkOverhead); err != nil {
			return err
		}
	}
	return nil
}

// EncryptedShardChunkedWriter streams an encrypted shard in the GFSENC3
// on-disk format. Bytes passed to Write are buffered until chunkSize is
// reached, then emitted as a single encrypted chunk (8-byte chunk header +
// AEAD-sealed payload). Close flushes any pending bytes as a final partial
// chunk. The result is a GFSENC3 stream decodable by DecodeEncryptedShard
// with the same baseFields.
//
// Single-use: NOT safe for concurrent Write calls. Always call Close before
// reading the underlying writer.
type EncryptedShardChunkedWriter struct {
	w             io.Writer
	enc           ShardEncryptor
	baseFields    []encrypt.AADField
	chunkSize     int
	headerWritten bool
	pinnedGen     uint32
	chunkOverhead uint16
	chunkIdx      uint32
	plainBuf      []byte // pending plaintext, len ≤ chunkSize
	plainPtr      *[]byte
	closed        bool
}

// NewEncryptedShardChunkedWriter constructs a streaming writer that produces
// a GFSENC3 stream decodable by DecodeEncryptedShard with the same baseFields.
func NewEncryptedShardChunkedWriter(w io.Writer, enc ShardEncryptor, baseFields []encrypt.AADField, chunkSize int) (*EncryptedShardChunkedWriter, error) {
	if enc == nil {
		return nil, fmt.Errorf("encrypted shard chunked writer requires encryptor")
	}
	if chunkSize <= 0 {
		chunkSize = DefaultEncryptedChunkSize
	}
	if chunkSize > maxEncryptedChunkSize {
		return nil, fmt.Errorf("encrypted shard chunk size too large: %d", chunkSize)
	}
	out := &EncryptedShardChunkedWriter{
		w:          w,
		enc:        enc,
		baseFields: append([]encrypt.AADField(nil), baseFields...),
		chunkSize:  chunkSize,
	}
	out.plainPtr = encryptedPlainChunkPool.Get().(*[]byte)
	plain := *out.plainPtr
	if cap(plain) < chunkSize {
		plain = make([]byte, 0, chunkSize)
	}
	out.plainBuf = plain[:0]
	return out, nil
}

// Write appends p to the chunk buffer, emitting full chunks as the buffer
// fills. Partial last chunks are emitted by Close.
func (w *EncryptedShardChunkedWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("encrypted shard chunked writer: write after close")
	}
	written := 0
	for len(p) > 0 {
		room := w.chunkSize - len(w.plainBuf)
		take := room
		if take > len(p) {
			take = len(p)
		}
		w.plainBuf = append(w.plainBuf, p[:take]...)
		p = p[take:]
		written += take
		if len(w.plainBuf) == w.chunkSize {
			if err := w.emitChunk(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

// Close flushes any pending partial chunk and releases pooled buffers.
// Returns nil on second call so defer-Close patterns are safe.
func (w *EncryptedShardChunkedWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	defer w.releasePools()
	if len(w.plainBuf) > 0 {
		if err := w.emitChunk(); err != nil {
			return err
		}
	}
	if !w.headerWritten {
		// Empty shard: emit a valid header (gen 0, overhead 0).
		if err := writeEncryptedShardHeader(w.w, 0, uint32(w.chunkSize), 0); err != nil {
			return err
		}
		w.headerWritten = true
	}
	return nil
}

func (w *EncryptedShardChunkedWriter) emitChunk() error {
	sealed, gen, err := w.enc.Seal(encrypt.DomainShard, chunkFields(w.baseFields, w.chunkIdx), w.plainBuf)
	if err != nil {
		return fmt.Errorf("encrypt shard chunk %d: %w", w.chunkIdx, err)
	}
	over := len(sealed) - len(w.plainBuf)
	if over < 0 || over > int(^uint16(0)) {
		return fmt.Errorf("encrypt shard chunk %d: implausible overhead %d", w.chunkIdx, over)
	}
	if !w.headerWritten {
		w.pinnedGen = gen
		w.chunkOverhead = uint16(over)
		if err := writeEncryptedShardHeader(w.w, w.pinnedGen, uint32(w.chunkSize), w.chunkOverhead); err != nil {
			return err
		}
		w.headerWritten = true
	} else {
		if gen != w.pinnedGen {
			return fmt.Errorf("encrypt shard chunk %d sealed at gen %d, pinned %d", w.chunkIdx, gen, w.pinnedGen)
		}
		if uint16(over) != w.chunkOverhead {
			return fmt.Errorf("encrypt shard chunk %d overhead %d != pinned %d", w.chunkIdx, over, w.chunkOverhead)
		}
	}
	var chunkHeader [encryptedChunkHeaderLen]byte
	binary.LittleEndian.PutUint32(chunkHeader[:4], uint32(len(w.plainBuf)))
	binary.LittleEndian.PutUint32(chunkHeader[4:], uint32(len(sealed)))
	if _, err := w.w.Write(chunkHeader[:]); err != nil {
		return fmt.Errorf("write shard chunk header: %w", err)
	}
	if _, err := w.w.Write(sealed); err != nil {
		return fmt.Errorf("write shard chunk: %w", err)
	}
	w.chunkIdx++
	if w.chunkIdx == 0 {
		return fmt.Errorf("encrypted shard has too many chunks")
	}
	w.plainBuf = w.plainBuf[:0]
	return nil
}

func (w *EncryptedShardChunkedWriter) releasePools() {
	if w.plainPtr != nil {
		clear(w.plainBuf[:cap(w.plainBuf)])
		*w.plainPtr = w.plainBuf[:0]
		encryptedPlainChunkPool.Put(w.plainPtr)
		w.plainPtr = nil
		w.plainBuf = nil
	}
}

func DecodeEncryptedShard(w io.Writer, r io.Reader, enc ShardEncryptor, baseFields []encrypt.AADField) error {
	er, err := NewEncryptedShardReader(r, enc, baseFields)
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

// NewEncryptedShardReader returns a reader that decrypts a GFSENC3 shard one
// chunk at a time. The encrypted header is consumed before the reader is
// returned; chunk authentication failures are reported by Read.
func NewEncryptedShardReader(r io.Reader, enc ShardEncryptor, baseFields []encrypt.AADField) (io.Reader, error) {
	if enc == nil {
		return nil, fmt.Errorf("encrypted shard decode requires encryptor")
	}
	gen, chunkSize, overhead, err := readEncryptedShardHeader(r)
	if err != nil {
		return nil, err
	}
	return &encryptedShardReader{
		r:          r,
		enc:        enc,
		baseFields: baseFields,
		gen:        gen,
		chunkSize:  chunkSize,
		overhead:   overhead,
	}, nil
}

// NewEncryptedShardRangeReader returns a plaintext reader for [offset,
// offset+length) without decrypting earlier chunks. It requires the GFSENC3
// fixed chunk layout produced by EncodeEncryptedShard.
func NewEncryptedShardRangeReader(r io.ReaderAt, enc ShardEncryptor, baseFields []encrypt.AADField, offset, length int64) (io.Reader, error) {
	if enc == nil {
		return nil, fmt.Errorf("encrypted shard decode requires encryptor")
	}
	if offset < 0 {
		return nil, fmt.Errorf("negative encrypted shard offset %d", offset)
	}
	if length < 0 {
		return nil, fmt.Errorf("negative encrypted shard length %d", length)
	}
	var hdr [encryptedHeaderLen]byte
	if _, err := r.ReadAt(hdr[:], 0); err != nil {
		// ReadAt reports io.EOF when the file is shorter than the fixed header
		// it must read: truncation → corruption. A non-EOF error is a live I/O
		// fault (EIO etc.) and stays transient.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("read encrypted shard header: %w: %w", ErrShardCorrupt, err)
		}
		return nil, fmt.Errorf("read encrypted shard header: %w", err)
	}
	gen, chunkSize, overhead, err := parseEncryptedShardHeader(hdr[:])
	if err != nil {
		return nil, err
	}

	return &encryptedShardRangeReader{
		r:          r,
		enc:        enc,
		baseFields: baseFields,
		gen:        gen,
		chunkSize:  chunkSize,
		overhead:   overhead,
		pos:        offset,
		remaining:  length,
	}, nil
}

// ReadEncryptedShardRangeAt decrypts plaintext bytes from an encrypted shard
// directly into dst. It authenticates full encrypted chunks.
func ReadEncryptedShardRangeAt(r io.ReaderAt, enc ShardEncryptor, baseFields []encrypt.AADField, offset int64, dst []byte) (int, error) {
	if enc == nil {
		return 0, fmt.Errorf("encrypted shard decode requires encryptor")
	}
	if offset < 0 {
		return 0, fmt.Errorf("negative encrypted shard offset %d", offset)
	}
	if len(dst) == 0 {
		return 0, nil
	}
	var hdr [encryptedHeaderLen]byte
	if _, err := r.ReadAt(hdr[:], 0); err != nil {
		// ReadAt reports io.EOF when the file is shorter than the fixed header
		// it must read: truncation → corruption. A non-EOF error is a live I/O
		// fault (EIO etc.) and stays transient.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, fmt.Errorf("read encrypted shard header: %w: %w", ErrShardCorrupt, err)
		}
		return 0, fmt.Errorf("read encrypted shard header: %w", err)
	}
	gen, chunkSize, overhead, err := parseEncryptedShardHeader(hdr[:])
	if err != nil {
		return 0, err
	}

	done := 0
	pos := offset
	for done < len(dst) {
		n, err := readEncryptedShardChunkAt(r, enc, baseFields, gen, overhead, chunkSize, pos, dst[done:])
		done += n
		pos += int64(n)
		if err != nil {
			// A truncated chunk wraps both ErrShardCorrupt and io.EOF; preserve the
			// corruption sentinel rather than remapping it to a bare unexpected-EOF.
			if errors.Is(err, ErrShardCorrupt) {
				return done, err
			}
			if done > 0 && errors.Is(err, io.EOF) {
				return done, io.ErrUnexpectedEOF
			}
			return done, err
		}
	}
	return done, nil
}

func readEncryptedShardChunkAt(r io.ReaderAt, enc ShardEncryptor, baseFields []encrypt.AADField, gen uint32, overhead uint16, chunkSize uint32, pos int64, dst []byte) (int, error) {
	chunkSize64 := int64(chunkSize)
	chunkIdx64 := pos / chunkSize64
	if chunkIdx64 > int64(^uint32(0)) {
		return 0, fmt.Errorf("encrypted shard chunk index too large: %d", chunkIdx64)
	}
	chunkIdx := uint32(chunkIdx64)
	inChunk := int(pos % chunkSize64)
	fullCipherLen := int64(chunkSize) + int64(overhead)
	chunkFileOffset := int64(encryptedHeaderLen) + chunkIdx64*(int64(encryptedChunkHeaderLen)+fullCipherLen)

	var chunkHeader [encryptedChunkHeaderLen]byte
	if n, err := r.ReadAt(chunkHeader[:], chunkFileOffset); err != nil {
		// ReadAt returns io.EOF for both "offset fully past EOF, 0 bytes read"
		// (clean end-of-stream: no chunk here, normal for a healthy shard read
		// past its last chunk) and "partial header at EOF" (truncation). Unlike
		// io.ReadFull, ReadAt does not split these into EOF vs ErrUnexpectedEOF,
		// so we disambiguate by bytes read (n).
		if n == 0 && errors.Is(err, io.EOF) {
			// Clean end-of-stream: NOT corruption. The caller
			// (ReadEncryptedShardRangeAt) remaps a partial (done>0) read to
			// io.ErrUnexpectedEOF; an exact-boundary full read sees no error.
			return 0, io.EOF
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// n>0: a partial chunk header at EOF means the file is truncated
			// mid-structure → corruption.
			return 0, fmt.Errorf("read encrypted shard chunk header: %w: %w", ErrShardCorrupt, err)
		}
		return 0, fmt.Errorf("read encrypted shard chunk header: %w", err)
	}
	plainLen := binary.LittleEndian.Uint32(chunkHeader[0:4])
	cipherLen := binary.LittleEndian.Uint32(chunkHeader[4:8])
	if plainLen > chunkSize {
		return 0, fmt.Errorf("%w: encrypted shard chunk %d plaintext length %d exceeds chunk size %d", ErrShardCorrupt, chunkIdx, plainLen, chunkSize)
	}
	if cipherLen != plainLen+uint32(overhead) {
		return 0, fmt.Errorf("%w: encrypted shard chunk %d ciphertext length %d != %d+%d", ErrShardCorrupt, chunkIdx, cipherLen, plainLen, overhead)
	}
	if inChunk >= int(plainLen) {
		// Clean end-of-stream at a chunk boundary: NOT corruption. The caller
		// (ReadEncryptedShardRangeAt) maps a partial read to io.ErrUnexpectedEOF.
		return 0, io.EOF
	}

	ciphertext := make([]byte, cipherLen)
	if _, err := r.ReadAt(ciphertext, chunkFileOffset+encryptedChunkHeaderLen); err != nil {
		// The header declared cipherLen bytes; a short ReadAt (io.EOF) means the
		// payload is truncated → corruption. Other errors are transient.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, fmt.Errorf("read encrypted shard chunk: %w: %w", ErrShardCorrupt, err)
		}
		return 0, fmt.Errorf("read encrypted shard chunk: %w", err)
	}
	// seam Open failure on an owned shard = corruption, not a key-version race;
	// re-audit when DEKKeeperAdapter is wired in slice C.
	plaintext, err := enc.Open(encrypt.DomainShard, chunkFields(baseFields, chunkIdx), gen, ciphertext)
	if err != nil {
		return 0, fmt.Errorf("decrypt shard chunk %d: %w: %w", chunkIdx, ErrShardCorrupt, err)
	}
	if uint32(len(plaintext)) != plainLen {
		return 0, fmt.Errorf("%w: encrypted shard chunk %d plaintext length mismatch: got %d, want %d", ErrShardCorrupt, chunkIdx, len(plaintext), plainLen)
	}

	end := len(plaintext)
	if max := inChunk + len(dst); max < end {
		end = max
	}
	return copy(dst, plaintext[inChunk:end]), nil
}

type encryptedShardReader struct {
	r          io.Reader
	enc        ShardEncryptor
	baseFields []encrypt.AADField
	chunkSize  uint32
	gen        uint32
	overhead   uint16
	chunkIdx   uint32
	plain      []byte
	done       bool
	closed     bool
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
	// The seam's Open returns a fresh plaintext slice; zero it before releasing
	// so decrypted bytes do not linger in the heap.
	if len(r.plain) > 0 {
		clear(r.plain)
	}
	r.plain = nil
	return nil
}

func (r *encryptedShardReader) loadChunk() error {
	var chunkHeader [encryptedChunkHeaderLen]byte
	if _, err := io.ReadFull(r.r, chunkHeader[:]); err != nil {
		// Clean EOF at a chunk boundary is the normal end of the stream.
		if errors.Is(err, io.EOF) {
			r.done = true
			return nil
		}
		// ErrUnexpectedEOF means a chunk header was started but the file ended
		// short of it: truncation → corruption. Other errors are transient.
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("read encrypted shard chunk header: %w: %w", ErrShardCorrupt, err)
		}
		return fmt.Errorf("read encrypted shard chunk header: %w", err)
	}
	plainLen := binary.LittleEndian.Uint32(chunkHeader[0:4])
	cipherLen := binary.LittleEndian.Uint32(chunkHeader[4:8])
	if plainLen > r.chunkSize {
		return fmt.Errorf("%w: encrypted shard chunk %d plaintext length %d exceeds chunk size %d", ErrShardCorrupt, r.chunkIdx, plainLen, r.chunkSize)
	}
	if cipherLen != plainLen+uint32(r.overhead) {
		return fmt.Errorf("%w: encrypted shard chunk %d ciphertext length %d != %d+%d", ErrShardCorrupt, r.chunkIdx, cipherLen, plainLen, r.overhead)
	}
	ciphertext := make([]byte, cipherLen)
	if _, err := io.ReadFull(r.r, ciphertext); err != nil {
		// The header declared cipherLen bytes; a short read means the payload
		// is truncated → corruption. Other errors are transient I/O faults.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("read encrypted shard chunk: %w: %w", ErrShardCorrupt, err)
		}
		return fmt.Errorf("read encrypted shard chunk: %w", err)
	}
	// seam Open failure on an owned shard = corruption, not a key-version race;
	// re-audit when DEKKeeperAdapter is wired in slice C.
	plaintext, err := r.enc.Open(encrypt.DomainShard, chunkFields(r.baseFields, r.chunkIdx), r.gen, ciphertext)
	if err != nil {
		return fmt.Errorf("decrypt shard chunk %d: %w: %w", r.chunkIdx, ErrShardCorrupt, err)
	}
	if uint32(len(plaintext)) != plainLen {
		return fmt.Errorf("%w: encrypted shard chunk %d plaintext length mismatch: got %d, want %d", ErrShardCorrupt, r.chunkIdx, len(plaintext), plainLen)
	}
	r.plain = plaintext
	r.chunkIdx++
	if r.chunkIdx == 0 {
		return fmt.Errorf("encrypted shard has too many chunks")
	}
	return nil
}

type encryptedShardRangeReader struct {
	r          io.ReaderAt
	enc        ShardEncryptor
	baseFields []encrypt.AADField
	gen        uint32
	chunkSize  uint32
	overhead   uint16
	pos        int64
	remaining  int64
	plain      []byte
	closed     bool
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
	// The seam's Open returns a fresh plaintext slice; zero it before releasing
	// so decrypted bytes do not linger in the heap.
	if len(r.plain) > 0 {
		clear(r.plain)
	}
	r.plain = nil
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
	fullCipherLen := int64(r.chunkSize) + int64(r.overhead)
	chunkFileOffset := int64(encryptedHeaderLen) + chunkIdx64*(int64(encryptedChunkHeaderLen)+fullCipherLen)

	var chunkHeader [encryptedChunkHeaderLen]byte
	if n, err := r.r.ReadAt(chunkHeader[:], chunkFileOffset); err != nil {
		// See readEncryptedShardChunkAt: ReadAt collapses clean end-of-stream
		// and truncation into io.EOF, so disambiguate by bytes read (n).
		if n == 0 && errors.Is(err, io.EOF) {
			// Clean end-of-stream at a chunk boundary: NOT corruption. The
			// Read loop treats a bare io.EOF as normal end.
			return io.EOF
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// n>0: partial chunk header at EOF = truncation → corruption.
			return fmt.Errorf("read encrypted shard chunk header: %w: %w", ErrShardCorrupt, err)
		}
		return fmt.Errorf("read encrypted shard chunk header: %w", err)
	}
	plainLen := binary.LittleEndian.Uint32(chunkHeader[0:4])
	cipherLen := binary.LittleEndian.Uint32(chunkHeader[4:8])
	if plainLen > r.chunkSize {
		return fmt.Errorf("%w: encrypted shard chunk %d plaintext length %d exceeds chunk size %d", ErrShardCorrupt, chunkIdx, plainLen, r.chunkSize)
	}
	if cipherLen != plainLen+uint32(r.overhead) {
		return fmt.Errorf("%w: encrypted shard chunk %d ciphertext length %d != %d+%d", ErrShardCorrupt, chunkIdx, cipherLen, plainLen, r.overhead)
	}
	if inChunk >= int(plainLen) {
		// Clean end-of-stream at a chunk boundary: NOT corruption.
		return io.EOF
	}

	ciphertext := make([]byte, cipherLen)
	if _, err := r.r.ReadAt(ciphertext, chunkFileOffset+encryptedChunkHeaderLen); err != nil {
		// The header declared cipherLen bytes; a short ReadAt (io.EOF) means the
		// payload is truncated → corruption. Other errors are transient.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("read encrypted shard chunk: %w: %w", ErrShardCorrupt, err)
		}
		return fmt.Errorf("read encrypted shard chunk: %w", err)
	}
	// seam Open failure on an owned shard = corruption, not a key-version race;
	// re-audit when DEKKeeperAdapter is wired in slice C.
	plaintext, err := r.enc.Open(encrypt.DomainShard, chunkFields(r.baseFields, chunkIdx), r.gen, ciphertext)
	if err != nil {
		return fmt.Errorf("decrypt shard chunk %d: %w: %w", chunkIdx, ErrShardCorrupt, err)
	}
	if uint32(len(plaintext)) != plainLen {
		return fmt.Errorf("%w: encrypted shard chunk %d plaintext length mismatch: got %d, want %d", ErrShardCorrupt, chunkIdx, len(plaintext), plainLen)
	}

	end := len(plaintext)
	if max := inChunk + int(r.remaining); max < end {
		end = max
	}
	r.plain = plaintext[inChunk:end]
	return nil
}

// WriteEncryptedShardStreamAtomic writes a chunked encrypted shard from r
// using the same tmp + sync + rename recipe as WriteShardStreamAtomic.
func WriteEncryptedShardStreamAtomic(path string, r io.Reader, enc ShardEncryptor, baseFields []encrypt.AADField, chunkSize int) error {
	return writeEncryptedShardStreamAtomic(path, r, enc, baseFields, chunkSize, true)
}

func writeEncryptedShardStreamAtomic(path string, r io.Reader, enc ShardEncryptor, baseFields []encrypt.AADField, chunkSize int, mkdir bool) error {
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
	if err := EncodeEncryptedShard(f, r, enc, baseFields, chunkSize); err != nil {
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

// writeEncryptedShardHeader writes the GFSENC3 fixed header.
func writeEncryptedShardHeader(w io.Writer, dekGen uint32, chunkSize uint32, chunkOverhead uint16) error {
	var hdr [encryptedHeaderLen]byte
	copy(hdr[:], encryptedShardMagic)
	binary.LittleEndian.PutUint16(hdr[8:10], encryptedShardFormatVersion)
	binary.LittleEndian.PutUint32(hdr[10:14], dekGen)
	binary.LittleEndian.PutUint32(hdr[14:18], chunkSize)
	binary.LittleEndian.PutUint16(hdr[18:20], chunkOverhead)
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("write encrypted shard header: %w", err)
	}
	return nil
}

// parseEncryptedShardHeader validates a header already read into hdr and
// returns (dek_gen, chunk_size, chunk_overhead). Truncation/format errors wrap
// ErrShardCorrupt; callers that read via io.ReadFull/ReadAt pre-classify EOF.
func parseEncryptedShardHeader(hdr []byte) (gen uint32, chunkSize uint32, overhead uint16, err error) {
	if !IsEncryptedShard(hdr) {
		return 0, 0, 0, fmt.Errorf("%w: not an encrypted shard", ErrShardCorrupt)
	}
	if v := binary.LittleEndian.Uint16(hdr[8:10]); v != encryptedShardFormatVersion {
		return 0, 0, 0, fmt.Errorf("%w: unsupported encrypted shard format version %d", ErrShardCorrupt, v)
	}
	gen = binary.LittleEndian.Uint32(hdr[10:14])
	chunkSize = binary.LittleEndian.Uint32(hdr[14:18])
	overhead = binary.LittleEndian.Uint16(hdr[18:20])
	if chunkSize == 0 || chunkSize > maxEncryptedChunkSize {
		return 0, 0, 0, fmt.Errorf("%w: invalid encrypted shard chunk size: %d", ErrShardCorrupt, chunkSize)
	}
	return gen, chunkSize, overhead, nil
}

// readEncryptedShardHeader reads + parses the fixed header from a stream.
func readEncryptedShardHeader(r io.Reader) (gen uint32, chunkSize uint32, overhead uint16, err error) {
	var hdr [encryptedHeaderLen]byte
	if _, e := io.ReadFull(r, hdr[:]); e != nil {
		if errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF) {
			return 0, 0, 0, fmt.Errorf("read encrypted shard header: %w: %w", ErrShardCorrupt, e)
		}
		return 0, 0, 0, fmt.Errorf("read encrypted shard header: %w", e)
	}
	return parseEncryptedShardHeader(hdr[:])
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
