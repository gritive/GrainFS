package cluster

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

var md5Pool = sync.Pool{
	New: func() any {
		return md5.New()
	},
}

const partRecordCopyBufferSize = 1 << 20
const maxEncryptedPartRecordBlobBytes = 2 * partRecordCopyBufferSize
const encryptedPartRecordCipherBufferSize = partRecordCopyBufferSize + 64

var partRecordCopyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, partRecordCopyBufferSize)
		return &buf
	},
}

var encryptedPartRecordCipherBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, encryptedPartRecordCipherBufferSize)
		return &buf
	},
}

// copyChunked copies src to dst while forcing chunked Writes no
// larger than partRecordCopyBufferSize. Callers writing into an encrypted
// part record stream must use this helper so the receiver-side
// maxEncryptedPartRecordBlobBytes invariant cannot be tripped by readers
// that implement WriteTo (e.g. *bytes.Reader) or by upstream HTTP
// frameworks that hand the body in 5 MiB+ slabs.
func copyChunked(dst io.Writer, src io.Reader) (int64, error) {
	bp := partRecordCopyBufferPool.Get().(*[]byte)
	defer partRecordCopyBufferPool.Put(bp)
	type readerOnly struct{ io.Reader }
	return io.CopyBuffer(dst, readerOnly{src}, *bp)
}

// encryptedPartRecordWriter / openEncryptedPartRecordFile and the record codec
// below stage encrypted multipart PART bytes to disk (UploadPart →
// openMultipartPart). The PUT-body disk staging was removed; this per-record
// codec remains because multipart parts arrive across separate requests and are
// fundamentally disk-resident.
type encryptedPartRecordWriter struct {
	w         io.Writer
	seam      storage.DataEncryptor
	domain    string
	record    uint64
	cipherBuf []byte // writer-owned ciphertext buffer, reused across records
	aadFields []encrypt.AADField
}

func (w *encryptedPartRecordWriter) Write(p []byte) (int, error) {
	if uint64(len(p)) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("encrypted part record too large: %d", len(p))
	}
	w.aadFields = partRecordAADFieldsInto(w.aadFields, w.domain, w.record)
	blob, gen, err := w.seam.SealTo(w.cipherBuf[:0], encrypt.DomainSpool, w.aadFields, p)
	if err != nil {
		return 0, err
	}
	if uint64(len(blob)) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("encrypted part record blob too large: %d", len(blob))
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

func openEncryptedPartRecordFile(path string, seam storage.DataEncryptor, domain string) (io.ReadCloser, error) {
	if seam == nil {
		return nil, fmt.Errorf("open encrypted part record: nil seam")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	plainRef := partRecordCopyBufferPool.Get().(*[]byte)
	cipherRef := encryptedPartRecordCipherBufferPool.Get().(*[]byte)
	return &encryptedPartRecordReader{
		f:         f,
		seam:      seam,
		domain:    domain,
		plain:     (*plainRef)[:0],
		cipherBuf: (*cipherRef)[:0],
		plainRef:  plainRef,
		cipherRef: cipherRef,
	}, nil
}

type encryptedPartRecordReader struct {
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

func (r *encryptedPartRecordReader) Read(p []byte) (int, error) {
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

func (r *encryptedPartRecordReader) Close() error {
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
		partRecordCopyBufferPool.Put(r.plainRef)
		r.plainRef = nil
	}
	if r.cipherRef != nil {
		clear((*r.cipherRef)[:cap(*r.cipherRef)])
		encryptedPartRecordCipherBufferPool.Put(r.cipherRef)
		r.cipherRef = nil
	}
	return r.f.Close()
}

func (r *encryptedPartRecordReader) loadNext() error {
	// Reuse the reader-owned plaintext/ciphertext buffers. Safe because the
	// previous record is fully drained before loadNext is called (Read loops
	// while len(r.buf)==0) and the plaintext only ever exits via Read's copy.
	plain, cipher, aadFields, done, err := readEncryptedPartRecord(r.f, r.seam, r.domain, r.record, r.plain[:0], r.cipherBuf[:0], r.aadFields)
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

func readEncryptedPartRecord(r io.Reader, seam storage.DataEncryptor, domain string, record uint64, plainDst, cipherDst []byte, aadDst []encrypt.AADField) (plain, cipher []byte, aadFields []encrypt.AADField, done bool, err error) {
	var header [12]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if err == io.EOF {
			return nil, cipherDst, aadDst, true, nil
		}
		return nil, cipherDst, aadDst, false, fmt.Errorf("read encrypted part record header: %w", err)
	}
	plainLen := binary.BigEndian.Uint32(header[:4])
	blobLen := binary.BigEndian.Uint32(header[4:8])
	gen := binary.BigEndian.Uint32(header[8:])
	if blobLen == 0 {
		return nil, cipherDst, aadDst, false, fmt.Errorf("read encrypted part record: empty blob")
	}
	if blobLen > maxEncryptedPartRecordBlobBytes {
		return nil, cipherDst, aadDst, false, fmt.Errorf("read encrypted part record: blob too large: %d", blobLen)
	}
	if cap(cipherDst) < int(blobLen) {
		cipherDst = make([]byte, blobLen)
	}
	blob := cipherDst[:blobLen]
	if _, err := io.ReadFull(r, blob); err != nil {
		return nil, blob, aadDst, false, fmt.Errorf("read encrypted part record blob: %w", err)
	}
	aadFields = partRecordAADFieldsInto(aadDst, domain, record)
	out, err := seam.OpenTo(plainDst[:0], encrypt.DomainSpool, aadFields, gen, blob)
	clear(blob) // zeroize ciphertext after decrypt; buffer is reused next record
	if err != nil {
		// cipher.AEAD.Open may overwrite dst up to capacity even on failure.
		// plainDst is the reader-owned reusable buffer, so wipe its full
		// capacity to leave no unauthenticated plaintext residue (defense in
		// depth: Go's GCM already zeroes on auth failure, but the AEAD
		// contract does not guarantee it).
		clear(plainDst[:cap(plainDst)])
		return nil, blob, aadFields, false, fmt.Errorf("open encrypted part record: %w", err)
	}
	if len(out) != int(plainLen) {
		clear(out[:cap(out)])
		return nil, blob, aadFields, false, fmt.Errorf("open encrypted part record: plaintext size mismatch")
	}
	return out, blob, aadFields, false, nil
}

// partRecordAADFieldsInto binds the per-record domain string (e.g.
// "cluster-multipart-part:<uploadID>:<n>") AND the record index into the AAD
// under DomainSpool. Including the record index keeps per-frame positional
// binding (records within one part file cannot be reordered, duplicated, or
// spliced without AEAD failure — preserving the old domain+record AAD). The
// domain string must be identical on write and read; the encrypted
// multipart-part codec recomputes it deterministically from IDs
// (uploadID/partNumber) — never from a filesystem path.
func partRecordAADFieldsInto(dst []encrypt.AADField, domain string, record uint64) []encrypt.AADField {
	if cap(dst) < 2 {
		dst = make([]encrypt.AADField, 2)
	} else {
		dst = dst[:2]
	}
	dst[0] = encrypt.FieldString(domain)
	dst[1] = encrypt.FieldUint64(record)
	return dst
}
