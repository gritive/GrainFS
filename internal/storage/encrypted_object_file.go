package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/gritive/GrainFS/internal/encrypt"
)

const (
	encryptedObjectMagic         = "GFOBJENC2"
	encryptedObjectFormatVersion = uint16(1)
	encryptedChunkSize           = 128 * 1024 // Balance write overhead with bounded ReadAt decrypt work.
	// encryptedObjectHeaderLen is the on-disk header size before the first
	// record: magic + format_version(2) + dek_gen(4).
	encryptedObjectHeaderLen = len(encryptedObjectMagic) + 6
)

// writeEncryptedObjectHeader writes the GFOBJENC2 file header: magic,
// format_version, and the dek_gen all chunks in the file were sealed under.
func writeEncryptedObjectHeader(w io.Writer, dekGen uint32) error {
	if _, err := w.Write([]byte(encryptedObjectMagic)); err != nil {
		return fmt.Errorf("write encrypted object magic: %w", err)
	}
	var hdr [6]byte
	binary.BigEndian.PutUint16(hdr[0:2], encryptedObjectFormatVersion)
	binary.BigEndian.PutUint32(hdr[2:6], dekGen)
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("write encrypted object header: %w", err)
	}
	return nil
}

// readEncryptedObjectHeader validates the magic + format_version and returns
// the dek_gen the file's chunks were sealed under.
func readEncryptedObjectHeader(r io.Reader) (uint32, error) {
	magic := make([]byte, len(encryptedObjectMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return 0, fmt.Errorf("read encrypted object magic: %w", err)
	}
	if string(magic) != encryptedObjectMagic {
		return 0, fmt.Errorf("invalid encrypted object magic")
	}
	var hdr [6]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, fmt.Errorf("read encrypted object header: %w", err)
	}
	if v := binary.BigEndian.Uint16(hdr[0:2]); v != encryptedObjectFormatVersion {
		return 0, fmt.Errorf("unsupported encrypted object format version %d", v)
	}
	return binary.BigEndian.Uint32(hdr[2:6]), nil
}

// writeEncryptedObjectFile streams r into the GFOBJENC2 format at path,
// sealing each plaintext chunk via enc under DomainShard with baseFields plus
// the per-chunk ordinal. plainSink receives the unsealed bytes for digesting
// (pass io.Discard when no digest is needed). Returns the plaintext byte count.
//
// All chunks are sealed under one pinned generation: the gen returned by the
// first chunk is captured and written into the file header; if a later chunk
// seals at a different gen (e.g. a rotation raced the write) the whole write
// fails so the header's dek_gen always describes every chunk.
func writeEncryptedObjectFile(path string, enc DataEncryptor, baseFields []encrypt.AADField, r io.Reader, plainSink io.Writer) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, fmt.Errorf("create encrypted object: %w", err)
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 1<<20)

	// fields is baseFields with one extra slot for the per-chunk ordinal,
	// rewritten each iteration to avoid per-chunk allocation.
	fields := make([]encrypt.AADField, len(baseFields)+1)
	copy(fields, baseFields)
	ordinalIdx := len(baseFields)

	buf := make([]byte, encryptedChunkSize)
	var size int64
	var chunk uint64
	var pinnedGen uint32
	var headerWritten bool
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			plain := buf[:n]
			if plainSink != nil {
				_, _ = plainSink.Write(plain)
			}
			fields[ordinalIdx] = encrypt.FieldUint32(uint32(chunk))
			sealed, gen, err := enc.Seal(encrypt.DomainShard, fields, plain)
			if err != nil {
				return 0, fmt.Errorf("encrypt object chunk %d: %w", chunk, err)
			}
			if chunk == 0 {
				pinnedGen = gen
				if err := writeEncryptedObjectHeader(bw, pinnedGen); err != nil {
					return 0, err
				}
				headerWritten = true
			} else if gen != pinnedGen {
				return 0, fmt.Errorf("encrypt object chunk %d sealed at gen %d, pinned %d", chunk, gen, pinnedGen)
			}
			if err := writeEncryptedObjectRecord(bw, uint32(n), sealed); err != nil {
				return 0, err
			}
			size += int64(n)
			chunk++
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, fmt.Errorf("read object plaintext: %w", readErr)
		}
	}
	if !headerWritten {
		// Empty object: still emit a valid header (gen 0) so open succeeds.
		if err := writeEncryptedObjectHeader(bw, pinnedGen); err != nil {
			return 0, err
		}
	}
	if err := bw.Flush(); err != nil {
		return 0, fmt.Errorf("flush encrypted object: %w", err)
	}
	// Durability is owned by internal/storage/datawal. The tmp+rename below
	// provides atomic visibility of already-WAL-flushed bytes.
	return size, nil
}

func openEncryptedObjectFile(path string, enc DataEncryptor, baseFields []encrypt.AADField, size int64) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	gen, err := readEncryptedObjectHeader(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &encryptedObjectReader{
		f:          f,
		enc:        enc,
		baseFields: baseFields,
		gen:        gen,
		remaining:  size,
	}, nil
}

type encryptedObjectReader struct {
	f          *os.File
	enc        DataEncryptor
	baseFields []encrypt.AADField
	gen        uint32
	chunk      uint64
	remaining  int64
	// buf holds the current chunk's plaintext, drained by Read(). After the
	// last byte is consumed, the underlying array is reused as the destination
	// for the next chunk's decrypt — eliminating one heap allocation per chunk.
	// Read() clears bytes as they leave the buffer (security), and the chunk
	// boundary truncation clears the discarded tail, so reused capacity always
	// starts zeroed.
	buf []byte
	// sealedBuf is reusable scratch for the on-disk sealed record body. It
	// grows to chunk-class size on the first chunk and stays there for the
	// lifetime of the reader.
	sealedBuf []byte
	err       error
}

func (r *encryptedObjectReader) Read(p []byte) (int, error) {
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

func (r *encryptedObjectReader) Close() error {
	// Zero all reusable scratch on close so any retained plaintext or
	// sealed-record bytes never linger past the reader's lifetime.
	if len(r.buf) > 0 {
		clear(r.buf)
	}
	if cap(r.sealedBuf) > 0 {
		clear(r.sealedBuf[:cap(r.sealedBuf)])
	}
	return r.f.Close()
}

func (r *encryptedObjectReader) loadNext() error {
	if r.remaining <= 0 {
		return io.EOF
	}
	plainLen, sealed, err := readEncryptedObjectRecordInto(r.f, r.sealedBuf[:0])
	if err != nil {
		if err == io.EOF {
			if r.remaining > 0 {
				return io.ErrUnexpectedEOF
			}
			return io.EOF
		}
		return err
	}
	r.sealedBuf = sealed
	fields := append(append([]encrypt.AADField(nil), r.baseFields...), encrypt.FieldUint32(uint32(r.chunk)))
	plain, err := r.enc.Open(encrypt.DomainShard, fields, r.gen, sealed)
	clear(sealed)
	if err != nil {
		return fmt.Errorf("decrypt object chunk %d: %w", r.chunk, err)
	}
	if len(plain) != int(plainLen) {
		clear(plain)
		return fmt.Errorf("encrypted object chunk %d length mismatch", r.chunk)
	}
	if int64(len(plain)) > r.remaining {
		keep := int(r.remaining)
		clear(plain[keep:])
		plain = plain[:keep]
	}
	r.remaining -= int64(len(plain))
	r.chunk++
	r.buf = plain
	return nil
}

func readAtEncryptedObjectFile(path string, enc DataEncryptor, baseFields []encrypt.AADField, size int64, offset int64, buf []byte) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if offset >= size {
		return 0, io.EOF
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	gen, err := readEncryptedObjectHeader(f)
	if err != nil {
		return 0, err
	}

	var (
		copied    int
		chunk     uint64
		plainPos  int64
		sealedBuf []byte
		plainBuf  []byte
	)
	// Zero scratch on exit so plaintext / sealed bytes never linger past the
	// call. Done as deferred clears rather than function-end inline so early
	// returns (errors) still wipe.
	defer func() {
		if cap(sealedBuf) > 0 {
			clear(sealedBuf[:cap(sealedBuf)])
		}
		if cap(plainBuf) > 0 {
			clear(plainBuf[:cap(plainBuf)])
		}
	}()
	for copied < len(buf) && plainPos < size {
		var hdr [8]byte
		if _, err := io.ReadFull(f, hdr[:]); err != nil {
			if err == io.EOF {
				break
			}
			return copied, fmt.Errorf("read encrypted object record header: %w", err)
		}
		plainLen := binary.BigEndian.Uint32(hdr[0:4])
		blobLen := binary.BigEndian.Uint32(hdr[4:8])
		if blobLen > 256*1024*1024 {
			return copied, fmt.Errorf("encrypted object record too large")
		}
		if err := validateEncryptedObjectRecordPlainLen(chunk, plainPos, size, plainLen); err != nil {
			return copied, err
		}
		chunkStart := plainPos
		chunkEnd := plainPos + int64(plainLen)
		if chunkEnd > size {
			chunkEnd = size
		}

		needEnd := offset + int64(len(buf))
		if chunkEnd <= offset || chunkStart >= needEnd {
			if _, err := f.Seek(int64(blobLen), io.SeekCurrent); err != nil {
				return copied, fmt.Errorf("skip encrypted object record body: %w", err)
			}
			plainPos += int64(plainLen)
			chunk++
			continue
		}

		// Grow sealedBuf only on the first matched chunk (or when blobLen
		// exceeds prior cap); reuse the backing array on subsequent chunks.
		if cap(sealedBuf) < int(blobLen) {
			sealedBuf = make([]byte, blobLen)
		} else {
			sealedBuf = sealedBuf[:blobLen]
		}
		if _, err := io.ReadFull(f, sealedBuf); err != nil {
			return copied, fmt.Errorf("read encrypted object record body: %w", err)
		}
		fields := append(append([]encrypt.AADField(nil), baseFields...), encrypt.FieldUint32(uint32(chunk)))
		plain, err := enc.Open(encrypt.DomainShard, fields, gen, sealedBuf)
		clear(sealedBuf)
		if err != nil {
			return copied, fmt.Errorf("decrypt object chunk %d: %w", chunk, err)
		}
		plainBuf = plain
		if len(plain) != int(plainLen) {
			clear(plain)
			return copied, fmt.Errorf("encrypted object chunk %d length mismatch", chunk)
		}
		readStart := offset
		if readStart < chunkStart {
			readStart = chunkStart
		}
		readEnd := needEnd
		if readEnd > chunkEnd {
			readEnd = chunkEnd
		}
		srcStart := int(readStart - chunkStart)
		srcEnd := int(readEnd - chunkStart)
		copied += copy(buf[copied:], plain[srcStart:srcEnd])
		clear(plain)
		plainPos += int64(plainLen)
		chunk++
	}
	if copied < len(buf) {
		return copied, io.EOF
	}
	return copied, nil
}

func writeAtEncryptedObjectFile(path string, enc DataEncryptor, baseFields []encrypt.AADField, offset uint64, data []byte, currentSize int64) (int64, string, error) {
	plain, err := readEncryptedObjectFile(path, enc, baseFields, currentSize)
	if err != nil {
		if !os.IsNotExist(err) || currentSize != 0 {
			return 0, "", err
		}
		plain = nil
	}
	if int64(len(plain)) > currentSize {
		plain = plain[:currentSize]
	}
	maxInt := int(^uint(0) >> 1)
	if offset > uint64(maxInt-len(data)) {
		return 0, "", fmt.Errorf("encrypted writeat offset too large: %d", offset)
	}
	off := int(offset)
	end := off + len(data)
	if end > len(plain) {
		extended := make([]byte, end)
		copy(extended, plain)
		plain = extended
	}
	copy(plain[off:], data)
	h, release := hashForBucket("")
	defer release()
	size, err := writeEncryptedObjectFileAtomic(path, enc, baseFields, bytes.NewReader(plain), h)
	if err != nil {
		return 0, "", err
	}
	return size, etagFromHash(h), nil
}

func truncateEncryptedObjectFile(path string, enc DataEncryptor, baseFields []encrypt.AADField, currentSize int64, newSize int64) (int64, error) {
	if newSize < 0 {
		return 0, fmt.Errorf("negative size")
	}
	plain, err := readEncryptedObjectFile(path, enc, baseFields, currentSize)
	if err != nil {
		return 0, err
	}
	if int64(len(plain)) > currentSize {
		plain = plain[:currentSize]
	}
	if newSize <= int64(len(plain)) {
		plain = plain[:newSize]
	} else {
		extended := make([]byte, newSize)
		copy(extended, plain)
		plain = extended
	}
	_, err = writeEncryptedObjectFileAtomic(path, enc, baseFields, bytes.NewReader(plain), io.Discard)
	if err != nil {
		return 0, err
	}
	return newSize, nil
}

func writeEncryptedObjectFileAtomic(path string, enc DataEncryptor, baseFields []encrypt.AADField, r io.Reader, plainSink io.Writer) (int64, error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".encrypted-object-*")
	if err != nil {
		return 0, fmt.Errorf("create encrypted object temp: %w", err)
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return 0, fmt.Errorf("close encrypted object temp: %w", err)
	}
	cleanup := func() {
		_ = os.Remove(tmpPath)
	}

	size, err := writeEncryptedObjectFile(tmpPath, enc, baseFields, r, plainSink)
	if err != nil {
		cleanup()
		return 0, err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		cleanup()
		return 0, fmt.Errorf("rename encrypted object: %w", err)
	}
	// Directory metadata durability is owned by the data WAL: the WAL
	// record was flushed before this materialization ran, so a crash
	// after rename and before the next natural dir sync replays the
	// same bytes from the WAL.
	return size, nil
}

func readEncryptedObjectFile(path string, enc DataEncryptor, baseFields []encrypt.AADField, expectedSize int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gen, err := readEncryptedObjectHeader(f)
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	var (
		chunk     uint64
		sealedBuf []byte
		plainBuf  []byte
	)
	defer func() {
		if cap(sealedBuf) > 0 {
			clear(sealedBuf[:cap(sealedBuf)])
		}
		if cap(plainBuf) > 0 {
			clear(plainBuf[:cap(plainBuf)])
		}
	}()
	for {
		plainLen, sealed, err := readEncryptedObjectRecordInto(f, sealedBuf[:0])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		sealedBuf = sealed
		fields := append(append([]encrypt.AADField(nil), baseFields...), encrypt.FieldUint32(uint32(chunk)))
		plain, err := enc.Open(encrypt.DomainShard, fields, gen, sealed)
		if err != nil {
			return nil, fmt.Errorf("decrypt object chunk %d: %w", chunk, err)
		}
		plainBuf = plain
		if len(plain) != int(plainLen) {
			return nil, fmt.Errorf("encrypted object chunk %d length mismatch", chunk)
		}
		_, _ = out.Write(plain)
		chunk++
	}
	if expectedSize >= 0 && int64(out.Len()) < expectedSize {
		return nil, io.ErrUnexpectedEOF
	}
	return out.Bytes(), nil
}

func hashEncryptedObjectFile(path string, enc DataEncryptor, baseFields []encrypt.AADField, h hash.Hash) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	gen, err := readEncryptedObjectHeader(f)
	if err != nil {
		return 0, err
	}

	var size int64
	var (
		chunk     uint64
		sealedBuf []byte
		plainBuf  []byte
	)
	defer func() {
		if cap(sealedBuf) > 0 {
			clear(sealedBuf[:cap(sealedBuf)])
		}
		if cap(plainBuf) > 0 {
			clear(plainBuf[:cap(plainBuf)])
		}
	}()
	for {
		plainLen, sealed, err := readEncryptedObjectRecordInto(f, sealedBuf[:0])
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		sealedBuf = sealed
		fields := append(append([]encrypt.AADField(nil), baseFields...), encrypt.FieldUint32(uint32(chunk)))
		plain, err := enc.Open(encrypt.DomainShard, fields, gen, sealed)
		clear(sealed)
		if err != nil {
			return 0, fmt.Errorf("decrypt object chunk %d: %w", chunk, err)
		}
		plainBuf = plain
		if len(plain) != int(plainLen) {
			clear(plain)
			return 0, fmt.Errorf("encrypted object chunk %d length mismatch", chunk)
		}
		_, _ = h.Write(plain)
		size += int64(len(plain))
		clear(plain)
		chunk++
	}
	return size, nil
}

func encryptedObjectFilePlainSize(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	if _, err := readEncryptedObjectHeader(f); err != nil {
		return 0, err
	}

	var size int64
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(f, hdr[:]); err != nil {
			if err == io.EOF {
				return size, nil
			}
			return 0, fmt.Errorf("read encrypted object record header: %w", err)
		}
		plainLen := binary.BigEndian.Uint32(hdr[0:4])
		blobLen := binary.BigEndian.Uint32(hdr[4:8])
		if blobLen > 256*1024*1024 {
			return 0, fmt.Errorf("encrypted object record too large")
		}
		if _, err := f.Seek(int64(blobLen), io.SeekCurrent); err != nil {
			return 0, fmt.Errorf("skip encrypted object record body: %w", err)
		}
		size += int64(plainLen)
	}
}

func writeEncryptedObjectRecord(w io.Writer, plainLen uint32, sealed []byte) error {
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], plainLen)
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(sealed)))
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("write encrypted object record header: %w", err)
	}
	if _, err := w.Write(sealed); err != nil {
		return fmt.Errorf("write encrypted object record body: %w", err)
	}
	return nil
}

// readEncryptedObjectRecordInto reads one sealed record into dst, growing
// dst only when its capacity is too small. Callers in tight chunk loops
// reuse the same buffer across iterations so the per-chunk
// `make([]byte, blobLen)` cost disappears after the first record.
func readEncryptedObjectRecordInto(r io.Reader, dst []byte) (uint32, []byte, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, nil, err
		}
		return 0, nil, fmt.Errorf("read encrypted object record header: %w", err)
	}
	plainLen := binary.BigEndian.Uint32(hdr[0:4])
	blobLen := binary.BigEndian.Uint32(hdr[4:8])
	if blobLen > 256*1024*1024 {
		return 0, nil, fmt.Errorf("encrypted object record too large")
	}
	if cap(dst) < int(blobLen) {
		dst = make([]byte, blobLen)
	} else {
		dst = dst[:blobLen]
	}
	if _, err := io.ReadFull(r, dst); err != nil {
		return 0, nil, fmt.Errorf("read encrypted object record body: %w", err)
	}
	return plainLen, dst, nil
}

func validateEncryptedObjectRecordPlainLen(chunk uint64, plainPos int64, size int64, plainLen uint32) error {
	if plainLen == 0 {
		return fmt.Errorf("encrypted object chunk %d has empty plaintext length", chunk)
	}
	if plainLen > encryptedChunkSize {
		return fmt.Errorf("encrypted object chunk %d length exceeds chunk size", chunk)
	}
	expected := int64(encryptedChunkSize)
	remaining := size - plainPos
	if remaining < expected {
		expected = remaining
	}
	if expected <= 0 {
		return fmt.Errorf("encrypted object chunk %d starts past object size", chunk)
	}
	if int64(plainLen) != expected {
		return fmt.Errorf("encrypted object chunk %d header length mismatch", chunk)
	}
	return nil
}
