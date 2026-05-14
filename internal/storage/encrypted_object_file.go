package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"
	"strconv"

	"github.com/gritive/GrainFS/internal/encrypt"
)

const (
	encryptedObjectMagic = "GFOBJENC1"
	encryptedChunkSize   = 128 * 1024 // Balance write overhead with bounded ReadAt decrypt work.
)

func encryptedChunkAAD(domain string, chunk uint64) string {
	return fmt.Sprintf("%s:chunk:%d", domain, chunk)
}

func encryptedChunkAADBytes(dst []byte, domain string, chunk uint64) []byte {
	dst = append(dst[:0], domain...)
	dst = append(dst, ":chunk:"...)
	return strconv.AppendUint(dst, chunk, 10)
}

func writeEncryptedObjectFile(path string, enc *encrypt.Encryptor, domain string, r io.Reader) (int64, string, error) {
	h, release := hashForBucket("")
	defer release()
	return writeEncryptedObjectFileWithHash(path, enc, domain, r, h)
}

func writeEncryptedObjectFileWithHash(path string, enc *encrypt.Encryptor, domain string, r io.Reader, h hash.Hash) (int64, string, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, "", fmt.Errorf("create encrypted object: %w", err)
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 1<<20)

	if _, err := bw.Write([]byte(encryptedObjectMagic)); err != nil {
		return 0, "", fmt.Errorf("write encrypted object magic: %w", err)
	}

	buf := make([]byte, encryptedChunkSize)
	sealedBuf := make([]byte, 0, 3+12+encryptedChunkSize+enc.AEADOverhead())
	aadBuf := make([]byte, 0, len(domain)+len(":chunk:")+20)
	var size int64
	var chunk uint64
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			plain := buf[:n]
			_, _ = h.Write(plain)
			aadBuf = encryptedChunkAADBytes(aadBuf, domain, chunk)
			sealed, err := enc.SealValueAADTo(sealedBuf[:0], aadBuf, plain)
			if err != nil {
				return 0, "", fmt.Errorf("encrypt object chunk %d: %w", chunk, err)
			}
			if err := writeEncryptedObjectRecord(bw, uint32(n), sealed); err != nil {
				return 0, "", err
			}
			sealedBuf = sealed
			size += int64(n)
			chunk++
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, "", fmt.Errorf("read object plaintext: %w", readErr)
		}
	}
	if err := bw.Flush(); err != nil {
		return 0, "", fmt.Errorf("flush encrypted object: %w", err)
	}
	return size, etagFromHash(h), nil
}

func openEncryptedObjectFile(path string, enc *encrypt.Encryptor, domain string, size int64) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	magic := make([]byte, len(encryptedObjectMagic))
	if _, err := io.ReadFull(f, magic); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("read encrypted object magic: %w", err)
	}
	if string(magic) != encryptedObjectMagic {
		_ = f.Close()
		return nil, fmt.Errorf("invalid encrypted object magic")
	}
	return &encryptedObjectReader{
		f:         f,
		enc:       enc,
		domain:    domain,
		remaining: size,
	}, nil
}

type encryptedObjectReader struct {
	f         *os.File
	enc       *encrypt.Encryptor
	domain    string
	chunk     uint64
	remaining int64
	buf       []byte
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
	if len(r.buf) > 0 {
		clear(r.buf)
	}
	return r.f.Close()
}

func (r *encryptedObjectReader) loadNext() error {
	if r.remaining <= 0 {
		return io.EOF
	}
	plainLen, sealed, err := readEncryptedObjectRecord(r.f)
	if err != nil {
		if err == io.EOF {
			return io.EOF
		}
		return err
	}
	plain, err := r.enc.OpenValue(encryptedChunkAAD(r.domain, r.chunk), sealed)
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

func readAtEncryptedObjectFile(path string, enc *encrypt.Encryptor, domain string, size int64, offset int64, buf []byte) (int, error) {
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

	magic := make([]byte, len(encryptedObjectMagic))
	if _, err := io.ReadFull(f, magic); err != nil {
		return 0, fmt.Errorf("read encrypted object magic: %w", err)
	}
	if string(magic) != encryptedObjectMagic {
		return 0, fmt.Errorf("invalid encrypted object magic")
	}

	var (
		copied   int
		chunk    uint64
		plainPos int64
	)
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
		chunkStart := plainPos
		chunkEnd := plainPos + int64(plainLen)
		if chunkEnd > size {
			chunkEnd = size
		}

		needEnd := offset + int64(len(buf))
		if chunkEnd <= offset || chunkStart >= needEnd {
			if _, err := io.CopyN(io.Discard, f, int64(blobLen)); err != nil {
				return copied, fmt.Errorf("skip encrypted object record body: %w", err)
			}
			plainPos += int64(plainLen)
			chunk++
			continue
		}

		sealed := make([]byte, blobLen)
		if _, err := io.ReadFull(f, sealed); err != nil {
			return copied, fmt.Errorf("read encrypted object record body: %w", err)
		}
		plain, err := enc.OpenValue(encryptedChunkAAD(domain, chunk), sealed)
		clear(sealed)
		if err != nil {
			return copied, fmt.Errorf("decrypt object chunk %d: %w", chunk, err)
		}
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

func writeAtEncryptedObjectFile(path string, enc *encrypt.Encryptor, domain string, offset uint64, data []byte, currentSize int64) (int64, string, error) {
	plain, err := readEncryptedObjectFile(path, enc, domain)
	if err != nil {
		if !os.IsNotExist(err) || currentSize != 0 {
			return 0, "", err
		}
		plain = nil
	}
	if int64(len(plain)) > currentSize {
		plain = plain[:currentSize]
	}
	off := int(offset)
	end := off + len(data)
	if end > len(plain) {
		extended := make([]byte, end)
		copy(extended, plain)
		plain = extended
	}
	copy(plain[off:], data)
	return writeEncryptedObjectFile(path, enc, domain, bytes.NewReader(plain))
}

func truncateEncryptedObjectFile(path string, enc *encrypt.Encryptor, domain string, currentSize int64, newSize int64) (int64, error) {
	if newSize < 0 {
		return 0, fmt.Errorf("negative size")
	}
	plain, err := readEncryptedObjectFile(path, enc, domain)
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
	_, _, err = writeEncryptedObjectFile(path, enc, domain, bytes.NewReader(plain))
	if err != nil {
		return 0, err
	}
	return newSize, nil
}

func readEncryptedObjectFile(path string, enc *encrypt.Encryptor, domain string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	magic := make([]byte, len(encryptedObjectMagic))
	if _, err := io.ReadFull(f, magic); err != nil {
		return nil, fmt.Errorf("read encrypted object magic: %w", err)
	}
	if string(magic) != encryptedObjectMagic {
		return nil, fmt.Errorf("invalid encrypted object magic")
	}

	var out bytes.Buffer
	var chunk uint64
	for {
		plainLen, sealed, err := readEncryptedObjectRecord(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		plain, err := enc.OpenValue(encryptedChunkAAD(domain, chunk), sealed)
		if err != nil {
			return nil, fmt.Errorf("decrypt object chunk %d: %w", chunk, err)
		}
		if len(plain) != int(plainLen) {
			return nil, fmt.Errorf("encrypted object chunk %d length mismatch", chunk)
		}
		_, _ = out.Write(plain)
		chunk++
	}
	return out.Bytes(), nil
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

func readEncryptedObjectRecord(r io.Reader) (uint32, []byte, error) {
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
	sealed := make([]byte, blobLen)
	if _, err := io.ReadFull(r, sealed); err != nil {
		return 0, nil, fmt.Errorf("read encrypted object record body: %w", err)
	}
	return plainLen, sealed, nil
}
