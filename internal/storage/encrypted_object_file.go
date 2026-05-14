package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/gritive/GrainFS/internal/encrypt"
)

const (
	encryptedObjectMagic = "GFOBJENC1"
	encryptedChunkSize   = 64 * 1024
)

func encryptedChunkAAD(domain string, chunk uint64) string {
	return fmt.Sprintf("%s:chunk:%d", domain, chunk)
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

	if _, err := f.Write([]byte(encryptedObjectMagic)); err != nil {
		return 0, "", fmt.Errorf("write encrypted object magic: %w", err)
	}

	buf := make([]byte, encryptedChunkSize)
	var size int64
	var chunk uint64
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			plain := buf[:n]
			_, _ = h.Write(plain)
			sealed, err := enc.SealValue(encryptedChunkAAD(domain, chunk), plain)
			if err != nil {
				return 0, "", fmt.Errorf("encrypt object chunk %d: %w", chunk, err)
			}
			if err := writeEncryptedObjectRecord(f, uint32(n), sealed); err != nil {
				return 0, "", err
			}
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
	return size, etagFromHash(h), nil
}

func openEncryptedObjectFile(path string, enc *encrypt.Encryptor, domain string, size int64) (io.ReadCloser, error) {
	plain, err := readEncryptedObjectFile(path, enc, domain)
	if err != nil {
		return nil, err
	}
	if int64(len(plain)) > size {
		plain = plain[:size]
	}
	return io.NopCloser(bytes.NewReader(plain)), nil
}

func readAtEncryptedObjectFile(path string, enc *encrypt.Encryptor, domain string, size int64, offset int64, buf []byte) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if offset >= size {
		return 0, io.EOF
	}
	plain, err := readEncryptedObjectFile(path, enc, domain)
	if err != nil {
		return 0, err
	}
	if int64(len(plain)) > size {
		plain = plain[:size]
	}
	n := copy(buf, plain[offset:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
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
