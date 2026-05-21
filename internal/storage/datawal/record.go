package datawal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/gritive/GrainFS/internal/encrypt"
)

const MaxPayloadBytes = 64 << 20

const (
	OpSegmentPut byte = iota + 1
	OpObjectWriteAt
	OpObjectTruncate
	OpShardPut
	OpShardPackPut
	OpShardPackDelete
	OpSpoolPut
)

const (
	maxRecordBodyBytes = MaxPayloadBytes + 1<<20
	maxMetadataBytes   = 1 << 20
	encryptedRecordAAD = "datawal:record:v1"
)

var ErrChecksumMismatch = errors.New("datawal: checksum mismatch")

type Record struct {
	Seq       uint64
	Timestamp int64
	Op        byte
	Bucket    string
	Key       string
	Target    string
	Offset    int64
	Size      int64
	Payload   []byte
	Checksum  []byte
}

func EncodeRecord(w io.Writer, rec Record) error {
	body, err := marshalRecordBody(rec)
	if err != nil {
		return err
	}
	return writeFrame(w, body)
}

func DecodeRecord(r io.Reader) (Record, error) {
	body, err := readFrame(r)
	if err != nil {
		return Record{}, err
	}
	return unmarshalRecordBody(body)
}

func EncodeEncryptedRecord(w io.Writer, rec Record, enc *encrypt.Encryptor) error {
	if enc == nil {
		return fmt.Errorf("datawal: encrypted record requires encryptor")
	}
	body, err := marshalRecordBody(rec)
	if err != nil {
		return err
	}
	sealed, err := enc.SealValueAADTo(nil, []byte(encryptedRecordAAD), body)
	clear(body)
	if err != nil {
		return fmt.Errorf("datawal: encrypt record: %w", err)
	}
	err = writeFrame(w, sealed)
	clear(sealed)
	return err
}

func DecodeEncryptedRecord(r io.Reader, enc *encrypt.Encryptor) (Record, error) {
	if enc == nil {
		return Record{}, fmt.Errorf("datawal: encrypted record requires encryptor")
	}
	sealed, err := readFrame(r)
	if err != nil {
		return Record{}, err
	}
	body, err := enc.OpenValueAAD([]byte(encryptedRecordAAD), sealed)
	if err != nil {
		return Record{}, fmt.Errorf("datawal: decrypt record: %w", err)
	}
	rec, err := unmarshalRecordBody(body)
	clear(body)
	if err != nil {
		return Record{}, err
	}
	return rec, nil
}

// PayloadReader returns a reader over the decoded in-memory payload.
func PayloadReader(rec Record) io.Reader {
	return bytes.NewReader(rec.Payload)
}

func writeFrame(w io.Writer, body []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if err := writeAll(w, hdr[:]); err != nil {
		return err
	}
	if err := writeAll(w, body); err != nil {
		return err
	}
	var tail [4]byte
	binary.BigEndian.PutUint32(tail[:], crc32.ChecksumIEEE(body))
	return writeAll(w, tail[:])
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	bodyLen := binary.BigEndian.Uint32(hdr[:])
	if bodyLen > maxRecordBodyBytes {
		return nil, fmt.Errorf("datawal: record too large: %d", bodyLen)
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	var tail [4]byte
	if _, err := io.ReadFull(r, tail[:]); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(tail[:]) {
		return nil, ErrChecksumMismatch
	}
	return body, nil
}

func marshalRecordBody(rec Record) ([]byte, error) {
	if len(rec.Payload) > MaxPayloadBytes {
		return nil, fmt.Errorf("datawal: payload too large: %d", len(rec.Payload))
	}
	bucket := []byte(rec.Bucket)
	key := []byte(rec.Key)
	target := []byte(rec.Target)
	if len(bucket)+len(key)+len(target) > maxMetadataBytes {
		return nil, fmt.Errorf("datawal: metadata too large")
	}
	checksum := sha256.Sum256(rec.Payload)
	size := 8 + 8 + 1 + 8 + 8 + 4 + len(bucket) + 4 + len(key) + 4 + len(target) + 32 + 8 + len(rec.Payload)
	if size > maxRecordBodyBytes {
		return nil, fmt.Errorf("datawal: record body too large: %d", size)
	}
	body := make([]byte, size)
	off := 0
	binary.BigEndian.PutUint64(body[off:], rec.Seq)
	off += 8
	binary.BigEndian.PutUint64(body[off:], uint64(rec.Timestamp))
	off += 8
	body[off] = rec.Op
	off++
	binary.BigEndian.PutUint64(body[off:], uint64(rec.Offset))
	off += 8
	binary.BigEndian.PutUint64(body[off:], uint64(rec.Size))
	off += 8
	off = putBytes(body, off, bucket)
	off = putBytes(body, off, key)
	off = putBytes(body, off, target)
	copy(body[off:], checksum[:])
	off += sha256.Size
	binary.BigEndian.PutUint64(body[off:], uint64(len(rec.Payload)))
	off += 8
	copy(body[off:], rec.Payload)
	return body, nil
}

func unmarshalRecordBody(body []byte) (Record, error) {
	var rec Record
	off := 0
	if len(body) < 8+8+1+8+8 {
		return rec, io.ErrUnexpectedEOF
	}
	rec.Seq = binary.BigEndian.Uint64(body[off:])
	off += 8
	rec.Timestamp = int64(binary.BigEndian.Uint64(body[off:]))
	off += 8
	rec.Op = body[off]
	off++
	rec.Offset = int64(binary.BigEndian.Uint64(body[off:]))
	off += 8
	rec.Size = int64(binary.BigEndian.Uint64(body[off:]))
	off += 8

	var err error
	if rec.Bucket, off, err = readString(body, off); err != nil {
		return rec, err
	}
	if rec.Key, off, err = readString(body, off); err != nil {
		return rec, err
	}
	if rec.Target, off, err = readString(body, off); err != nil {
		return rec, err
	}
	if len(body[off:]) < sha256.Size+8 {
		return rec, io.ErrUnexpectedEOF
	}
	rec.Checksum = append([]byte(nil), body[off:off+sha256.Size]...)
	off += sha256.Size
	payloadLen := binary.BigEndian.Uint64(body[off:])
	off += 8
	if payloadLen > MaxPayloadBytes {
		return rec, fmt.Errorf("datawal: payload too large: %d", payloadLen)
	}
	if uint64(len(body[off:])) < payloadLen {
		return rec, io.ErrUnexpectedEOF
	}
	rec.Payload = append([]byte(nil), body[off:off+int(payloadLen)]...)
	sum := sha256.Sum256(rec.Payload)
	if !bytes.Equal(rec.Checksum, sum[:]) {
		return rec, ErrChecksumMismatch
	}
	return rec, nil
}

func putBytes(dst []byte, off int, data []byte) int {
	binary.BigEndian.PutUint32(dst[off:], uint32(len(data)))
	off += 4
	copy(dst[off:], data)
	return off + len(data)
}

func readString(body []byte, off int) (string, int, error) {
	if len(body[off:]) < 4 {
		return "", off, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint32(body[off:]))
	off += 4
	if len(body[off:]) < n {
		return "", off, io.ErrUnexpectedEOF
	}
	return string(body[off : off+n]), off + n, nil
}
