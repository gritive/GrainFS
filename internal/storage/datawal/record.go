package datawal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

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

	recordBodyFixedBytes = 8 + 8 + 1 + 8 + 8 + 4 + 4 + 4 + sha256.Size + 8
)

var ErrChecksumMismatch = errors.New("datawal: checksum mismatch")

// RecordSealer is datawal's view of the storage.DataEncryptor seam. datawal
// cannot import storage (storage imports datawal), so it declares the method
// set locally; *storage.EncryptorAdapter satisfies it structurally.
type RecordSealer interface {
	Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) (ct []byte, gen uint32, err error)
	Open(domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) (plain []byte, err error)
}

// walRecordAADFields binds a record to its WAL namespace and sequence so a
// frame cannot be replayed at another position or in another WAL.
func walRecordAADFields(namespace string, seq uint64) []encrypt.AADField {
	return []encrypt.AADField{
		encrypt.FieldString(namespace),
		encrypt.FieldUint64(seq),
	}
}

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

const poolMaxBufSize = 1280 * 1024 // 1.25 MiB

var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, poolMaxBufSize)
		return &b
	},
}

func getBuffer(size int) []byte {
	if size > poolMaxBufSize {
		return make([]byte, size)
	}
	ptr := bufferPool.Get().(*[]byte)
	return (*ptr)[:size]
}

func putBuffer(buf []byte) {
	if cap(buf) < poolMaxBufSize {
		return
	}
	clear(buf[:cap(buf)])
	b := buf[:poolMaxBufSize]
	bufferPool.Put(&b)
}

func EncodeRecord(w io.Writer, rec Record) error {
	body, err := marshalRecordBody(rec)
	if err != nil {
		return err
	}
	defer putBuffer(body)
	return writeFrame(w, body)
}

func DecodeRecord(r io.Reader) (Record, error) {
	body, err := readFrame(r)
	if err != nil {
		return Record{}, err
	}
	return unmarshalRecordBody(body, false)
}

func EncodeEncryptedRecord(w io.Writer, rec Record, sealer RecordSealer, namespace string) error {
	_, err := encodeEncryptedRecordGen(w, rec, sealer, namespace)
	return err
}

// encodeEncryptedRecordGen seals and writes rec, returning the DEK generation
// the ciphertext was sealed under so the caller can assert it matches the
// segment's pinned header gen.
func encodeEncryptedRecordGen(w io.Writer, rec Record, sealer RecordSealer, namespace string) (uint32, error) {
	if sealer == nil {
		return 0, fmt.Errorf("datawal: encrypted record requires sealer")
	}
	body, err := marshalRecordBody(rec)
	if err != nil {
		return 0, err
	}
	defer putBuffer(body)

	sealed, gen, err := sealer.Seal(encrypt.DomainWAL, walRecordAADFields(namespace, rec.Seq), body)
	clear(body)
	if err != nil {
		return 0, fmt.Errorf("datawal: encrypt record: %w", err)
	}
	if len(sealed) > maxRecordBodyBytes {
		clear(sealed)
		return 0, fmt.Errorf("datawal: encrypted record body too large: %d", len(sealed))
	}
	err = writeEncryptedFrame(w, rec.Seq, sealed)
	clear(sealed)
	return gen, err
}

func DecodeEncryptedRecord(r io.Reader, sealer RecordSealer, namespace string, gen uint32) (Record, error) {
	if sealer == nil {
		return Record{}, fmt.Errorf("datawal: encrypted record requires sealer")
	}
	seq, sealed, err := readEncryptedFrame(r)
	if err != nil {
		return Record{}, err
	}
	defer clear(sealed)

	body, err := sealer.Open(encrypt.DomainWAL, walRecordAADFields(namespace, seq), gen, sealed)
	if err != nil {
		return Record{}, fmt.Errorf("datawal: decrypt record: %w", err)
	}
	rec, err := unmarshalRecordBody(body, true)
	clear(body)
	if err != nil {
		return Record{}, err
	}
	if rec.Seq != seq {
		return Record{}, fmt.Errorf("datawal: frame seq %d != record seq %d", seq, rec.Seq)
	}
	return rec, nil
}

// writeEncryptedFrame writes [seq:8][len:4][sealed][crc32(sealed):4]. The seq
// is plaintext so the reader can build the AAD before decrypting; tampering it
// makes Open fail because seq is bound into the AAD.
func writeEncryptedFrame(w io.Writer, seq uint64, sealed []byte) error {
	var prefix [8]byte
	binary.BigEndian.PutUint64(prefix[:], seq)
	if err := writeAll(w, prefix[:]); err != nil {
		return err
	}
	return writeFrame(w, sealed)
}

func readEncryptedFrame(r io.Reader) (uint64, []byte, error) {
	var prefix [8]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return 0, nil, err
	}
	sealed, err := readFrame(r)
	if err == io.EOF {
		// The seq prefix was fully consumed but the frame is missing — this is
		// a torn record, NOT a clean end-of-file. Translate so scanRecords only
		// tolerates it on the active tail (allowTruncatedTail).
		return 0, nil, io.ErrUnexpectedEOF
	}
	if err != nil {
		return 0, nil, err
	}
	return binary.BigEndian.Uint64(prefix[:]), sealed, nil
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
	if !validOp(rec.Op) {
		return nil, fmt.Errorf("datawal: invalid op %d", rec.Op)
	}
	if len(rec.Payload) > MaxPayloadBytes {
		return nil, fmt.Errorf("datawal: payload too large: %d", len(rec.Payload))
	}
	metaLen := len(rec.Bucket) + len(rec.Key) + len(rec.Target)
	if metaLen > maxMetadataBytes {
		return nil, fmt.Errorf("datawal: metadata too large")
	}
	checksum := sha256.Sum256(rec.Payload)
	size := recordBodyFixedBytes + metaLen + len(rec.Payload)
	if size > maxRecordBodyBytes {
		return nil, fmt.Errorf("datawal: record body too large: %d", size)
	}
	body := getBuffer(size)
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
	off = putString(body, off, rec.Bucket)
	off = putString(body, off, rec.Key)
	off = putString(body, off, rec.Target)
	copy(body[off:], checksum[:])
	off += sha256.Size
	binary.BigEndian.PutUint64(body[off:], uint64(len(rec.Payload)))
	off += 8
	copy(body[off:], rec.Payload)
	return body, nil
}

func unmarshalRecordBody(body []byte, copyBytes bool) (Record, error) {
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
	if !validOp(rec.Op) {
		return rec, fmt.Errorf("datawal: invalid op %d", rec.Op)
	}
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
	if len(rec.Bucket)+len(rec.Key)+len(rec.Target) > maxMetadataBytes {
		return rec, fmt.Errorf("datawal: metadata too large")
	}
	if len(body[off:]) < sha256.Size+8 {
		return rec, io.ErrUnexpectedEOF
	}
	if copyBytes {
		rec.Checksum = append([]byte(nil), body[off:off+sha256.Size]...)
	} else {
		rec.Checksum = body[off : off+sha256.Size]
	}
	off += sha256.Size
	payloadLen := binary.BigEndian.Uint64(body[off:])
	off += 8
	if payloadLen > MaxPayloadBytes {
		return rec, fmt.Errorf("datawal: payload too large: %d", payloadLen)
	}
	if uint64(len(body[off:])) < payloadLen {
		return rec, io.ErrUnexpectedEOF
	}
	if copyBytes {
		rec.Payload = append([]byte(nil), body[off:off+int(payloadLen)]...)
	} else {
		rec.Payload = body[off : off+int(payloadLen)]
	}
	off += int(payloadLen)
	if off != len(body) {
		return rec, fmt.Errorf("datawal: trailing record bytes: %d", len(body)-off)
	}
	sum := sha256.Sum256(rec.Payload)
	if !bytes.Equal(rec.Checksum, sum[:]) {
		return rec, ErrChecksumMismatch
	}
	return rec, nil
}

func putString(dst []byte, off int, s string) int {
	binary.BigEndian.PutUint32(dst[off:], uint32(len(s)))
	off += 4
	copy(dst[off:], s)
	return off + len(s)
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

func validOp(op byte) bool {
	switch op {
	case OpSegmentPut, OpObjectWriteAt, OpObjectTruncate, OpShardPut, OpShardPackPut, OpShardPackDelete, OpSpoolPut:
		return true
	default:
		return false
	}
}
