package migration

import (
	"encoding/binary"
	"fmt"
)

const maxBucketLen = 255  // S3 bucket names ≤ 63 chars; 255 gives ample headroom
const maxReasonLen = 4096 // error reason string cap

// EncodeJobStartPayload encodes [4-byte bucket-len][bucket][8-byte startedAt unix-nano].
func EncodeJobStartPayload(bucket string, startedAt int64) []byte {
	b := make([]byte, 4+len(bucket)+8)
	binary.BigEndian.PutUint32(b[:4], uint32(len(bucket)))
	copy(b[4:], bucket)
	binary.BigEndian.PutUint64(b[4+len(bucket):], uint64(startedAt))
	return b
}

// DecodeJobStartPayload reverses EncodeJobStartPayload.
func DecodeJobStartPayload(buf []byte) (bucket string, startedAt int64, err error) {
	if len(buf) < 4 {
		return "", 0, fmt.Errorf("migration: DecodeJobStartPayload: truncated header")
	}
	n := int(binary.BigEndian.Uint32(buf[:4]))
	if n > maxBucketLen {
		return "", 0, fmt.Errorf("migration: DecodeJobStartPayload: bucket name too long (%d)", n)
	}
	if len(buf) < 4+n+8 {
		return "", 0, fmt.Errorf("migration: DecodeJobStartPayload: buffer too short")
	}
	bucket = string(buf[4 : 4+n])
	startedAt = int64(binary.BigEndian.Uint64(buf[4+n:]))
	return bucket, startedAt, nil
}

// EncodeJobDonePayload encodes [4-byte bucket-len][bucket][8-byte copied][8-byte errors][8-byte updatedAt unix-nano].
func EncodeJobDonePayload(bucket string, copied, errors, updatedAt int64) []byte {
	b := make([]byte, 4+len(bucket)+24)
	binary.BigEndian.PutUint32(b[:4], uint32(len(bucket)))
	copy(b[4:], bucket)
	off := 4 + len(bucket)
	binary.BigEndian.PutUint64(b[off:], uint64(copied))
	binary.BigEndian.PutUint64(b[off+8:], uint64(errors))
	binary.BigEndian.PutUint64(b[off+16:], uint64(updatedAt))
	return b
}

// DecodeJobDonePayload reverses EncodeJobDonePayload.
func DecodeJobDonePayload(buf []byte) (bucket string, copied, errors, updatedAt int64, err error) {
	if len(buf) < 4 {
		return "", 0, 0, 0, fmt.Errorf("migration: DecodeJobDonePayload: truncated")
	}
	n := int(binary.BigEndian.Uint32(buf[:4]))
	if n > maxBucketLen {
		return "", 0, 0, 0, fmt.Errorf("migration: DecodeJobDonePayload: bucket name too long (%d)", n)
	}
	if len(buf) < 4+n+24 {
		return "", 0, 0, 0, fmt.Errorf("migration: DecodeJobDonePayload: buffer too short")
	}
	bucket = string(buf[4 : 4+n])
	off := 4 + n
	copied = int64(binary.BigEndian.Uint64(buf[off:]))
	errors = int64(binary.BigEndian.Uint64(buf[off+8:]))
	updatedAt = int64(binary.BigEndian.Uint64(buf[off+16:]))
	return bucket, copied, errors, updatedAt, nil
}

// EncodeJobFailedPayload encodes [4-byte bucket-len][bucket][4-byte reason-len][reason][8-byte errors][8-byte updatedAt unix-nano].
func EncodeJobFailedPayload(bucket, reason string, errors, updatedAt int64) []byte {
	b := make([]byte, 4+len(bucket)+4+len(reason)+16)
	binary.BigEndian.PutUint32(b[:4], uint32(len(bucket)))
	copy(b[4:], bucket)
	off := 4 + len(bucket)
	binary.BigEndian.PutUint32(b[off:], uint32(len(reason)))
	copy(b[off+4:], reason)
	off += 4 + len(reason)
	binary.BigEndian.PutUint64(b[off:], uint64(errors))
	binary.BigEndian.PutUint64(b[off+8:], uint64(updatedAt))
	return b
}

// DecodeJobFailedPayload reverses EncodeJobFailedPayload.
func DecodeJobFailedPayload(buf []byte) (bucket, reason string, errors, updatedAt int64, err error) {
	if len(buf) < 4 {
		return "", "", 0, 0, fmt.Errorf("migration: DecodeJobFailedPayload: truncated")
	}
	n := int(binary.BigEndian.Uint32(buf[:4]))
	if n > maxBucketLen {
		return "", "", 0, 0, fmt.Errorf("migration: DecodeJobFailedPayload: bucket name too long (%d)", n)
	}
	if len(buf) < 4+n+4 {
		return "", "", 0, 0, fmt.Errorf("migration: DecodeJobFailedPayload: buffer too short for reason header")
	}
	bucket = string(buf[4 : 4+n])
	off := 4 + n
	m := int(binary.BigEndian.Uint32(buf[off:]))
	if m > maxReasonLen {
		return "", "", 0, 0, fmt.Errorf("migration: DecodeJobFailedPayload: reason too long (%d)", m)
	}
	if len(buf) < off+4+m+16 {
		return "", "", 0, 0, fmt.Errorf("migration: DecodeJobFailedPayload: buffer too short for reason body")
	}
	reason = string(buf[off+4 : off+4+m])
	off += 4 + m
	errors = int64(binary.BigEndian.Uint64(buf[off:]))
	updatedAt = int64(binary.BigEndian.Uint64(buf[off+8:]))
	return bucket, reason, errors, updatedAt, nil
}
