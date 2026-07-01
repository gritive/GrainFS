package lifecycle

import (
	"encoding/binary"
	"fmt"
)

// EncodePutPayload returns a binary envelope holding the bucket name and the
// raw S3 wire XML body. Layout: uint16 bucket_len (big-endian) | bucket bytes
// | xml bytes. XML body length is implicit (rest of buffer).
func EncodePutPayload(bucket string, raw []byte) []byte {
	bb := []byte(bucket)
	buf := make([]byte, 2+len(bb)+len(raw))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(bb)))
	copy(buf[2:], bb)
	copy(buf[2+len(bb):], raw)
	return buf
}

// DecodePutPayload reverses EncodePutPayload.
func DecodePutPayload(buf []byte) (bucket string, raw []byte, err error) {
	if len(buf) < 2 {
		return "", nil, fmt.Errorf("lifecycle: put payload too short (%d bytes)", len(buf))
	}
	n := int(binary.BigEndian.Uint16(buf[:2]))
	if 2+n > len(buf) {
		return "", nil, fmt.Errorf("lifecycle: put payload bucket_len=%d exceeds buffer=%d", n, len(buf)-2)
	}
	return string(buf[2 : 2+n]), buf[2+n:], nil
}

// EncodeDeletePayload returns a binary envelope: uint16 bucket_len | bucket |
// uint64 observedGen (big-endian). The 8-byte generation suffix carries the CAS
// token captured by the bucket-delete cascade. Pass UnconditionalDeleteGen for
// an unconditional delete.
func EncodeDeletePayload(bucket string, observedGen uint64) []byte {
	bb := []byte(bucket)
	buf := make([]byte, 2+len(bb)+8)
	binary.BigEndian.PutUint16(buf[:2], uint16(len(bb)))
	copy(buf[2:], bb)
	binary.BigEndian.PutUint64(buf[2+len(bb):], observedGen)
	return buf
}

// DecodeDeletePayload reverses EncodeDeletePayload. A legacy payload with no
// generation suffix (exactly 2+bucket_len bytes) decodes to UnconditionalDeleteGen
// so replay of pre-generation delete entries preserves the old unconditional
// behavior.
func DecodeDeletePayload(buf []byte) (string, uint64, error) {
	if len(buf) < 2 {
		return "", 0, fmt.Errorf("lifecycle: delete payload too short")
	}
	n := int(binary.BigEndian.Uint16(buf[:2]))
	if 2+n > len(buf) {
		return "", 0, fmt.Errorf("lifecycle: delete payload bucket_len=%d exceeds buffer=%d", n, len(buf)-2)
	}
	bucket := string(buf[2 : 2+n])
	rest := buf[2+n:]
	switch len(rest) {
	case 0:
		return bucket, UnconditionalDeleteGen, nil // legacy payload
	case 8:
		return bucket, binary.BigEndian.Uint64(rest), nil
	default:
		return "", 0, fmt.Errorf("lifecycle: delete payload trailing=%d bytes, want 0 or 8", len(rest))
	}
}
