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

// EncodeDeletePayload returns a binary envelope holding only the bucket name.
func EncodeDeletePayload(bucket string) []byte {
	bb := []byte(bucket)
	buf := make([]byte, 2+len(bb))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(bb)))
	copy(buf[2:], bb)
	return buf
}

// DecodeDeletePayload reverses EncodeDeletePayload.
func DecodeDeletePayload(buf []byte) (string, error) {
	if len(buf) < 2 {
		return "", fmt.Errorf("lifecycle: delete payload too short")
	}
	n := int(binary.BigEndian.Uint16(buf[:2]))
	if 2+n != len(buf) {
		return "", fmt.Errorf("lifecycle: delete payload bucket_len=%d does not match buffer=%d", n, len(buf)-2)
	}
	return string(buf[2:]), nil
}
