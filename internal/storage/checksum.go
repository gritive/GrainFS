package storage

import (
	"encoding/binary"
	"hash"

	"github.com/zeebo/xxh3"
)

// ChecksumLen is the byte length of a SegmentRef.checksum value.
const ChecksumLen = 16

// ChecksumHasher computes a streaming xxhash3-128 over plaintext segment
// bytes. Use NewChecksumHasher() and io.Copy. Call Sum() once at EOF.
type ChecksumHasher struct {
	h *xxh3.Hasher
}

// NewChecksumHasher returns a fresh streaming xxhash3-128 hasher.
func NewChecksumHasher() *ChecksumHasher {
	return &ChecksumHasher{h: xxh3.New()}
}

// Write implements io.Writer.
func (c *ChecksumHasher) Write(p []byte) (int, error) {
	return c.h.Write(p)
}

// Sum returns the 16-byte xxhash3-128 digest.
// Encoding is big-endian: Hi in bytes[0:8], Lo in bytes[8:16].
func (c *ChecksumHasher) Sum() []byte {
	sum128 := c.h.Sum128()
	out := make([]byte, ChecksumLen)
	binary.BigEndian.PutUint64(out[0:8], sum128.Hi)
	binary.BigEndian.PutUint64(out[8:16], sum128.Lo)
	return out
}

// Reset clears state so the hasher can be reused.
func (c *ChecksumHasher) Reset() { c.h.Reset() }

// Compile-time check: ChecksumHasher implements hash.Hash-ish (no full
// hash.Hash because Sum() differs from hash.Hash.Sum(b []byte) []byte).
var _ interface {
	Write(p []byte) (int, error)
} = (*ChecksumHasher)(nil)

// ChecksumOf computes the xxhash3-128 of buf in one shot.
// Encoding matches ChecksumHasher.Sum (big-endian Hi||Lo).
func ChecksumOf(buf []byte) []byte {
	sum128 := xxh3.Hash128(buf)
	out := make([]byte, ChecksumLen)
	binary.BigEndian.PutUint64(out[0:8], sum128.Hi)
	binary.BigEndian.PutUint64(out[8:16], sum128.Lo)
	return out
}

// hashHashShim adapts ChecksumHasher when a hash.Hash interface is needed.
// Sum(b []byte) returns append(b, digest...).
type hashHashShim struct{ *ChecksumHasher }

func (s hashHashShim) Sum(b []byte) []byte { return append(b, s.ChecksumHasher.Sum()...) }
func (s hashHashShim) Size() int           { return ChecksumLen }
func (s hashHashShim) BlockSize() int      { return 64 }

// AsHash returns a hash.Hash view of the hasher.
func (c *ChecksumHasher) AsHash() hash.Hash { return hashHashShim{c} }
