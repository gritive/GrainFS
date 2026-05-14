package storage

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"io"

	"github.com/gritive/GrainFS/internal/pool"
	"github.com/zeebo/xxh3"
)

var xxh3Pool = pool.New(func() *xxh3.Hasher { return xxh3.New() })
var md5Pool = pool.New(func() hash.Hash { return md5.New() })

// InternalETag returns the xxhash3-based ETag for in-memory data.
// Result is a 16-char hex string (8-byte hash). Use for internal buckets only.
func InternalETag(data []byte) string {
	h := xxh3.Hash(data)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], h)
	return hex.EncodeToString(buf[:])
}

// GetXXH3Hasher returns a pooled xxh3 hasher (Reset already called).
func GetXXH3Hasher() *xxh3.Hasher {
	h := xxh3Pool.Get()
	h.Reset()
	return h
}

// PutXXH3Hasher returns a hasher to the pool.
func PutXXH3Hasher(h *xxh3.Hasher) {
	xxh3Pool.Put(h)
}

// VerifyETag recomputes the ETag of rc and compares against expected.
// Algorithm is selected by expected's hex length: 32=MD5, 16=xxhash3.
// Internal buckets (__grainfs_*) use 16-char xxhash3; S3-exposed use 32-char MD5.
// __grainfs_nfs4 is excluded from IsInternalBucket and always produces MD5 ETags.
// Multipart ETags (hex-N, 34+ chars) fall to default — internal buckets never use multipart.
func VerifyETag(rc io.Reader, expected string) (bool, error) {
	switch len(expected) {
	case 32: // MD5
		h := md5Pool.Get()
		h.Reset()
		defer md5Pool.Put(h)
		if _, err := io.Copy(h, rc); err != nil {
			return false, err
		}
		return hex.EncodeToString(h.Sum(nil)) == expected, nil
	case 16: // xxhash3
		h := GetXXH3Hasher()
		defer PutXXH3Hasher(h)
		if _, err := io.Copy(h, rc); err != nil {
			return false, err
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], h.Sum64())
		return hex.EncodeToString(buf[:]) == expected, nil
	default:
		return false, nil
	}
}

// hashForBucket returns (hash, release) for the given bucket.
// release() MUST be called when done — it returns the hasher to its pool.
// Internal buckets use xxhash3; S3-exposed buckets use MD5.
func hashForBucket(bucket string) (hash.Hash, func()) {
	if IsInternalBucket(bucket) {
		h := GetXXH3Hasher()
		return h, func() { PutXXH3Hasher(h) }
	}
	h := md5Pool.Get()
	h.Reset()
	return h, func() { md5Pool.Put(h) }
}

// etagFromHash encodes the hash result to hex.
// Handles xxhash3's BigEndian Sum64 encoding separately from MD5's Sum(nil).
func etagFromHash(h hash.Hash) string {
	if xh, ok := h.(*xxh3.Hasher); ok {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], xh.Sum64())
		return hex.EncodeToString(buf[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}
