package storage_test

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

var sizes = []int{4 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024}

func BenchmarkETag_MD5(b *testing.B) {
	for _, size := range sizes {
		data := make([]byte, size)
		b.Run(byteSize(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h := md5.Sum(data)
				_ = hex.EncodeToString(h[:])
			}
		})
	}
}

func BenchmarkETag_XXH3(b *testing.B) {
	for _, size := range sizes {
		data := make([]byte, size)
		b.Run(byteSize(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h := xxh3.Hash(data)
				var buf [8]byte
				binary.BigEndian.PutUint64(buf[:], h)
				_ = hex.EncodeToString(buf[:])
			}
		})
	}
}

func BenchmarkStreamingMD5Hasher(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 5*1024*1024)
	md5Pool := sync.Pool{New: func() any { return md5.New() }}

	for _, tc := range []struct {
		name string
		hash func() (hash.Hash, func(hash.Hash))
	}{
		{
			name: "new",
			hash: func() (hash.Hash, func(hash.Hash)) {
				return md5.New(), func(hash.Hash) {}
			},
		},
		{
			name: "pooled",
			hash: func() (hash.Hash, func(hash.Hash)) {
				h := md5Pool.Get().(hash.Hash)
				h.Reset()
				return h, func(h hash.Hash) { md5Pool.Put(h) }
			},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h, release := tc.hash()
				_, err := io.Copy(h, bytes.NewReader(data))
				require.NoError(b, err)
				_ = h.Sum(nil)
				release(h)
			}
		})
	}
}

func byteSize(n int) string {
	switch {
	case n >= 1024*1024:
		return fmt.Sprintf("%dMB", n/(1024*1024))
	case n >= 1024:
		return fmt.Sprintf("%dKB", n/1024)
	default:
		return fmt.Sprintf("%dB", n)
	}
}
