package storage_test

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"

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
