package encrypt

import (
	"bytes"
	"fmt"
	"testing"
)

func benchmarkEncryptor(b *testing.B) *Encryptor {
	b.Helper()
	enc, err := NewEncryptor(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		b.Fatal(err)
	}
	return enc
}

func benchmarkPayload(size int) []byte {
	return bytes.Repeat([]byte("x"), size)
}

// BenchmarkSealValue_NilDst measures the SealValueAADTo cost with a nil
// destination — i.e. the path that allocates a fresh output buffer on every
// call. Complements BenchmarkSealValueAADTo, which pre-allocates dst and
// measures the alloc-free steady state.
func BenchmarkSealValue_NilDst(b *testing.B) {
	enc := benchmarkEncryptor(b)
	aad := []byte("bench:value")
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20, 4 << 20} {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			plaintext := benchmarkPayload(size)
			b.SetBytes(int64(len(plaintext)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sealed, err := enc.SealValueAADTo(nil, aad, plaintext)
				if err != nil {
					b.Fatal(err)
				}
				if len(sealed) == 0 {
					b.Fatal("empty ciphertext")
				}
			}
		})
	}
}

func BenchmarkSealValueAADTo(b *testing.B) {
	enc := benchmarkEncryptor(b)
	aad := []byte("bench:value")
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20, 4 << 20} {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			plaintext := benchmarkPayload(size)
			dst := make([]byte, 0, 3+12+len(plaintext)+enc.AEADOverhead())
			b.SetBytes(int64(len(plaintext)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sealed, err := enc.SealValueAADTo(dst[:0], aad, plaintext)
				if err != nil {
					b.Fatal(err)
				}
				if len(sealed) == 0 {
					b.Fatal("empty ciphertext")
				}
			}
		})
	}
}

// BenchmarkOpenValueAAD measures the OpenValueAAD path — equivalent to
// OpenValueAADTo with a nil destination, allocating a fresh plaintext buffer
// per call. Use OpenValueAADTo with a reusable dst in hot loops.
func BenchmarkOpenValueAAD(b *testing.B) {
	enc := benchmarkEncryptor(b)
	aad := []byte("bench:value")
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20, 4 << 20} {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			plaintext := benchmarkPayload(size)
			sealed, err := enc.SealValueAADTo(nil, aad, plaintext)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(plaintext)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := enc.OpenValueAAD(aad, sealed)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) != len(plaintext) {
					b.Fatal("plaintext length mismatch")
				}
			}
		})
	}
}

func BenchmarkSealWithNonceAAD(b *testing.B) {
	enc := benchmarkEncryptor(b)
	nonce := bytes.Repeat([]byte{0x24}, 12)
	aad := []byte("bench:value")
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20, 4 << 20} {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			plaintext := benchmarkPayload(size)
			b.SetBytes(int64(len(plaintext)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst := make([]byte, 0, len(plaintext)+enc.AEADOverhead())
				sealed, err := enc.SealWithNonceAAD(dst, nonce, plaintext, aad)
				if err != nil {
					b.Fatal(err)
				}
				if len(sealed) == 0 {
					b.Fatal("empty ciphertext")
				}
			}
		})
	}
}
