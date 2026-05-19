package storage

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func benchmarkStorageEncryptor(b *testing.B) *encrypt.Encryptor {
	b.Helper()
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		b.Fatal(err)
	}
	return enc
}

func BenchmarkEncryptedObjectFileWrite(b *testing.B) {
	enc := benchmarkStorageEncryptor(b)
	payload := bytes.Repeat([]byte("w"), 8<<20)
	path := filepath.Join(b.TempDir(), "object.enc")

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		size, err := writeEncryptedObjectFile(path, enc, "bench:object", bytes.NewReader(payload), io.Discard)
		if err != nil {
			b.Fatal(err)
		}
		if size != int64(len(payload)) {
			b.Fatal("size mismatch")
		}
	}
}

func BenchmarkEncryptedObjectFileRead(b *testing.B) {
	enc := benchmarkStorageEncryptor(b)
	payload := bytes.Repeat([]byte("r"), 8<<20)
	path := filepath.Join(b.TempDir(), "object.enc")
	size, err := writeEncryptedObjectFile(path, enc, "bench:object", bytes.NewReader(payload), io.Discard)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc, err := openEncryptedObjectFile(path, enc, "bench:object", size)
		if err != nil {
			b.Fatal(err)
		}
		n, err := io.Copy(io.Discard, rc)
		closeErr := rc.Close()
		if err != nil {
			b.Fatal(err)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
		if n != size {
			b.Fatal("size mismatch")
		}
	}
}

func BenchmarkEncryptedObjectFileReadAt(b *testing.B) {
	enc := benchmarkStorageEncryptor(b)
	payload := bytes.Repeat([]byte("a"), 8<<20)
	path := filepath.Join(b.TempDir(), "object.enc")
	size, err := writeEncryptedObjectFile(path, enc, "bench:object", bytes.NewReader(payload), io.Discard)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 4<<10)

	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := readAtEncryptedObjectFile(path, enc, "bench:object", size, int64(i%1024), buf)
		if err != nil && err != io.EOF {
			b.Fatal(err)
		}
		if n == 0 {
			b.Fatal("empty read")
		}
	}
}
