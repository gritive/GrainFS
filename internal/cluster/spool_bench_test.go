package cluster

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func benchmarkClusterEncryptor(b *testing.B) *encrypt.Encryptor {
	b.Helper()
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		b.Fatal(err)
	}
	return enc
}

func BenchmarkEncryptedSpoolWrite(b *testing.B) {
	enc := benchmarkClusterEncryptor(b)
	payload := bytes.Repeat([]byte("s"), 8<<20)
	dir := b.TempDir()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp, err := spoolObjectEncrypted(context.Background(), dir, bytes.NewReader(payload), "bench-bucket", enc, "bench:spool")
		if err != nil {
			b.Fatal(err)
		}
		if sp.Size != int64(len(payload)) {
			b.Fatal("size mismatch")
		}
		sp.Cleanup()
	}
}

func BenchmarkEncryptedSpoolOpen(b *testing.B) {
	enc := benchmarkClusterEncryptor(b)
	payload := bytes.Repeat([]byte("o"), 8<<20)
	sp, err := spoolObjectEncrypted(context.Background(), b.TempDir(), bytes.NewReader(payload), "bench-bucket", enc, "bench:spool")
	if err != nil {
		b.Fatal(err)
	}
	defer sp.Cleanup()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc, err := sp.Open()
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
		if n != int64(len(payload)) {
			b.Fatal("size mismatch")
		}
	}
}
