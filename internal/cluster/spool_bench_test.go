package cluster

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

func benchmarkClusterSeam(b *testing.B) storage.DataEncryptor {
	b.Helper()
	clusterID := bytes.Repeat([]byte{0x42}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x42}, encrypt.KEKSize), clusterID)
	if err != nil {
		b.Fatal(err)
	}
	return storage.NewDEKKeeperAdapter(keeper, clusterID)
}

func BenchmarkEncryptedSpoolWrite(b *testing.B) {
	seam := benchmarkClusterSeam(b)
	payload := bytes.Repeat([]byte("s"), 8<<20)
	dir := b.TempDir()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp, err := spoolObjectEncrypted(context.Background(), dir, bytes.NewReader(payload), "bench-bucket", seam, "bench:spool", true)
		if err != nil {
			b.Fatal(err)
		}
		if sp.Size != int64(len(payload)) {
			b.Fatal("size mismatch")
		}
		sp.Cleanup()
	}
}

// BenchmarkSpoolMD5WithMD5 / NoMD5 measure the CPU impact of skipping MD5 in spool.
// Run with: go test -bench=BenchmarkSpoolMD5 -benchmem ./internal/cluster/
func BenchmarkSpoolMD5WithMD5(b *testing.B) {
	seam := benchmarkClusterSeam(b)
	payload := bytes.Repeat([]byte("x"), 1<<20)
	dir := b.TempDir()
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp, err := spoolObjectEncrypted(context.Background(), dir, bytes.NewReader(payload), "user-bucket", seam, "bench:md5-with", true)
		if err != nil {
			b.Fatal(err)
		}
		sp.Cleanup()
	}
}

func BenchmarkSpoolMD5NoMD5(b *testing.B) {
	seam := benchmarkClusterSeam(b)
	payload := bytes.Repeat([]byte("x"), 1<<20)
	dir := b.TempDir()
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp, err := spoolObjectEncrypted(context.Background(), dir, bytes.NewReader(payload), "user-bucket", seam, "bench:md5-no", false)
		if err != nil {
			b.Fatal(err)
		}
		sp.Cleanup()
	}
}

// BenchmarkSpoolECShardsEncrypted measures the EC spool pipeline using
// the encrypted put-spool path (MPU-style). Compare against
// BenchmarkSpoolECShardsPlain to see the put-spool encryption overhead.
func BenchmarkSpoolECShardsEncrypted(b *testing.B) {
	seam := benchmarkClusterSeam(b)
	payload := bytes.Repeat([]byte("s"), 8<<20) // 8 MiB
	spoolDir := b.TempDir()
	ecDir := b.TempDir()
	cfg := ECConfig{DataShards: 4, ParityShards: 2}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp, err := spoolObjectEncrypted(context.Background(), spoolDir, bytes.NewReader(payload), "bench-bucket", seam, "bench:ec-encrypted", false)
		if err != nil {
			b.Fatal(err)
		}
		shards, err := spoolECShards(context.Background(), cfg, ecDir, sp)
		sp.Cleanup()
		if err != nil {
			b.Fatal(err)
		}
		shards.Cleanup()
	}
}

// BenchmarkPlainSpoolWrite measures the new plaintext put-spool write path.
// Compare against BenchmarkEncryptedSpoolWrite to see the eliminated overhead.
func BenchmarkPlainSpoolWrite(b *testing.B) {
	payload := bytes.Repeat([]byte("s"), 8<<20)
	dir := b.TempDir()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp, err := spoolObject(context.Background(), dir, bytes.NewReader(payload), "bench-bucket", true)
		if err != nil {
			b.Fatal(err)
		}
		if sp.Size != int64(len(payload)) {
			b.Fatal("size mismatch")
		}
		sp.Cleanup()
	}
}

// BenchmarkSpoolECShardsPlain measures the full put-spool → EC split pipeline
// with plaintext spool files. Both the eliminated put-spool encryption and the
// eliminated ec-spool re-encryption are captured here.
func BenchmarkSpoolECShardsPlain(b *testing.B) {
	payload := bytes.Repeat([]byte("s"), 8<<20) // 8 MiB
	spoolDir := b.TempDir()
	ecDir := b.TempDir()
	cfg := ECConfig{DataShards: 4, ParityShards: 2}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp, err := spoolObject(context.Background(), spoolDir, bytes.NewReader(payload), "bench-bucket", false)
		if err != nil {
			b.Fatal(err)
		}
		shards, err := spoolECShards(context.Background(), cfg, ecDir, sp)
		sp.Cleanup()
		if err != nil {
			b.Fatal(err)
		}
		shards.Cleanup()
	}
}

func BenchmarkEncryptedSpoolOpen(b *testing.B) {
	seam := benchmarkClusterSeam(b)
	payload := bytes.Repeat([]byte("o"), 8<<20)
	sp, err := spoolObjectEncrypted(context.Background(), b.TempDir(), bytes.NewReader(payload), "bench-bucket", seam, "bench:spool", true)
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
