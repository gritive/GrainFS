package snapshot

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/klauspost/compress/zstd"
)

func benchmarkSnapshotPayload() *Snapshot {
	const objectCount = 1000
	objects := make([]storage.SnapshotObject, 0, objectCount)
	bucketMeta := []storage.SnapshotBucket{
		{Name: "hot", VersioningState: "Enabled"},
		{Name: "archive", VersioningState: "Suspended"},
	}
	for i := range objectCount {
		bucket := "hot"
		if i%5 == 0 {
			bucket = "archive"
		}
		objects = append(objects, storage.SnapshotObject{
			Bucket:    bucket,
			Key:       "objects/2026/05/15/object-" + fixedWidthDecimal(i),
			ETag:      "etag-" + fixedWidthDecimal(i),
			Size:      int64(1024 + i%8192),
			VersionID: "v-" + fixedWidthDecimal(i),
			IsLatest:  i%7 != 0,
		})
	}
	return &Snapshot{
		Seq:         1,
		Timestamp:   time.Unix(1778800000, 0).UTC(),
		WALOffset:   123456,
		Reason:      "benchmark",
		ObjectCount: len(objects),
		SizeBytes:   4096 * int64(len(objects)),
		Buckets:     []string{"archive", "hot"},
		Objects:     objects,
		BucketMeta:  bucketMeta,
	}
}

func fixedWidthDecimal(n int) string {
	var buf [6]byte
	for i := len(buf) - 1; i >= 0; i-- {
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[:])
}

func marshalBenchmarkSnapshot(b testing.TB) []byte {
	b.Helper()
	raw, err := json.Marshal(benchmarkSnapshotPayload())
	if err != nil {
		b.Fatalf("marshal snapshot: %v", err)
	}
	return raw
}

func gzipCompressForBenchmark(b testing.TB, raw []byte) []byte {
	b.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(raw); err != nil {
		b.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		b.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func zstdCompressForBenchmark(b testing.TB, raw []byte) []byte {
	b.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		b.Fatalf("zstd writer: %v", err)
	}
	if _, err := zw.Write(raw); err != nil {
		b.Fatalf("zstd write: %v", err)
	}
	if err := zw.Close(); err != nil {
		b.Fatalf("zstd close: %v", err)
	}
	return buf.Bytes()
}

func BenchmarkSnapshotCompression(b *testing.B) {
	raw := marshalBenchmarkSnapshot(b)
	b.Run("gzip/compress", func(b *testing.B) {
		b.SetBytes(int64(len(raw)))
		b.ReportAllocs()
		for b.Loop() {
			_ = gzipCompressForBenchmark(b, raw)
		}
	})
	b.Run("zstd/compress", func(b *testing.B) {
		b.SetBytes(int64(len(raw)))
		b.ReportAllocs()
		for b.Loop() {
			_ = zstdCompressForBenchmark(b, raw)
		}
	})
}

func BenchmarkSnapshotDecompression(b *testing.B) {
	raw := marshalBenchmarkSnapshot(b)
	gzipData := gzipCompressForBenchmark(b, raw)
	zstdData := zstdCompressForBenchmark(b, raw)
	b.Run("gzip/decompress", func(b *testing.B) {
		b.SetBytes(int64(len(raw)))
		b.ReportAllocs()
		for b.Loop() {
			gr, err := gzip.NewReader(bytes.NewReader(gzipData))
			if err != nil {
				b.Fatalf("gzip reader: %v", err)
			}
			_, err = io.ReadAll(gr)
			if closeErr := gr.Close(); err == nil {
				err = closeErr
			}
			if err != nil {
				b.Fatalf("gzip read: %v", err)
			}
		}
	})
	b.Run("zstd/decompress", func(b *testing.B) {
		b.SetBytes(int64(len(raw)))
		b.ReportAllocs()
		for b.Loop() {
			zr, err := zstd.NewReader(bytes.NewReader(zstdData))
			if err != nil {
				b.Fatalf("zstd reader: %v", err)
			}
			_, err = io.ReadAll(zr)
			zr.Close()
			if err != nil {
				b.Fatalf("zstd read: %v", err)
			}
		}
	})
}

func TestSnapshotCompressionRatios(t *testing.T) {
	if testing.Short() {
		t.Skip("ratio table only printed on full test runs")
	}
	raw := marshalBenchmarkSnapshot(t)
	gzipData := gzipCompressForBenchmark(t, raw)
	zstdData := zstdCompressForBenchmark(t, raw)
	t.Logf("snapshot raw=%d gzip=%d gzip_ratio=%.1f%% zstd=%d zstd_ratio=%.1f%%",
		len(raw),
		len(gzipData), 100*float64(len(gzipData))/float64(len(raw)),
		len(zstdData), 100*float64(len(zstdData))/float64(len(raw)))
}
