package storage

// S3-operation micro-benchmarks for the single-node hot path:
//
//	storage.Operations (S3-facing facade)  ->  LocalBackend
//
// These isolate the per-op cost the cluster path can never strip and quantify
// the write-path buffer right-sizing fixes:
//   - PUT body type matters. firstChunkBufferSize right-sizes the first chunk
//     only for readers exposing Len(): *bytes.Reader (in-process callers) and
//     the server's buffered PUT body (*putObjectBodyReader, mimicked here by
//     lenReader). An opaque HTTP BodyStream (>=8 MiB streaming path, mimicked
//     by opaqueReader) has no Len() and uses a full DefaultChunkSize chunk.
//   - encrypted-vs-plaintext: the XAES-256-GCM at-rest seam. Encrypted writes
//     also pay the writeEncryptedObjectFile bufio/working buffers (now pooled).
//   - facade-vs-raw: the Operations wrapper overhead — one extra HEAD
//     (previousObject) before the mutation for the *Result entry points.
//   - size sweep (4KiB/256KiB/5MiB): fixed per-op cost vs per-byte cost.
//
// Wall-time on macOS over b.TempDir() (real fsync) is noisy and won't match
// Linux prod — read it as relative, and lead with allocs/op (-benchmem), which
// transfer. Use `go test -run '^$' -bench BenchmarkS3 -benchmem ./internal/storage/`.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/require"
)

const s3BenchBucket = "bench"

var s3BenchSizes = []struct {
	name string
	size int
}{
	{"4KiB", 4 << 10},
	{"256KiB", 256 << 10},
	{"5MiB", 5 << 20},
}

// newS3BenchOps builds an Operations facade over a LocalBackend, optionally
// encrypted (production default), with a ready bucket.
func newS3BenchOps(b *testing.B, encrypted bool) (*Operations, *LocalBackend) {
	b.Helper()
	var backend *LocalBackend
	var err error
	if encrypted {
		cid := bytes.Repeat([]byte{0x88}, 16)
		keeper, kerr := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x88}, encrypt.KEKSize), cid)
		require.NoError(b, kerr)
		backend, err = NewLocalBackendWithDEKKeeper(b.TempDir(), keeper, cid)
	} else {
		backend, err = NewLocalBackend(b.TempDir())
	}
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, backend.Close()) })
	require.NoError(b, backend.CreateBucket(context.Background(), s3BenchBucket))
	return NewOperations(backend), backend
}

func s3BenchPayload(n int) []byte { return bytes.Repeat([]byte("x"), n) }

// opaqueReader hides the concrete type AND any Len() — this is the >=8 MiB
// streaming PUT path (an HTTP BodyStream / aws-chunked decoder). The chunk
// buffer cannot be right-sized; it stays at DefaultChunkSize.
type opaqueReader struct{ r io.Reader }

func (o opaqueReader) Read(p []byte) (int, error) { return o.r.Read(p) }

// BenchmarkS3Put_Facade is the real server PUT path with an in-memory
// *bytes.Reader body (in-process callers). Same key each iter = overwrite, so
// the previousObject HEAD always hits an existing object. Compare against
// BenchmarkS3Put_Raw to read off the Operations facade overhead.
func BenchmarkS3Put_Facade(b *testing.B) {
	for _, enc := range []bool{false, true} {
		for _, sz := range s3BenchSizes {
			b.Run(cryptoLabel(enc)+"/"+sz.name, func(b *testing.B) {
				ops, _ := newS3BenchOps(b, enc)
				payload := s3BenchPayload(sz.size)
				ctx := context.Background()
				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := ops.PutObjectWithResult(ctx, s3BenchBucket, "key", bytes.NewReader(payload), "application/octet-stream")
					require.NoError(b, err)
				}
			})
		}
	}
}

// BenchmarkS3Put_BufferedBody is the production-realistic PUT below the 8 MiB
// streaming threshold: the body is a buffered reader that reports Len() (like
// server.putObjectBodyReader). With first-chunk right-sizing the chunk buffer
// shrinks to the object size; without it, it is a full 16 MiB DefaultChunkSize.
func BenchmarkS3Put_BufferedBody(b *testing.B) {
	for _, enc := range []bool{false, true} {
		for _, sz := range s3BenchSizes {
			b.Run(cryptoLabel(enc)+"/"+sz.name, func(b *testing.B) {
				ops, _ := newS3BenchOps(b, enc)
				payload := s3BenchPayload(sz.size)
				size := int64(sz.size)
				ctx := context.Background()
				b.SetBytes(size)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := ops.PutObjectWithRequestResult(ctx, PutObjectRequest{
						Bucket:      s3BenchBucket,
						Key:         "key",
						Body:        &lenReader{data: payload},
						SizeHint:    &size,
						ContentType: "application/octet-stream",
					})
					require.NoError(b, err)
				}
			})
		}
	}
}

// BenchmarkS3Put_Streaming is the >=8 MiB streaming PUT: an opaque body with no
// Len(). The chunk buffer stays at DefaultChunkSize (proportionate for large
// objects). Encrypted variants still benefit from the pooled write buffers.
func BenchmarkS3Put_Streaming(b *testing.B) {
	for _, enc := range []bool{false, true} {
		for _, sz := range s3BenchSizes {
			b.Run(cryptoLabel(enc)+"/"+sz.name, func(b *testing.B) {
				ops, _ := newS3BenchOps(b, enc)
				payload := s3BenchPayload(sz.size)
				size := int64(sz.size)
				ctx := context.Background()
				b.SetBytes(size)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := ops.PutObjectWithRequestResult(ctx, PutObjectRequest{
						Bucket:      s3BenchBucket,
						Key:         "key",
						Body:        opaqueReader{bytes.NewReader(payload)},
						SizeHint:    &size,
						ContentType: "application/octet-stream",
					})
					require.NoError(b, err)
				}
			})
		}
	}
}

// BenchmarkS3Put_Raw is the backend PUT with no facade wrapper (no
// previousObject HEAD, no facts). Delta vs Facade = Operations overhead.
func BenchmarkS3Put_Raw(b *testing.B) {
	for _, enc := range []bool{false, true} {
		for _, sz := range s3BenchSizes {
			b.Run(cryptoLabel(enc)+"/"+sz.name, func(b *testing.B) {
				_, backend := newS3BenchOps(b, enc)
				payload := s3BenchPayload(sz.size)
				ctx := context.Background()
				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := backend.PutObject(ctx, s3BenchBucket, "key", bytes.NewReader(payload), "application/octet-stream")
					require.NoError(b, err)
				}
			})
		}
	}
}

// BenchmarkS3Get reads the full object body (GetObject is pure facade
// delegation, so facade==raw). Measures meta read + open + stream + decrypt.
func BenchmarkS3Get(b *testing.B) {
	for _, enc := range []bool{false, true} {
		for _, sz := range s3BenchSizes {
			b.Run(cryptoLabel(enc)+"/"+sz.name, func(b *testing.B) {
				ops, _ := newS3BenchOps(b, enc)
				payload := s3BenchPayload(sz.size)
				ctx := context.Background()
				_, err := ops.PutObjectWithResult(ctx, s3BenchBucket, "key", bytes.NewReader(payload), "application/octet-stream")
				require.NoError(b, err)
				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					rc, _, err := ops.GetObject(ctx, s3BenchBucket, "key")
					require.NoError(b, err)
					_, err = io.Copy(io.Discard, rc)
					require.NoError(b, err)
					require.NoError(b, rc.Close())
				}
			})
		}
	}
}

// BenchmarkS3Head measures the metadata-only read (quorum-meta blob decode).
// Size-independent; this IS the cost the *Result facade pays as its extra HEAD.
func BenchmarkS3Head(b *testing.B) {
	for _, enc := range []bool{false, true} {
		b.Run(cryptoLabel(enc), func(b *testing.B) {
			ops, _ := newS3BenchOps(b, enc)
			payload := s3BenchPayload(4 << 10)
			ctx := context.Background()
			_, err := ops.PutObjectWithResult(ctx, s3BenchBucket, "key", bytes.NewReader(payload), "application/octet-stream")
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ops.HeadObject(ctx, s3BenchBucket, "key")
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkS3Delete_Facade is the server DELETE path (DeleteObjectWithResult ->
// previousObject HEAD + delete). Per-iter re-PUT is excluded via StopTimer.
func BenchmarkS3Delete_Facade(b *testing.B) {
	benchDelete(b, func(ops *Operations, backend *LocalBackend, ctx context.Context, key string) {
		_, err := ops.DeleteObjectWithResult(ctx, s3BenchBucket, key)
		require.NoError(b, err)
	})
}

// BenchmarkS3Delete_Raw is the backend delete with no previousObject HEAD.
func BenchmarkS3Delete_Raw(b *testing.B) {
	benchDelete(b, func(ops *Operations, backend *LocalBackend, ctx context.Context, key string) {
		require.NoError(b, backend.DeleteObject(ctx, s3BenchBucket, key))
	})
}

func benchDelete(b *testing.B, del func(*Operations, *LocalBackend, context.Context, string)) {
	for _, enc := range []bool{false, true} {
		b.Run(cryptoLabel(enc), func(b *testing.B) {
			ops, backend := newS3BenchOps(b, enc)
			payload := s3BenchPayload(4 << 10)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				key := fmt.Sprintf("k-%d", i)
				_, err := backend.PutObject(ctx, s3BenchBucket, key, bytes.NewReader(payload), "application/octet-stream")
				require.NoError(b, err)
				b.StartTimer()
				del(ops, backend, ctx, key)
			}
		})
	}
}

// BenchmarkS3Copy is the server COPY path (CopyObjectWithResult -> source HEAD +
// destination previousObject HEAD + server-side copy). Overwrites a fixed dest.
func BenchmarkS3Copy(b *testing.B) {
	for _, enc := range []bool{false, true} {
		for _, sz := range s3BenchSizes {
			b.Run(cryptoLabel(enc)+"/"+sz.name, func(b *testing.B) {
				ops, _ := newS3BenchOps(b, enc)
				payload := s3BenchPayload(sz.size)
				ctx := context.Background()
				_, err := ops.PutObjectWithResult(ctx, s3BenchBucket, "src", bytes.NewReader(payload), "application/octet-stream")
				require.NoError(b, err)
				req := CopyObjectRequest{
					Source:      ObjectRef{Bucket: s3BenchBucket, Key: "src"},
					Destination: ObjectRef{Bucket: s3BenchBucket, Key: "dst"},
				}
				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := ops.CopyObjectWithResult(ctx, req)
					require.NoError(b, err)
				}
			})
		}
	}
}

func cryptoLabel(encrypted bool) string {
	if encrypted {
		return "encrypted"
	}
	return "plaintext"
}
