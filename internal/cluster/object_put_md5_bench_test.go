package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// md5BenchSink prevents the compiler from eliding the PUT result.
var md5BenchSink string

// putContentMD5BenchSizes are the bodies exercised by the streaming Content-MD5
// PUT benchmark.
var putContentMD5BenchSizes = []struct {
	name string
	size int
}{
	{"64KiB", 64 << 10},
	{"1MiB", 1 << 20},
	{"8MiB", 8 << 20},
}

// makeMD5BenchBody builds a deterministic non-zero body and its correct
// Content-MD5 hex (computed once, outside the benchmark loop).
func makeMD5BenchBody(size int) ([]byte, string) {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i * 31)
	}
	sum := md5.Sum(data)
	return data, hex.EncodeToString(sum[:])
}

// BenchmarkPutContentMD5_Stream measures the no-spool streaming path: a sized
// exact body (SizeHint + SizeHintExact) with a correct Content-MD5, which is
// validated by teeing the plaintext through md5 in a beforeCommit hook. No body
// is staged to a temp file.
func BenchmarkPutContentMD5_Stream(b *testing.B) {
	for _, tc := range putContentMD5BenchSizes {
		b.Run(tc.name, func(b *testing.B) {
			bk := newChunkedECBenchmarkBackend(b)
			require.NoError(b, bk.CreateBucket(context.Background(), "bench"))

			data, md5hex := makeMD5BenchBody(tc.size)
			size := int64(len(data))
			b.SetBytes(size)
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				obj, err := bk.PutObjectWithRequest(context.Background(), storage.PutObjectRequest{
					Bucket:        "bench",
					Key:           "key",
					Body:          bytes.NewReader(data),
					SizeHint:      &size,
					SizeHintExact: true,
					ContentType:   "application/octet-stream",
					ContentMD5Hex: md5hex,
				})
				require.NoError(b, err)
				md5BenchSink = obj.ETag
			}
		})
	}
}
