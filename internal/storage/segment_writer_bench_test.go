package storage

import (
	"bytes"
	"context"
	"io"
	"testing"
)

type benchmarkReaderSegmentBackend struct{}

func (benchmarkReaderSegmentBackend) WriteSegment(_ context.Context, _ string, _ string, idx int, r io.Reader) (SegmentRef, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return SegmentRef{}, err
	}
	return SegmentRef{
		BlobID:   string(rune('a' + idx)),
		Size:     int64(len(data)),
		Checksum: ChecksumOf(data),
	}, nil
}

type benchmarkBytesSegmentBackend struct{}

func (benchmarkBytesSegmentBackend) WriteSegment(ctx context.Context, bucket, key string, idx int, r io.Reader) (SegmentRef, error) {
	return benchmarkReaderSegmentBackend{}.WriteSegment(ctx, bucket, key, idx, r)
}

func (benchmarkBytesSegmentBackend) WriteSegmentBytes(_ context.Context, _ string, _ string, idx int, data []byte) (SegmentRef, error) {
	return SegmentRef{
		BlobID:   string(rune('a' + idx)),
		Size:     int64(len(data)),
		Checksum: ChecksumOf(data),
	}, nil
}

func BenchmarkSegmentWriterBackendDispatch(b *testing.B) {
	payload := makePattern(DefaultChunkSize)
	cases := []struct {
		name    string
		backend segmentWriterBackend
	}{
		{name: "reader-path", backend: benchmarkReaderSegmentBackend{}},
		{name: "byte-fast-path", backend: benchmarkBytesSegmentBackend{}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for i := 0; i < b.N; i++ {
				obj, err := NewSegmentWriter(tc.backend).Write(context.Background(), "bench", "object", "application/octet-stream", bytes.NewReader(payload))
				if err != nil {
					b.Fatal(err)
				}
				if obj.Size != int64(len(payload)) {
					b.Fatalf("size: got %d, want %d", obj.Size, len(payload))
				}
			}
		})
	}
}
