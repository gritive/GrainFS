package cluster

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

func BenchmarkWriteSegmentBlobForAppend64KiB(b *testing.B) {
	backend := newTestDistributedBackend(b)
	requireNoErrorB(b, backend.CreateBucket(context.Background(), "bench"))
	payload := bytes.Repeat([]byte("x"), 64*1024)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seg, err := backend.writeSegmentBlobForAppend("bench", fmt.Sprintf("key-%d", i), bytes.NewReader(payload))
		if err != nil {
			b.Fatalf("writeSegmentBlobForAppend: %v", err)
		}
		if seg.Size != int64(len(payload)) {
			b.Fatalf("segment size=%d, want %d", seg.Size, len(payload))
		}
	}
}

func requireNoErrorB(b *testing.B, err error) {
	b.Helper()
	if err != nil {
		b.Fatal(err)
	}
}
