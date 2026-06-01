package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"
	"time"
)

// BenchmarkTCPDataPlanePut measures pooled CallWithBody throughput over loopback.
// LOCAL CORRECTNESS / NON-REGRESSION ONLY — a macOS/dev loopback CANNOT reproduce
// the GCP-Linux 0.36x QUIC gap, so this does NOT validate the S5 >=1.0x parity
// gate. Parity is a Linux measurement at S5. See the S3a plan's "Benchmark scope".
func BenchmarkTCPDataPlanePut(b *testing.B) {
	const psk = "bench-dp"
	srv := MustNewTCPTransport(psk)
	if err := srv.Listen(context.Background(), "127.0.0.1:0"); err != nil {
		b.Fatal(err)
	}
	defer srv.Close()
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		_, _ = io.Copy(io.Discard, body)
		return NewResponse(req, nil)
	})
	cli := MustNewTCPTransport(psk)
	defer cli.Close()
	addr := srv.LocalAddr()

	payload := make([]byte, 4<<20) // 4 MiB shard-ish
	_, _ = rand.Read(payload)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err := cli.CallWithBody(ctx, addr, &Message{Type: StreamShardWriteBody}, bytes.NewReader(payload))
			cancel()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
