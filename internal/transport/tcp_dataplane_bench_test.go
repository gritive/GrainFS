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

// BenchmarkQUICDataPlanePut is the QUIC arm of the same 16-MB-ish data-plane PUT
// workload, for an apples-to-apples TCP-vs-QUIC comparison. RUN BOTH ON GCP LINUX:
//
//	go test ./internal/transport/ -run '^$' -bench 'DataPlanePut' -benchtime=200x -cpu 1,4,8
//
// The MB/s ratio (TCP/QUIC) is the EARLY PARTIAL parity signal — it answers "did
// our chunked framing + pooling preserve the structural kernel-TCP win over
// userspace-QUIC on bulk?" It is NOT the full S5 gate (which requires the
// concurrent/mixed bulk×heartbeat workload + elastic pool, i.e. S3b). On a macOS
// dev box the ratio is meaningless (the 0.36x QUIC penalty is Linux-CPU-specific).
func BenchmarkQUICDataPlanePut(b *testing.B) {
	const psk = "bench-dp-quic"
	srv := MustNewQUICTransport(psk)
	if err := srv.Listen(context.Background(), "127.0.0.1:0"); err != nil {
		b.Fatal(err)
	}
	defer srv.Close()
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		_, _ = io.Copy(io.Discard, body)
		return NewResponse(req, nil)
	})
	cli := MustNewQUICTransport(psk)
	defer cli.Close()
	addr := srv.LocalAddr()

	payload := make([]byte, 4<<20) // 4 MiB shard-ish — same as the TCP arm
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
