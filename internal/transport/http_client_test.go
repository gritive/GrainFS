package transport

import (
	"context"
	"io"
	"runtime"
	"testing"
	"time"

	errs "github.com/cloudwego/hertz/pkg/common/errors"
	"github.com/cloudwego/hertz/pkg/protocol"
)

// httpPair brings up a same-PSK server+client and waits until the server accepts.
func httpPair(t *testing.T) (srv, cli *HTTPTransport, addr string) {
	t.Helper()
	srv = MustNewHTTPTransport("dp-psk")
	cli = MustNewHTTPTransport("dp-psk")
	t.Cleanup(func() { srv.Close(); cli.Close() })
	addr = listenHTTP(t, srv)
	if err := pingReady(t, cli, addr); err != nil {
		t.Fatalf("server not ready: %v", err)
	}
	return srv, cli, addr
}

// TestHTTPNative_MemoryFlat is the ROADMAP headline check: a large body streams
// over the native shard-write route without full-body buffering. Measured via
// TotalAlloc (monotonic cumulative) — NOT HeapAlloc, which would be ~0 after the
// round-trip even for a buffering impl that already freed its buffer. A 256 MiB
// buffer allocation on either side would push TotalAlloc past body/4; streaming's
// reused io.Copy buffer keeps it to a few MiB.
func TestHTTPNative_MemoryFlat(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("race instrumentation inflates TotalAlloc, making the streaming-vs-buffering threshold meaningless")
	}
	srv, cli, addr := httpPair(t)
	srv.RegisterShardWriteHandler(func(req ShardWriteRequest, body io.Reader) error {
		_, err := io.Copy(io.Discard, body)
		return err
	})

	const bodyLen = 256 << 20 // 256 MiB
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	err := cli.ShardWrite(ctx, addr, ShardWriteRequest{Bucket: "b", Key: "k", ShardIdx: 0},
		&limitedZeroReader{remaining: bodyLen})
	if err != nil {
		t.Fatalf("ShardWrite: %v", err)
	}
	runtime.ReadMemStats(&m2)

	delta := m2.TotalAlloc - m1.TotalAlloc
	if delta > bodyLen/4 {
		t.Fatalf("TotalAlloc delta %d MiB exceeds body/4 (%d MiB) — body was buffered, not streamed",
			delta>>20, (bodyLen/4)>>20)
	}
	t.Logf("256 MiB streamed; TotalAlloc delta = %d MiB (flat)", delta>>20)
}

// TestHTTPDataPlane_RetryIf_RefusesBodyStream pins the load-bearing safety of the
// custom retry policy: httpRetryIf must NEVER retry a streamed-body request
// (ShardWrite/ForwardWrite's one-shot pipe body — the S3b "retry-after-body"
// landmine), even on a stale-pooled-conn error it would otherwise retry. A
// buffered (SetBody) request on the same error IS retried.
func TestHTTPDataPlane_RetryIf_RefusesBodyStream(t *testing.T) {
	stream := protocol.AcquireRequest()
	defer protocol.ReleaseRequest(stream)
	stream.SetMethod("POST")
	stream.SetRequestURI("https://peer" + httpShardWritePath)
	stream.SetBodyStream(&limitedZeroReader{remaining: 1 << 20}, -1)
	if !stream.IsBodyStream() {
		t.Fatal("precondition: request must be a body stream")
	}
	if httpRetryIf(stream, protocol.AcquireResponse(), errs.ErrBadPoolConn) {
		t.Fatal("httpRetryIf must refuse to retry a streamed-body request even on ErrBadPoolConn (S3b safety)")
	}

	// A buffered (rewindable) request on the same stale-conn error must retry.
	buffered := protocol.AcquireRequest()
	defer protocol.ReleaseRequest(buffered)
	buffered.SetMethod("POST")
	buffered.SetRequestURI("https://peer" + RouteRaftDataRPC)
	buffered.SetBody([]byte("raft-rpc-envelope"))
	if !httpRetryIf(buffered, protocol.AcquireResponse(), errs.ErrBadPoolConn) {
		t.Fatal("httpRetryIf must retry a buffered request on ErrBadPoolConn (the stale-keep-alive fix)")
	}
	if httpRetryIf(buffered, protocol.AcquireResponse(), nil) {
		t.Fatal("httpRetryIf must NOT retry on a nil error")
	}
}

// limitedZeroReader yields `remaining` zero bytes lazily (no full-body allocation),
// so the streaming tests can push hundreds of MiB without allocating them up front.
type limitedZeroReader struct{ remaining int64 }

func (z *limitedZeroReader) Read(p []byte) (int, error) {
	if z.remaining <= 0 {
		return 0, io.EOF
	}
	n := len(p)
	if int64(n) > z.remaining {
		n = int(z.remaining)
	}
	clear(p[:n])
	z.remaining -= int64(n)
	return n, nil
}
