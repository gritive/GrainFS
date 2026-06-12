package transport

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

// stallBodyReader sends a fixed prefix on the first Read(s) then blocks forever
// (until release is closed, so the server's body goroutine is not leaked). It
// models a peer that streams a few bytes of a shard then stalls mid-body.
type stallBodyReader struct {
	prefix  []byte
	off     int
	release <-chan struct{}
}

func (r *stallBodyReader) Read(p []byte) (int, error) {
	if r.off < len(r.prefix) {
		n := copy(p, r.prefix[r.off:])
		r.off += n
		return n, nil
	}
	<-r.release
	return 0, io.EOF
}

// TestHTTPShardRead_IdleDeadline is the FIRING test for the S8-5 mandatory
// flip gate (Task 1): a streaming shard read whose peer stalls mid-body must
// surface a timeout IN THE SAME GOROUTINE within the idle window — never hang
// forever. This is the tcpReadCloser (S3b-cbd) parity that is load-bearing now
// that HTTP is the only transport.
//
// Mutation-verify (done manually, RED-confirmed): set clientBodyTimeout = 0 →
// wrapIdleReadConn returns the conn unwrapped → no read deadline → the stalled Read
// hangs → the 5s timer below fires → test fails. With the idle bound armed the Read
// returns a timeout error at ~clientBodyTimeout.
func TestHTTPShardRead_IdleDeadline(t *testing.T) {
	srv := MustNewHTTPTransport("idle-psk")
	cli := MustNewHTTPTransport("idle-psk")
	cli.clientBodyTimeout = 300 * time.Millisecond // armed BEFORE the lazy client build
	t.Cleanup(func() { srv.Close(); cli.Close() })

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	prefix := []byte("PREFIX")
	srv.RegisterShardReadHandler(func(req ShardReadRequest) (io.ReadCloser, error) {
		return io.NopCloser(&stallBodyReader{prefix: prefix, release: release}), nil
	})

	addr := listenHTTP(t, srv)
	if err := pingReady(t, cli, addr); err != nil {
		t.Fatalf("server not ready: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body, err := cli.ShardRead(ctx, addr, ShardReadRequest{Bucket: "b", Key: "k", ShardIdx: 0})
	if err != nil {
		t.Fatalf("ShardRead: %v", err)
	}
	defer body.Close()

	// Drain the prefix the server sent before stalling.
	got := make([]byte, len(prefix))
	if _, err := io.ReadFull(body, got); err != nil {
		t.Fatalf("read prefix: %v", err)
	}

	// The server now stalls forever; the NEXT Read must error within the idle window.
	errc := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		_, rerr := body.Read(buf)
		errc <- rerr
	}()
	select {
	case rerr := <-errc:
		if rerr == nil {
			t.Fatal("stalled ShardRead body Read returned nil error; idle deadline not enforced")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stalled ShardRead body Read did not return within 5s — idle deadline not armed (hang)")
	}
}

// TestHTTPShardRead_ReuseAfterIdleTrip proves the idle bound does not turn into a
// worse availability bug: after a stall trips the idle deadline (poisoning that
// conn), a later RPC on the SAME client must still succeed. A poisoned conn left in
// the keep-alive pool would fail the next reuse; this asserts the observable
// guarantee (the bad conn is discarded / not silently reused).
func TestHTTPShardRead_ReuseAfterIdleTrip(t *testing.T) {
	srv := MustNewHTTPTransport("reuse-psk")
	cli := MustNewHTTPTransport("reuse-psk")
	cli.clientBodyTimeout = 300 * time.Millisecond
	t.Cleanup(func() { srv.Close(); cli.Close() })

	release := make(chan struct{})
	var relOnce sync.Once
	rel := func() { relOnce.Do(func() { close(release) }) }
	t.Cleanup(rel)
	srv.RegisterShardReadHandler(func(req ShardReadRequest) (io.ReadCloser, error) {
		return io.NopCloser(&stallBodyReader{prefix: []byte("X"), release: release}), nil
	})
	srv.RegisterBufferedRoute(RouteShardRPC, func(payload []byte) ([]byte, error) {
		return append([]byte("echo:"), payload...), nil
	})

	addr := listenHTTP(t, srv)
	if err := pingReady(t, cli, addr); err != nil {
		t.Fatalf("server not ready: %v", err)
	}

	// Trip the idle deadline on a streaming shard read.
	ctx, cancel := context.WithCancel(context.Background())
	body, err := cli.ShardRead(ctx, addr, ShardReadRequest{Bucket: "b", Key: "k", ShardIdx: 0})
	if err != nil {
		cancel()
		t.Fatalf("ShardRead: %v", err)
	}
	one := make([]byte, 1)
	if _, err := io.ReadFull(body, one); err != nil {
		body.Close()
		cancel()
		t.Fatalf("read prefix: %v", err)
	}
	buf := make([]byte, 64)
	if _, err := body.Read(buf); err == nil {
		body.Close()
		cancel()
		t.Fatal("expected idle-trip error on the stalled body")
	}
	body.Close()
	cancel()
	rel() // unblock the stalled server goroutine so teardown is not held by graceful shutdown

	// A fresh RPC on the same client must succeed.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	reply, err := cli.CallBuffered(ctx2, addr, RouteShardRPC, []byte("hi"))
	if err != nil {
		t.Fatalf("CallBuffered after idle-trip must succeed (poisoned conn must not be reused): %v", err)
	}
	if string(reply) != "echo:hi" {
		t.Fatalf("reply = %q, want echo:hi", reply)
	}
}
