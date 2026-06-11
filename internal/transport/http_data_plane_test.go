package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/protocol"
	protoclient "github.com/cloudwego/hertz/pkg/protocol/client"
)

const testStreamType = StreamShardWriteBody

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

func TestHTTPDataPlane_CallRoundTrip(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.Handle(testStreamType, func(req *Message) *Message {
		return NewResponse(req, append([]byte("echo:"), req.Payload...))
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := cli.Call(ctx, addr, &Message{Type: testStreamType, ID: 7, Payload: []byte("hi")})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if string(resp.Payload) != "echo:hi" {
		t.Fatalf("payload = %q, want echo:hi", resp.Payload)
	}
	if resp.Status != StatusOK {
		t.Fatalf("status = %d, want OK", resp.Status)
	}
}

func TestHTTPDataPlane_CallWithBody(t *testing.T) {
	srv, cli, addr := httpPair(t)
	var got int64
	srv.HandleBody(testStreamType, func(req *Message, body io.Reader) *Message {
		n, err := io.Copy(io.Discard, body)
		if err != nil {
			return NewErrorResponse(req, StatusError, err)
		}
		atomic.StoreInt64(&got, n)
		var nb [8]byte
		binary.BigEndian.PutUint64(nb[:], uint64(n))
		return NewResponse(req, nb[:])
	})

	const bodyLen = 4 << 20 // 4 MiB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cli.CallWithBody(ctx, addr, &Message{Type: testStreamType, ID: 1}, &limitedZeroReader{remaining: bodyLen})
	if err != nil {
		t.Fatalf("CallWithBody: %v", err)
	}
	if resp.Status != StatusOK {
		t.Fatalf("status = %d (%s), want OK", resp.Status, resp.Payload)
	}
	if n := atomic.LoadInt64(&got); n != bodyLen {
		t.Fatalf("server received %d bytes, want %d", n, bodyLen)
	}
	if binary.BigEndian.Uint64(resp.Payload) != bodyLen {
		t.Fatalf("reply count = %d, want %d", binary.BigEndian.Uint64(resp.Payload), bodyLen)
	}
}

func TestHTTPDataPlane_CallReadStream(t *testing.T) {
	srv, cli, addr := httpPair(t)
	const bodyLen = 2 << 20 // 2 MiB
	srv.HandleRead(testStreamType, func(req *Message) (*Message, io.ReadCloser) {
		return NewResponse(req, []byte("ok")), io.NopCloser(&limitedZeroReader{remaining: bodyLen})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, body, err := cli.CallRead(ctx, addr, &Message{Type: testStreamType, ID: 2})
	if err != nil {
		t.Fatalf("CallRead: %v", err)
	}
	if string(resp.Payload) != "ok" {
		t.Fatalf("metadata payload = %q, want ok", resp.Payload)
	}
	n, err := io.Copy(io.Discard, body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if err := body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}
	if n != bodyLen {
		t.Fatalf("read %d bytes, want %d", n, bodyLen)
	}
}

// TestHTTPDataPlane_MemoryFlat is the ROADMAP headline check: a large body streams
// without full-body buffering. Measured via TotalAlloc (monotonic cumulative) — NOT
// HeapAlloc, which would be ~0 after the round-trip even for a buffering impl that
// already freed its buffer. A 256 MiB buffer allocation on either side would push
// TotalAlloc past body/4; streaming's reused io.Copy buffer keeps it to a few MiB.
func TestHTTPDataPlane_MemoryFlat(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("race instrumentation inflates TotalAlloc, making the streaming-vs-buffering threshold meaningless")
	}
	srv, cli, addr := httpPair(t)
	srv.HandleBody(testStreamType, func(req *Message, body io.Reader) *Message {
		_, err := io.Copy(io.Discard, body)
		if err != nil {
			return NewErrorResponse(req, StatusError, err)
		}
		return NewResponse(req, nil)
	})

	const bodyLen = 256 << 20 // 256 MiB
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	resp, err := cli.CallWithBody(ctx, addr, &Message{Type: testStreamType, ID: 3}, &limitedZeroReader{remaining: bodyLen})
	if err != nil {
		t.Fatalf("CallWithBody: %v", err)
	}
	if resp.Status != StatusOK {
		t.Fatalf("status = %d, want OK", resp.Status)
	}
	runtime.ReadMemStats(&m2)

	delta := m2.TotalAlloc - m1.TotalAlloc
	if delta > bodyLen/4 {
		t.Fatalf("TotalAlloc delta %d MiB exceeds body/4 (%d MiB) — body was buffered, not streamed",
			delta>>20, (bodyLen/4)>>20)
	}
	t.Logf("256 MiB streamed; TotalAlloc delta = %d MiB (flat)", delta>>20)
}

// TestHTTPDataPlane_ErrorStatusMapsToError: an RPC-level StatusError reply must map
// to a Go error (nil Message), mirroring every TCP Call* path's checkResponseStatus —
// NOT a Message with err==nil that consumers (shard_service.Ping) take as success.
// The transport delivered (HTTP 200); the RPC failed.
func TestHTTPDataPlane_ErrorStatusMapsToError(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.Handle(testStreamType, func(req *Message) *Message {
		return NewErrorResponse(req, StatusError, io.ErrUnexpectedEOF)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := cli.Call(ctx, addr, &Message{Type: testStreamType, ID: 4})
	if err == nil {
		t.Fatalf("StatusError reply must surface as a Go error, got resp=%+v", resp)
	}
	if resp != nil {
		t.Fatalf("error reply must return a nil Message, got %+v", resp)
	}
}

// TestHTTPDataPlane_CallReadErrorStatus: a StatusError metadata frame on CallRead
// must return an error and NO body (mirror TCP CallRead checkResponseStatus).
func TestHTTPDataPlane_CallReadErrorStatus(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.HandleRead(testStreamType, func(req *Message) (*Message, io.ReadCloser) {
		return NewErrorResponse(req, StatusError, io.ErrUnexpectedEOF), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, body, err := cli.CallRead(ctx, addr, &Message{Type: testStreamType, ID: 8})
	if err == nil {
		if body != nil {
			_ = body.Close()
		}
		t.Fatalf("CallRead StatusError must surface as a Go error, got resp=%+v", resp)
	}
	if body != nil {
		t.Fatal("error reply must not return a body")
	}
}

// TestHTTPDataPlane_CallReadPartialClose exercises the trickiest lifecycle: read only
// a few bytes then Close, repeatedly. The pooled conn must not be corrupted/leaked —
// a final full round-trip after many partial reads must still succeed.
func TestHTTPDataPlane_CallReadPartialClose(t *testing.T) {
	srv, cli, addr := httpPair(t)
	const bodyLen = 1 << 20
	srv.HandleRead(testStreamType, func(req *Message) (*Message, io.ReadCloser) {
		return NewResponse(req, nil), io.NopCloser(&limitedZeroReader{remaining: bodyLen})
	})

	for i := 0; i < 30; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, body, err := cli.CallRead(ctx, addr, &Message{Type: testStreamType, ID: uint64(i)})
		if err != nil {
			cancel()
			t.Fatalf("CallRead #%d: %v", i, err)
		}
		buf := make([]byte, 16)
		_, _ = io.ReadFull(body, buf)
		_ = body.Close()
		cancel()
	}
	// Pool must still be healthy.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, body, err := cli.CallRead(ctx, addr, &Message{Type: testStreamType, ID: 99})
	if err != nil {
		t.Fatalf("CallRead after partial reads: %v", err)
	}
	n, _ := io.Copy(io.Discard, body)
	_ = body.Close()
	if n != bodyLen {
		t.Fatalf("final full read got %d, want %d", n, bodyLen)
	}
}

// TestHTTPDataPlane_LargeBufferedResponse exercises the wire-format asymmetry this
// slice introduced: a NON-streaming response carries its payload in the response
// BODY (not a header) precisely because ReadShard (CallFlatBuffer) returns whole-shard
// data in Message.Payload. The largest other test is an 8-byte ack, so prove a large
// (8 MiB) buffered response round-trips intact via ctx.Response.SetBody → io.ReadAll.
func TestHTTPDataPlane_LargeBufferedResponse(t *testing.T) {
	srv, cli, addr := httpPair(t)
	const payloadLen = 8 << 20
	want := make([]byte, payloadLen)
	for i := range want {
		want[i] = byte(i * 7)
	}
	srv.Handle(testStreamType, func(req *Message) *Message {
		return NewResponse(req, want)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := cli.Call(ctx, addr, &Message{Type: testStreamType, ID: 5})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if len(resp.Payload) != payloadLen {
		t.Fatalf("payload len = %d, want %d", len(resp.Payload), payloadLen)
	}
	if !bytes.Equal(resp.Payload, want) {
		t.Fatal("large buffered response payload corrupted in round-trip")
	}
}

// TestHTTPDataPlane_LargeCallPayload is the S8-3 firing test: entries-bearing
// AppendEntries (~16 MiB of raft log) and InstallSnapshot travel over plain Call
// with a large Message.Payload. The payload must ride the request BODY, not the
// X-Gfs-Payload header — an 8 MiB base64 header would blow Hertz's header limit.
// RED against the S8-2 header-based code.
func TestHTTPDataPlane_LargeCallPayload(t *testing.T) {
	srv, cli, addr := httpPair(t)
	const payloadLen = 8 << 20 // 8 MiB (entries-AE-sized)
	want := make([]byte, payloadLen)
	for i := range want {
		want[i] = byte(i*31 + 7)
	}
	srv.Handle(testStreamType, func(req *Message) *Message {
		return NewResponse(req, req.Payload) // echo so we verify the payload arrived intact
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := cli.Call(ctx, addr, &Message{Type: testStreamType, ID: 11, Payload: want})
	if err != nil {
		t.Fatalf("large Call (payload must ride the body): %v", err)
	}
	if len(resp.Payload) != payloadLen || !bytes.Equal(resp.Payload, want) {
		t.Fatalf("large Call payload corrupted in round-trip: got %d bytes", len(resp.Payload))
	}
}

// TestHTTPDataPlane_DefaultRetryIf_RefusesBodyStream pins the Hertz contract that
// makes CallWithBody's one-shot streamed body safe: a streamed-body request is never
// retried. RED if a Hertz upgrade changes DefaultRetryIf (then a streamed shard write
// could be re-sent after the body was already consumed — the S3b landmine).
func TestHTTPDataPlane_DefaultRetryIf_RefusesBodyStream(t *testing.T) {
	req := protocol.AcquireRequest()
	defer protocol.ReleaseRequest(req)
	req.SetMethod("POST")
	req.SetRequestURI("https://peer/_grainfs/rpc")
	req.SetBodyStream(&limitedZeroReader{remaining: 1 << 20}, -1)
	if !req.IsBodyStream() {
		t.Fatal("precondition: request must be a body stream")
	}
	if protoclient.DefaultRetryIf(req, protocol.AcquireResponse(), nil) {
		t.Fatal("DefaultRetryIf must refuse to retry a streamed-body request (S3b safety)")
	}
}

// TestHTTPDataPlane_SendReceiveGossip: a fire-and-forget Send (no handler for the
// type) is delivered to the peer's inbox (Receive), and Send sees a clean reply.
func TestHTTPDataPlane_SendReceiveGossip(t *testing.T) {
	srv, cli, addr := httpPair(t)
	const gossipType = StreamReceipt // no handler registered → inbox path
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Send(ctx, addr, &Message{Type: gossipType, Payload: []byte("gossip")}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case rm := <-srv.Receive():
		if rm.Message.Type != gossipType || string(rm.Message.Payload) != "gossip" {
			t.Fatalf("inbox message mismatch: %+v", rm.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gossip Send did not reach the peer inbox (Receive)")
	}
}

func TestHTTPDataPlane_RespMetaMissingFrame(t *testing.T) {
	// A response with no X-Gfs-* headers must surface a clean error, not a panic.
	resp := protocol.AcquireResponse()
	defer protocol.ReleaseResponse(resp)
	resp.SetStatusCode(200)
	if _, err := respMeta(resp); err == nil {
		t.Fatal("respMeta on a header-less response must error")
	}
}

// zeroReader yields `remaining` zero bytes lazily (no full-body allocation), so the
// streaming tests can push hundreds of MiB without allocating them up front.
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
