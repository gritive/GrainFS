package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestForwardWrite_RoundTrip(t *testing.T) {
	srv, cli, addr := httpPair(t)
	type got struct {
		frame, body []byte
	}
	gotCh := make(chan got, 1)
	srv.RegisterForwardWriteHandler(func(frame []byte, body io.Reader) ([]byte, error) {
		b, err := io.ReadAll(body)
		if err != nil {
			return nil, err
		}
		gotCh <- got{frame: frame, body: b}
		return []byte("fb-forward-reply"), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	frame := []byte("fb-frame-\x00binary\xffbytes") // frames are raw FB bytes — base64 must round-trip arbitrary bytes
	reply, err := cli.ForwardWrite(ctx, addr, frame, bytes.NewReader([]byte("raw-s3-body")))
	if err != nil {
		t.Fatalf("ForwardWrite: %v", err)
	}
	if string(reply) != "fb-forward-reply" {
		t.Fatalf("reply = %q, want fb-forward-reply", reply)
	}
	select {
	case g := <-gotCh:
		if !bytes.Equal(g.frame, frame) {
			t.Fatalf("handler frame = %q, want %q", g.frame, frame)
		}
		if string(g.body) != "raw-s3-body" {
			t.Fatalf("handler body = %q", g.body)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not invoked")
	}
	if n := srv.InboundNativeForwardWrites(); n != 1 {
		t.Fatalf("InboundNativeForwardWrites = %d, want 1", n)
	}
}

func TestForwardRead_RoundTrip(t *testing.T) {
	srv, cli, addr := httpPair(t)
	const objBody = "streamed-object-bytes"
	gotFrame := make(chan []byte, 1)
	srv.RegisterForwardReadHandler(func(frame []byte) ([]byte, io.ReadCloser, error) {
		gotFrame <- append([]byte(nil), frame...)
		return []byte("fb-read-reply-meta"), io.NopCloser(strings.NewReader(objBody)), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	frame := []byte("fb-get-frame\x01\x02")
	reply, rc, err := cli.ForwardRead(ctx, addr, frame)
	if err != nil {
		t.Fatalf("ForwardRead: %v", err)
	}
	if string(reply) != "fb-read-reply-meta" {
		t.Fatalf("reply meta = %q", reply)
	}
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if cerr := rc.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
	if string(b) != objBody {
		t.Fatalf("body = %q, want %q", b, objBody)
	}
	select {
	case f := <-gotFrame:
		if !bytes.Equal(f, frame) {
			t.Fatalf("handler frame = %q, want %q", f, frame)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not invoked")
	}
	if n := srv.InboundNativeForwardReads(); n != 1 {
		t.Fatalf("InboundNativeForwardReads = %d, want 1", n)
	}
}

// TestForwardRead_NilBodyFromHandler: an in-band error reply has metadata but no
// stream (HandleRead returns (errReply, nil)). The wire must deliver the reply
// metadata with an EMPTY (not absent) body and a working closer.
func TestForwardRead_NilBodyFromHandler(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterForwardReadHandler(func(frame []byte) ([]byte, io.ReadCloser, error) {
		return []byte("fb-notleader-reply"), nil, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply, rc, err := cli.ForwardRead(ctx, addr, []byte("f"))
	if err != nil {
		t.Fatalf("ForwardRead: %v", err)
	}
	if string(reply) != "fb-notleader-reply" {
		t.Fatalf("reply = %q", reply)
	}
	n, _ := io.Copy(io.Discard, rc)
	if cerr := rc.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
	if n != 0 {
		t.Fatalf("body = %d bytes, want 0", n)
	}
}

func TestForwardWrite_HandlerErrorSurfaces500(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterForwardWriteHandler(func(frame []byte, body io.Reader) ([]byte, error) {
		_, _ = io.Copy(io.Discard, body)
		return nil, errors.New("receiver exploded")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.ForwardWrite(ctx, addr, []byte("f"), bytes.NewReader([]byte("x")))
	if err == nil || !strings.Contains(err.Error(), "receiver exploded") {
		t.Fatalf("err = %v, want 500 with handler text", err)
	}
}

func TestForward_NotRegistered503(t *testing.T) {
	_, cli, addr := httpPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.ForwardWrite(ctx, addr, []byte("f"), bytes.NewReader(nil)); err == nil {
		t.Fatal("write: want not-ready error")
	}
	if _, _, err := cli.ForwardRead(ctx, addr, []byte("f")); err == nil {
		t.Fatal("read: want not-ready error")
	}
}

func TestForward_MissingFrame400(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterForwardReadHandler(func(frame []byte) ([]byte, io.ReadCloser, error) {
		t.Error("handler must not run without a frame")
		return nil, nil, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := cli.httpClient()
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	status, body, err := c.Get(ctx, nil, "https://"+addr+httpForwardReadPath)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if status != 400 {
		t.Fatalf("status = %d (%s), want 400", status, body)
	}
}

// TestForwardWrite_FrameSizeBoundary: a frame at the cap round-trips through the
// header (the real wire bound is Hertz's ~1 MiB header limit, so the 256 KiB cap
// must clear it with room for other headers); one byte over is rejected
// CLIENT-SIDE with a clear error (never reaching Hertz header parsing, which
// would fail opaquely before the server handler runs).
func TestForwardWrite_FrameSizeBoundary(t *testing.T) {
	srv, cli, addr := httpPair(t)
	gotLen := make(chan int, 1)
	srv.RegisterForwardWriteHandler(func(frame []byte, body io.Reader) ([]byte, error) {
		_, _ = io.Copy(io.Discard, body)
		gotLen <- len(frame)
		return []byte("ok"), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	atCap := bytes.Repeat([]byte("k"), maxForwardFrameBytes)
	if _, err := cli.ForwardWrite(ctx, addr, atCap, bytes.NewReader([]byte("b"))); err != nil {
		t.Fatalf("ForwardWrite at cap: %v", err)
	}
	if n := <-gotLen; n != maxForwardFrameBytes {
		t.Fatalf("frame len = %d, want %d", n, maxForwardFrameBytes)
	}

	over := bytes.Repeat([]byte("k"), maxForwardFrameBytes+1)
	_, err := cli.ForwardWrite(ctx, addr, over, bytes.NewReader([]byte("b")))
	if err == nil || !strings.Contains(err.Error(), "frame size") {
		t.Fatalf("over-cap err = %v, want client-side frame size error", err)
	}
}
