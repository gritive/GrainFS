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

func TestAppendSegmentRead_RoundTrip(t *testing.T) {
	srv, cli, addr := httpPair(t)
	const segBody = "streamed-segment-bytes"
	gotFrame := make(chan []byte, 1)
	srv.RegisterAppendSegmentReadHandler(func(frame []byte) ([]byte, io.ReadCloser, error) {
		gotFrame <- append([]byte(nil), frame...)
		return []byte{0x00}, io.NopCloser(strings.NewReader(segBody)), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	frame := []byte("append-frame-\x00binary\xffbytes") // frames are raw binary — base64 must round-trip arbitrary bytes
	reply, rc, err := cli.AppendSegmentRead(ctx, addr, frame)
	if err != nil {
		t.Fatalf("AppendSegmentRead: %v", err)
	}
	if !bytes.Equal(reply, []byte{0x00}) {
		t.Fatalf("reply = %v, want [0x00]", reply)
	}
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if cerr := rc.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
	if string(b) != segBody {
		t.Fatalf("body = %q, want %q", b, segBody)
	}
	select {
	case f := <-gotFrame:
		if !bytes.Equal(f, frame) {
			t.Fatalf("handler frame = %q, want %q", f, frame)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not invoked")
	}
	if n := srv.InboundNativeAppendSegmentReads(); n != 1 {
		t.Fatalf("InboundNativeAppendSegmentReads = %d, want 1", n)
	}
}

// TestAppendSegmentRead_NilBodyFromHandler: an in-band ENOENT reply has
// metadata but no stream (the handler returns (status-frame, nil)). The wire
// must deliver the reply metadata with an EMPTY (not absent) body and a
// working closer.
func TestAppendSegmentRead_NilBodyFromHandler(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterAppendSegmentReadHandler(func(frame []byte) ([]byte, io.ReadCloser, error) {
		return []byte{0x01}, nil, nil // ENOENT: segment not on this node
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply, rc, err := cli.AppendSegmentRead(ctx, addr, []byte("f"))
	if err != nil {
		t.Fatalf("AppendSegmentRead: %v", err)
	}
	if !bytes.Equal(reply, []byte{0x01}) {
		t.Fatalf("reply = %v, want [0x01]", reply)
	}
	n, _ := io.Copy(io.Discard, rc)
	if cerr := rc.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
	if n != 0 {
		t.Fatalf("body = %d bytes, want 0", n)
	}
}

func TestAppendSegmentRead_HandlerErrorSurfaces500(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterAppendSegmentReadHandler(func(frame []byte) ([]byte, io.ReadCloser, error) {
		return nil, nil, errors.New("lookup exploded")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := cli.AppendSegmentRead(ctx, addr, []byte("f"))
	if err == nil || !strings.Contains(err.Error(), "lookup exploded") {
		t.Fatalf("err = %v, want 500 with handler text", err)
	}
}

func TestAppendSegmentRead_NotRegistered503(t *testing.T) {
	_, cli, addr := httpPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := cli.AppendSegmentRead(ctx, addr, []byte("f"))
	if err == nil || !strings.Contains(err.Error(), "status 503") {
		t.Fatalf("err = %v, want not-ready 503 error", err)
	}
}

// TestAppendSegmentRead_FrameSizeBoundary: a frame at the cap round-trips
// through the header; one byte over is rejected CLIENT-SIDE with a clear error
// (never reaching Hertz header parsing, which would fail opaquely before the
// server handler runs). Mirrors TestForwardWrite_FrameSizeBoundary.
func TestAppendSegmentRead_FrameSizeBoundary(t *testing.T) {
	srv, cli, addr := httpPair(t)
	gotLen := make(chan int, 1)
	srv.RegisterAppendSegmentReadHandler(func(frame []byte) ([]byte, io.ReadCloser, error) {
		gotLen <- len(frame)
		return []byte{0x01}, nil, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	atCap := bytes.Repeat([]byte("k"), maxAppendFrameBytes)
	reply, rc, err := cli.AppendSegmentRead(ctx, addr, atCap)
	if err != nil {
		t.Fatalf("AppendSegmentRead at cap: %v", err)
	}
	_ = rc.Close()
	if !bytes.Equal(reply, []byte{0x01}) {
		t.Fatalf("reply = %v", reply)
	}
	if n := <-gotLen; n != maxAppendFrameBytes {
		t.Fatalf("frame len = %d, want %d", n, maxAppendFrameBytes)
	}

	over := bytes.Repeat([]byte("k"), maxAppendFrameBytes+1)
	_, _, err = cli.AppendSegmentRead(ctx, addr, over)
	if err == nil || !strings.Contains(err.Error(), "frame size") {
		t.Fatalf("over-cap err = %v, want client-side frame size error", err)
	}
}
