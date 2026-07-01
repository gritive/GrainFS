package transport

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestBufferedRoute_RoundTrip(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterBufferedRoute(RouteShardRPC, func(payload []byte) ([]byte, error) {
		return append([]byte("echo:"), payload...), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload := []byte("fb-frame-\x00binary\xffbytes") // payloads are raw FB/binary frames
	reply, err := cli.CallBuffered(ctx, addr, RouteShardRPC, payload)
	if err != nil {
		t.Fatalf("CallBuffered: %v", err)
	}
	if want := append([]byte("echo:"), payload...); !bytes.Equal(reply, want) {
		t.Fatalf("reply = %q, want %q", reply, want)
	}
	if n := srv.InboundNativeBuffered(RouteShardRPC); n != 1 {
		t.Fatalf("InboundNativeBuffered = %d, want 1", n)
	}
	if n := srv.InboundNativeBuffered(RouteRaftDataRPC); n != 0 {
		t.Fatalf("InboundNativeBuffered(other route) = %d, want 0", n)
	}
}

// TestBufferedRoute_EmptyPayloadAndReply: empty request payloads ARE legal
// (probe families send empty requests) and an empty reply is a clean success.
func TestBufferedRoute_EmptyPayloadAndReply(t *testing.T) {
	srv, cli, addr := httpPair(t)
	gotLen := make(chan int, 1)
	srv.RegisterBufferedRoute(RouteProbeAppliedIndex, func(payload []byte) ([]byte, error) {
		gotLen <- len(payload)
		return nil, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply, err := cli.CallBuffered(ctx, addr, RouteProbeAppliedIndex, nil)
	if err != nil {
		t.Fatalf("CallBuffered(empty): %v", err)
	}
	if len(reply) != 0 {
		t.Fatalf("reply = %q, want empty", reply)
	}
	select {
	case n := <-gotLen:
		if n != 0 {
			t.Fatalf("handler payload len = %d, want 0", n)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not invoked")
	}
}

func TestBufferedRoute_HandlerErrorSurfaces500(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterBufferedRoute(RouteRaftDataRPC, func(payload []byte) ([]byte, error) {
		return nil, errors.New("handler exploded")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.CallBuffered(ctx, addr, RouteRaftDataRPC, []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "handler exploded") {
		t.Fatalf("err = %v, want 500 with handler text", err)
	}
	if !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("err = %v, want status 500", err)
	}
}

// TestBufferedRoute_AllRoutesRegistered503: every buffered route in the table
// is registered at Listen even before a handler arrives — an unregistered
// family answers 503 not-ready (NOT a Hertz 404), so a client that migrates
// ahead of its server peer sees a clean retryable error.
func TestBufferedRoute_AllRoutesRegistered503(t *testing.T) {
	_, cli, addr := httpPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, rt := range bufferedRouteTable {
		_, err := cli.CallBuffered(ctx, addr, rt.path, []byte("x"))
		if err == nil || !strings.Contains(err.Error(), "status 503") {
			t.Fatalf("route %s: err = %v, want status 503 not-ready", rt.path, err)
		}
	}
}

func TestBufferedRouteTable_DataGroupProposeRouteRetired(t *testing.T) {
	for _, rt := range bufferedRouteTable {
		if rt.path == "/forward/propose/data-group" {
			t.Fatalf("retired data-group propose route is still declared")
		}
	}
}

func TestBufferedRoute_UnregisterRevertsTo503(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterBufferedRoute(RouteReceiptQuery, func(payload []byte) ([]byte, error) { return payload, nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.CallBuffered(ctx, addr, RouteReceiptQuery, []byte("x")); err != nil {
		t.Fatalf("CallBuffered: %v", err)
	}
	srv.RegisterBufferedRoute(RouteReceiptQuery, nil)
	_, err := cli.CallBuffered(ctx, addr, RouteReceiptQuery, []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "status 503") {
		t.Fatalf("err = %v, want status 503 after unregister", err)
	}
}

func TestBufferedRoute_UnknownPathRegistrationPanics(t *testing.T) {
	srv := MustNewHTTPTransport("dp-psk")
	t.Cleanup(func() { srv.Close() })
	defer func() {
		if recover() == nil {
			t.Fatal("RegisterBufferedRoute on undeclared path must panic")
		}
	}()
	srv.RegisterBufferedRoute("/not/in/the/table", func(payload []byte) ([]byte, error) { return nil, nil })
}

// TestBufferedRoute_ReplyCapEnforced: a reply larger than maxPayloadSize is
// rejected client-side (WithResponseBodyStream bypasses Hertz's
// MaxResponseBodySize, mirroring the tunnel's codec cap).
func TestBufferedRoute_ReplyCapEnforced(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterBufferedRoute(RouteShardRPC, func(payload []byte) ([]byte, error) {
		return make([]byte, maxPayloadSize+1), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := cli.CallBuffered(ctx, addr, RouteShardRPC, []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("err = %v, want reply-cap error", err)
	}
}

// TestBufferedRoute_RequestCapEnforced: a request payload larger than
// maxPayloadSize is rejected by the server with 400 (the tunnel's
// readReqBodyPayload guard, kept by the native primitive).
func TestBufferedRoute_RequestCapEnforced(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterBufferedRoute(RouteShardRPC, func(payload []byte) ([]byte, error) {
		t.Error("handler must not run on an over-cap request")
		return nil, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := cli.CallBuffered(ctx, addr, RouteShardRPC, make([]byte, maxPayloadSize+1))
	if err == nil || !strings.Contains(err.Error(), "status 400") {
		t.Fatalf("err = %v, want status 400 request-cap error", err)
	}
}

// TestBufferedRoute_HandlerPanicContained: a panicking handler (the corrupt-
// FlatBuffer lazy-accessor class) must surface as a per-request error — NOT
// kill the server process. hzserver.New ships no recovery middleware; Listen
// installs recovery.Recovery() explicitly, and this test is its FIRING proof:
// without the middleware the panic crashes the test process here.
func TestBufferedRoute_HandlerPanicContained(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterBufferedRoute(RouteProbeCapability, func(payload []byte) ([]byte, error) {
		panic("corrupt flatbuffer: slice bounds out of range")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.CallBuffered(ctx, addr, RouteProbeCapability, []byte("x"))
	if err == nil {
		t.Fatal("want error from panicking handler, got nil")
	}

	// The server must still serve subsequent requests on other routes.
	srv.RegisterBufferedRoute(RouteProbeKEKDisk, func(payload []byte) ([]byte, error) {
		return []byte("alive"), nil
	})
	reply, err := cli.CallBuffered(ctx, addr, RouteProbeKEKDisk, nil)
	if err != nil {
		t.Fatalf("server did not survive the panic: %v", err)
	}
	if string(reply) != "alive" {
		t.Fatalf("reply = %q, want alive", reply)
	}
}
