package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func TestShardRead_WholeShardRoundTrip(t *testing.T) {
	srv, cli, addr := httpPair(t)
	const body = "whole-shard-bytes"
	got := make(chan ShardReadRequest, 1)
	srv.RegisterShardReadHandler(func(req ShardReadRequest) (io.ReadCloser, error) {
		got <- req
		return io.NopCloser(strings.NewReader(body)), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := ShardReadRequest{Bucket: "bkt", Key: "dir/제목 with space&x=1?q%2F", ShardIdx: 3}
	rc, err := cli.ShardRead(ctx, addr, req)
	if err != nil {
		t.Fatalf("ShardRead: %v", err)
	}
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if cerr := rc.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
	if string(b) != body {
		t.Fatalf("body = %q, want %q", b, body)
	}
	select {
	case w := <-got:
		if w != req {
			t.Fatalf("handler req = %+v, want %+v (escaping must round-trip; Range must be false)", w, req)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not invoked")
	}
	if n := srv.InboundNativeShardReads(); n != 1 {
		t.Fatalf("InboundNativeShardReads = %d, want 1", n)
	}
}

func TestShardRead_RangeParamsRoundTrip(t *testing.T) {
	srv, cli, addr := httpPair(t)
	got := make(chan ShardReadRequest, 1)
	srv.RegisterShardReadHandler(func(req ShardReadRequest) (io.ReadCloser, error) {
		got <- req
		return io.NopCloser(bytes.NewReader(make([]byte, 16))), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := ShardReadRequest{Bucket: "b", Key: "k", ShardIdx: 0, Range: true, Offset: 4096, Length: 16}
	rc, err := cli.ShardRead(ctx, addr, req)
	if err != nil {
		t.Fatalf("ShardRead range: %v", err)
	}
	_, _ = io.Copy(io.Discard, rc)
	_ = rc.Close()
	select {
	case w := <-got:
		if w != req {
			t.Fatalf("handler req = %+v, want %+v", w, req)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not invoked")
	}
}

func TestShardRead_HandlerErrorSurfacesText(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterShardReadHandler(func(req ShardReadRequest) (io.ReadCloser, error) {
		return nil, errors.New("object not found: bkt/k shard 0")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.ShardRead(ctx, addr, ShardReadRequest{Bucket: "bkt", Key: "k"})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "object not found") {
		t.Fatalf("error %q must carry the remote handler text", err)
	}
}

func TestShardRead_NotRegistered503(t *testing.T) {
	_, cli, addr := httpPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.ShardRead(ctx, addr, ShardReadRequest{Bucket: "b", Key: "k"})
	if err == nil {
		t.Fatal("want error when no handler registered, got nil")
	}
	if !strings.Contains(err.Error(), "503") && !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("error %q should reflect 503/not-ready", err)
	}
}

func TestShardRead_HalfRangeParamsRejected400(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterShardReadHandler(func(req ShardReadRequest) (io.ReadCloser, error) {
		t.Error("handler must not be invoked for malformed params")
		return io.NopCloser(strings.NewReader("")), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Drive the malformed wire directly (offset without length) — the typed
	// client cannot produce it, which is the point of the typed surface.
	c, err := cli.httpClient()
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	status, body, err := c.Get(ctx, nil, "https://"+addr+httpShardReadPath+"?bucket=b&key=k&idx=0&offset=5")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if status != 400 {
		t.Fatalf("status = %d (%s), want 400", status, body)
	}
}

// TestShardRead_PartialCloseReleasesConn: closing the streamed body early must
// not wedge the client (pooled-conn hygiene — mirrors TestHTTPDataPlane_CallReadPartialClose).
func TestShardRead_PartialCloseReleasesConn(t *testing.T) {
	srv, cli, addr := httpPair(t)
	srv.RegisterShardReadHandler(func(req ShardReadRequest) (io.ReadCloser, error) {
		return io.NopCloser(&limitedZeroReader{remaining: 4 << 20}), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		rc, err := cli.ShardRead(ctx, addr, ShardReadRequest{Bucket: "b", Key: "k", ShardIdx: i})
		if err != nil {
			t.Fatalf("ShardRead %d: %v", i, err)
		}
		buf := make([]byte, 1024)
		if _, err := io.ReadFull(rc, buf); err != nil {
			t.Fatalf("partial read %d: %v", i, err)
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("partial close %d: %v", i, err)
		}
	}
}

func TestShardRead_AdmissionHeldUntilBodyClose(t *testing.T) {
	tr := &HTTPTransport{}
	tr.SetTrafficLimits(TrafficLimits{Bulk: 1})
	tr.RegisterShardReadHandler(func(req ShardReadRequest) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("body")), nil
	})

	rc := app.NewContext(0)
	rc.Request.SetRequestURI("https://peer" + httpShardReadPath + "?bucket=b&key=k&idx=0")
	tr.handleShardRead(context.Background(), rc)
	if rc.Response.StatusCode() != consts.StatusOK {
		t.Fatalf("status = %d, want 200", rc.Response.StatusCode())
	}

	blockedCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	release, err := tr.traffic.Acquire(blockedCtx, StreamShardReadBody)
	if err == nil {
		release()
		_ = rc.Response.CloseBodyStream()
		t.Fatal("bulk slot was released when the handler returned; want it held until response body close")
	}

	if err := rc.Response.CloseBodyStream(); err != nil {
		t.Fatalf("close response body: %v", err)
	}
	release, err = tr.traffic.Acquire(context.Background(), StreamShardReadBody)
	if err != nil {
		t.Fatalf("bulk slot was not released after response body close: %v", err)
	}
	release()
}
