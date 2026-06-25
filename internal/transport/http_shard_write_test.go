package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// shardWritePair wires a server transport with a recording native handler and a
// client transport (same PSK).
func shardWritePair(t *testing.T) (srv, cli *HTTPTransport, addr string) {
	t.Helper()
	srv, cli, addr = httpPair(t) // reuses the data-plane test helper
	return srv, cli, addr
}

type recordedWrite struct {
	req  ShardWriteRequest
	body []byte
}

func TestShardWrite_RoundTripPlain(t *testing.T) {
	srv, cli, addr := shardWritePair(t)
	got := make(chan recordedWrite, 1)
	srv.RegisterShardWriteHandler(func(req ShardWriteRequest, body io.Reader) error {
		b, err := io.ReadAll(body)
		if err != nil {
			return err
		}
		got <- recordedWrite{req: req, body: b}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := ShardWriteRequest{Bucket: "bkt", Key: "obj/key", ShardIdx: 3, Sealed: false}
	if err := cli.ShardWrite(ctx, addr, req, bytes.NewReader([]byte("shard-bytes"))); err != nil {
		t.Fatalf("ShardWrite: %v", err)
	}
	select {
	case w := <-got:
		if w.req != req {
			t.Fatalf("handler req = %+v, want %+v", w.req, req)
		}
		if string(w.body) != "shard-bytes" {
			t.Fatalf("handler body = %q, want shard-bytes", w.body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler not invoked")
	}
	if n := srv.InboundNativeShardWrites(); n != 1 {
		t.Fatalf("InboundNativeShardWrites = %d, want 1", n)
	}
}

func TestShardWrite_SealedFlagAndKeyEscaping(t *testing.T) {
	srv, cli, addr := shardWritePair(t)
	got := make(chan ShardWriteRequest, 1)
	srv.RegisterShardWriteHandler(func(req ShardWriteRequest, body io.Reader) error {
		_, _ = io.Copy(io.Discard, body)
		got <- req
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// S3 keys are arbitrary: slashes, spaces, '&', '=', '%', unicode, '?'.
	req := ShardWriteRequest{Bucket: "b-1", Key: "dir/제목 with space&x=1?q%2F", ShardIdx: 11, Sealed: true}
	if err := cli.ShardWrite(ctx, addr, req, bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("ShardWrite: %v", err)
	}
	select {
	case w := <-got:
		if w != req {
			t.Fatalf("handler req = %+v, want %+v (query escaping must round-trip)", w, req)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler not invoked")
	}
}

// TestShardWrite_StagingKeyRoundTrips locks the PR1 segment-staging wire: the
// optional StagingKey (staging physical path) must round-trip via the `staging`
// query param while Key stays the final logical key. A receiver that did not read
// the param would see StagingKey="" and write to the final path, silently
// bypassing staging — exactly what this guards.
func TestShardWrite_StagingKeyRoundTrips(t *testing.T) {
	srv, cli, addr := shardWritePair(t)
	got := make(chan ShardWriteRequest, 1)
	srv.RegisterShardWriteHandler(func(req ShardWriteRequest, body io.Reader) error {
		_, _ = io.Copy(io.Discard, body)
		got <- req
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := ShardWriteRequest{
		Bucket:     "bkt",
		Key:        "obj/segments/blob-1", // final logical key (AAD)
		StagingKey: ".segstaging/txn-7/blob-1",
		ShardIdx:   2,
		Sealed:     false,
	}
	if err := cli.ShardWrite(ctx, addr, req, bytes.NewReader([]byte("staged-shard-bytes"))); err != nil {
		t.Fatalf("ShardWrite: %v", err)
	}
	select {
	case w := <-got:
		if w != req {
			t.Fatalf("handler req = %+v, want %+v (StagingKey must round-trip)", w, req)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler not invoked")
	}
}

// TestShardWrite_NoStagingKeyOmitsParam confirms the legacy path is unchanged: an
// empty StagingKey round-trips as empty (the client omits the `staging` param).
func TestShardWrite_NoStagingKeyOmitsParam(t *testing.T) {
	srv, cli, addr := shardWritePair(t)
	got := make(chan ShardWriteRequest, 1)
	srv.RegisterShardWriteHandler(func(req ShardWriteRequest, body io.Reader) error {
		_, _ = io.Copy(io.Discard, body)
		got <- req
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := ShardWriteRequest{Bucket: "bkt", Key: "obj/key", ShardIdx: 1, Sealed: false}
	if err := cli.ShardWrite(ctx, addr, req, bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("ShardWrite: %v", err)
	}
	select {
	case w := <-got:
		if w.StagingKey != "" {
			t.Fatalf("StagingKey = %q, want empty for legacy write", w.StagingKey)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler not invoked")
	}
}

func TestShardWrite_HandlerErrorSurfacesText(t *testing.T) {
	srv, cli, addr := shardWritePair(t)
	srv.RegisterShardWriteHandler(func(req ShardWriteRequest, body io.Reader) error {
		_, _ = io.Copy(io.Discard, body)
		return errors.New("sealed shard truncated: received 5, declared 9")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := cli.ShardWrite(ctx, addr, ShardWriteRequest{Bucket: "b", Key: "k"}, bytes.NewReader([]byte("y")))
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "sealed shard truncated") {
		t.Fatalf("error %q must carry the remote handler text", err)
	}
}

func TestShardWrite_NotRegistered503(t *testing.T) {
	_, cli, addr := shardWritePair(t) // server never registers a handler
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := cli.ShardWrite(ctx, addr, ShardWriteRequest{Bucket: "b", Key: "k"}, bytes.NewReader(nil))
	if err == nil {
		t.Fatal("want error when no handler registered, got nil")
	}
	if !strings.Contains(err.Error(), "503") && !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("error %q should reflect 503/not-ready", err)
	}
}

func TestShardWrite_AdmissionBlocksWhenClassExhausted(t *testing.T) {
	// Built by hand (not httpPair) so the client idle read bound is armed BEFORE
	// the lazy Hertz client build (mirrors http_idle_deadline_test.go): the Hertz
	// client only checks ctx between retry attempts, so the blocked second
	// request below surfaces as an idle-read timeout, not a ctx error.
	srv := MustNewHTTPTransport("dp-psk")
	cli := MustNewHTTPTransport("dp-psk")
	cli.clientBodyTimeout = 500 * time.Millisecond
	t.Cleanup(func() { srv.Close(); cli.Close() })
	addr := listenHTTP(t, srv)
	if err := pingReady(t, cli, addr); err != nil {
		t.Fatalf("server not ready: %v", err)
	}

	block := make(chan struct{})
	var entered atomic.Bool
	srv.RegisterShardWriteHandler(func(req ShardWriteRequest, body io.Reader) error {
		entered.Store(true)
		<-block // hold the admission slot until the test releases it
		_, _ = io.Copy(io.Discard, body)
		return nil
	})
	// One bulk-class slot: the first in-flight write occupies it.
	srv.SetTrafficLimits(TrafficLimits{Bulk: 1, Data: 1, Meta: 1, Control: 1})

	// First request: body stays open (io.Pipe) so the handler — and its
	// admission slot — stays held until we close the pipe.
	pr, pw := io.Pipe()
	firstDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		firstDone <- cli.ShardWrite(ctx, addr, ShardWriteRequest{Bucket: "b", Key: "k1"}, pr)
	}()
	// Prime the stream: Hertz buffers the request HEADERS until the first body
	// chunk flushes (WriteBodyChunked flushes per chunk), so an empty pipe would
	// keep the request invisible to the server and the handler would never enter.
	if _, err := pw.Write([]byte("first-chunk")); err != nil {
		t.Fatalf("prime pipe: %v", err)
	}
	for !entered.Load() {
		time.Sleep(5 * time.Millisecond)
	}

	// Second request: TrafficLimiter.Acquire BLOCKS server-side until a slot
	// frees (transport_shared.go), and the Hertz client does not abort a request
	// mid-flight on ctx — so the observed failure mode is the client-side idle
	// read timeout (clientBodyTimeout, armed above), not a fast 503. (The 503
	// branch fires only when Acquire itself errors.) Either way the consumer
	// sees an error.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := cli.ShardWrite(ctx, addr, ShardWriteRequest{Bucket: "b", Key: "k2"}, bytes.NewReader([]byte("z")))
	if err == nil {
		t.Fatal("want admission-blocked error (idle-read timeout or 503), got nil")
	}

	// Release the first request cleanly: unblock the handler, then EOF the body.
	close(block)
	_ = pw.Close()
	if ferr := <-firstDone; ferr != nil {
		t.Fatalf("first write should complete after release: %v", ferr)
	}
}
