package transport

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestGossipRoute_DeliversFromAndPayload(t *testing.T) {
	srv, cli, addr := httpPair(t)
	type got struct {
		from    string
		payload []byte
	}
	gotCh := make(chan got, 1)
	srv.RegisterGossipRoute(RouteGossipAdmin, func(from string, payload []byte) {
		gotCh <- got{from: from, payload: payload}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload := []byte("gossip-\x00binary\xffstats")
	if err := cli.GossipSend(ctx, addr, RouteGossipAdmin, payload); err != nil {
		t.Fatalf("GossipSend: %v", err)
	}
	select {
	case g := <-gotCh:
		if !bytes.Equal(g.payload, payload) {
			t.Fatalf("payload = %q, want %q", g.payload, payload)
		}
		if g.from == "" {
			t.Fatal("from is empty, want sender remote addr")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gossip callback not invoked")
	}
}

// TestGossipRoute_SequentialDeliveries: the drain goroutine survives across
// messages — every enqueue-then-200 delivery reaches the callback in order.
func TestGossipRoute_SequentialDeliveries(t *testing.T) {
	srv, cli, addr := httpPair(t)
	gotCh := make(chan string, 8)
	srv.RegisterGossipRoute(RouteGossipReceipt, func(from string, payload []byte) {
		gotCh <- string(payload)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, m := range []string{"r1", "r2", "r3"} {
		if err := cli.GossipSend(ctx, addr, RouteGossipReceipt, []byte(m)); err != nil {
			t.Fatalf("GossipSend(%s): %v", m, err)
		}
	}
	for _, want := range []string{"r1", "r2", "r3"} {
		select {
		case g := <-gotCh:
			if g != want {
				t.Fatalf("delivery = %q, want %q", g, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("delivery %q not received", want)
		}
	}
}

// TestGossipRoute_UnregisteredPath503: a gossip POST to a declared-but-
// unregistered route answers 503 — the sender logs and moves on, exactly how
// gossip Send errors are treated today (gossip.go warn + error counter).
func TestGossipRoute_UnregisteredPath503(t *testing.T) {
	_, cli, addr := httpPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := cli.GossipSend(ctx, addr, RouteGossipReceipt, []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "status 503") {
		t.Fatalf("err = %v, want status 503", err)
	}
}

func TestGossipRoute_UnknownPathRegistrationPanics(t *testing.T) {
	srv := MustNewHTTPTransport("dp-psk")
	t.Cleanup(func() { srv.Close() })
	defer func() {
		if recover() == nil {
			t.Fatal("RegisterGossipRoute on undeclared path must panic")
		}
	}()
	srv.RegisterGossipRoute("/not/gossip", func(from string, payload []byte) {})
}
