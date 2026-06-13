package transport

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// Native gossip routes (Phase 8 N7-3 — the Send/inbox shape).
//
//	POST <path>: payload = raw body; the route ENQUEUES (from, payload) to a
//	per-route buffered channel and replies 200.
//
// Tunnel-faithful: today the tunnel enqueues to the inbox and replies OK
// (http_data_plane.go handleRPC's inbox tail), and the actual decode/store
// work happens LATER on the GossipReceiver.Run goroutine. Running the consumer
// callback synchronously before the 200 would change sender latency and the
// failure surface — so the primitive owns a small per-route channel + drain
// goroutine (started at registration, stopped at transport Close via t.ctx),
// and the consumer callback runs on that drain goroutine, exactly where the
// Receive() loop ran it.
const (
	RouteGossipAdmin   = "/gossip/admin"
	RouteGossipReceipt = "/gossip/receipt"
)

// gossipInboxCap mirrors the tunnel inbox capacity (the inbox make() in
// NewHTTPTransport: make(chan *ReceivedMessage, 256)).
const gossipInboxCap = 256

// GossipHandler is a consumer-registered callback for one gossip family. It
// runs on the route's drain goroutine; from is the sender's remote address
// (what ReceivedMessage.From carried).
type GossipHandler func(from string, payload []byte)

type gossipDelivery struct {
	from    string
	payload []byte
}

// gossipRouteState is the per-route runtime state, built once at transport
// construction.
type gossipRouteState struct {
	st      StreamType
	handler atomic.Pointer[GossipHandler]
	ch      chan gossipDelivery
	drain   sync.Once
}

var gossipRouteTable = []struct {
	path string
	st   StreamType
}{
	{RouteGossipAdmin, StreamAdmin},
	{RouteGossipReceipt, StreamReceipt},
}

func newGossipRouteStates() map[string]*gossipRouteState {
	m := make(map[string]*gossipRouteState, len(gossipRouteTable))
	for _, r := range gossipRouteTable {
		m[r.path] = &gossipRouteState{st: r.st, ch: make(chan gossipDelivery, gossipInboxCap)}
	}
	return m
}

// RegisterGossipRoute installs h for a route DECLARED in gossipRouteTable and
// starts the route's drain goroutine (once; it exits when the transport
// closes). An unknown path panics (wiring bug); nil h unregisters (route
// reverts to 503, enqueued-but-undrained deliveries are dropped by the drain
// goroutine's nil-handler check).
func (t *HTTPTransport) RegisterGossipRoute(path string, h GossipHandler) {
	rs, ok := t.gossipByPath[path]
	if !ok {
		panic(fmt.Sprintf("transport: RegisterGossipRoute: path %q not declared in gossipRouteTable", path))
	}
	if h == nil {
		rs.handler.Store(nil)
		return
	}
	rs.handler.Store(&h)
	rs.drain.Do(func() { go t.drainGossipRoute(rs) })
}

// drainGossipRoute delivers enqueued gossip to the consumer callback — the
// native replacement for the GossipReceiver.Run inbox loop. Stops when the
// transport closes (t.cancel → t.ctx.Done).
func (t *HTTPTransport) drainGossipRoute(rs *gossipRouteState) {
	for {
		select {
		case d := <-rs.ch:
			if hp := rs.handler.Load(); hp != nil {
				(*hp)(d.from, d.payload)
			}
		case <-t.ctx.Done():
			return
		}
	}
}

// handleGossipRoute returns the Hertz handler for one gossip route:
// enqueue-then-200. A full channel blocks (the tunnel inbox select blocked on
// inbox<- too), aborted only by transport shutdown — in which case the 200
// still answers, matching the tunnel's unconditional OK reply.
func (t *HTTPTransport) handleGossipRoute(rs *gossipRouteState) app.HandlerFunc {
	return func(c context.Context, ctx *app.RequestContext) {
		if rs.handler.Load() == nil {
			ctx.SetStatusCode(consts.StatusServiceUnavailable)
			ctx.SetBodyString("gossip route handler not ready")
			return
		}
		payload, perr := readBufferedPayload(ctx)
		if perr != nil {
			ctx.SetStatusCode(consts.StatusBadRequest)
			ctx.SetBodyString(perr.Error())
			return
		}

		// Inbound admission AFTER reading the bounded payload (acquireAdmission),
		// so a slow-body peer cannot hold a traffic slot during the read.
		release, aerr := t.acquireAdmission(c, rs.st)
		if aerr != nil {
			ctx.SetStatusCode(consts.StatusServiceUnavailable)
			ctx.SetBodyString("overloaded: " + aerr.Error())
			return
		}
		defer release()

		// payload is an io.ReadAll allocation (not Hertz pooled memory), so the
		// drain goroutine may retain it past this handler's return.
		select {
		case rs.ch <- gossipDelivery{from: ctx.RemoteAddr().String(), payload: payload}:
		case <-t.ctx.Done():
		}
		ctx.SetStatusCode(consts.StatusOK)
	}
}

// GossipSend POSTs payload to a gossip route on addr. A non-200 answer
// surfaces as an error (senders log and move on, exactly how tunnel Send
// errors are treated today).
func (t *HTTPTransport) GossipSend(ctx context.Context, addr, path string, payload []byte) error {
	c, err := t.httpClient()
	if err != nil {
		return err
	}
	hreq := protocol.AcquireRequest()
	hresp := protocol.AcquireResponse()
	defer protocol.ReleaseRequest(hreq)
	defer protocol.ReleaseResponse(hresp)
	hreq.SetMethod(consts.MethodPost)
	hreq.SetRequestURI("https://" + addr + path)
	if len(payload) > 0 {
		hreq.SetBody(payload)
	}

	if err := c.Do(ctx, hreq, hresp); err != nil {
		return fmt.Errorf("gossip send %s%s: %w", addr, path, err)
	}
	if sc := hresp.StatusCode(); sc != consts.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(hresp.BodyStream(), bufferedErrCap))
		return fmt.Errorf("gossip send %s%s: status %d: %s", addr, path, sc, msg)
	}
	// Drain the (empty) success body so the pooled conn is clean for reuse.
	_, _ = io.Copy(io.Discard, io.LimitReader(hresp.BodyStream(), bufferedErrCap))
	return nil
}
