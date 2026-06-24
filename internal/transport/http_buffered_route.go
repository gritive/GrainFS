package transport

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// Native buffered-Call routes (Phase 8 N7-3).
//
//	POST <path>: request payload = raw body (the family's own FB/binary frame),
//	reply = raw response body, 200.
//
// These families carry application status IN-BAND in their replies, mirroring
// the tunnel where every handler returned a StatusOK Message whose payload
// encoded the outcome. A handler error maps to 500 + text — the exact
// consumer-visible semantic the tunnel's StatusError → checkResponseStatus →
// Go-error path provided. 503 = handler not ready / overloaded. There is no
// envelope on the wire (no StreamType, no Message.ID, no transport-status
// header); the StreamType in the route table below is an INTERNAL
// admission/metrics key only (TrafficLimiter classes).
//
// Route path constants — one per buffered-Call family. The table keeps the
// taxonomy centralized in transport; handlers are consumer-registered per path
// from the same domain sites that register tunnel handlers today.
const (
	RouteRaftDataRPC             = "/raft/data/rpc"
	RouteRaftMetaRPC             = "/raft/meta/rpc"
	RouteRaftGroupRPC            = "/raft/group/rpc"
	RouteShardRPC                = "/shard/rpc"
	RouteForwardProposeLegacy    = "/forward/propose/legacy"
	RouteForwardProposeGroup     = "/forward/propose/group"
	RouteForwardProposeDataGroup = "/forward/propose/data-group"
	RouteForwardReadIndex        = "/forward/read-index"
	RouteRaftMetaPropose         = "/raft/meta/propose"
	RouteForwardMetaReadIndex    = "/raft/meta/read-index"
	RouteReceiptQuery            = "/receipt/query"
	RouteProbeCapability         = "/probe/capability"
	RouteProbeKEKDisk            = "/probe/kek-disk"
	RouteProbeKEKLease           = "/probe/kek-lease"
	RouteProbeAppliedIndex       = "/probe/applied-index"
	RouteAuditShip               = "/audit/ship"
)

// bufferedErrCap bounds how much of a non-200 response body the client reads
// as the remote error text (mirrors forwardErrCap).
const bufferedErrCap = 4 << 10

// bufferedRouteTable declares every buffered-Call route with its admission
// StreamType. It drives Listen registration: ALL declared routes are live on
// every node from Listen, and a route whose family handler has not (yet)
// registered answers 503 — so a client that migrates ahead of its server peer
// sees a clean not-ready error, never a 404.
var bufferedRouteTable = []struct {
	path string
	st   StreamType
}{
	{RouteRaftDataRPC, StreamControl},
	{RouteRaftMetaRPC, StreamMetaRaft},
	{RouteRaftGroupRPC, StreamGroupRaft},
	{RouteShardRPC, StreamData},
	{RouteForwardProposeLegacy, StreamProposeForward},
	{RouteForwardProposeGroup, StreamProposeGroupForward},
	{RouteForwardProposeDataGroup, StreamDataGroupProposeForward},
	{RouteForwardReadIndex, StreamReadIndex},
	{RouteRaftMetaPropose, StreamMetaProposeForward},
	{RouteForwardMetaReadIndex, StreamMetaReadIndex},
	{RouteReceiptQuery, StreamReceiptQuery},
	{RouteProbeCapability, StreamCapabilityProbe},
	{RouteProbeKEKDisk, StreamKEKDiskSpaceProbe},
	{RouteProbeKEKLease, StreamKEKLeaseSnapshotProbe},
	{RouteProbeAppliedIndex, StreamAppliedIndexProbe},
	{RouteAuditShip, StreamAuditShip},
}

// BufferedRouteHandler is a consumer-registered native handler for a
// buffered-Call family. payload may be empty (probe families send empty
// requests); reply may be empty; a non-nil error maps to 500 + text.
type BufferedRouteHandler func(payload []byte) (reply []byte, err error)

// bufferedRouteState is the per-route runtime state, built once at transport
// construction (the map itself is immutable afterwards; handler/served are
// atomic so registration may follow Listen — boot ordering).
type bufferedRouteState struct {
	st      StreamType
	handler atomic.Pointer[BufferedRouteHandler]
	served  atomic.Uint64
}

func newBufferedRouteStates() map[string]*bufferedRouteState {
	m := make(map[string]*bufferedRouteState, len(bufferedRouteTable))
	for _, r := range bufferedRouteTable {
		m[r.path] = &bufferedRouteState{st: r.st}
	}
	return m
}

// RegisterBufferedRoute installs h for a route DECLARED in bufferedRouteTable.
// An unknown path panics (wiring bug, not a runtime condition); nil h
// unregisters (route reverts to 503).
func (t *HTTPTransport) RegisterBufferedRoute(path string, h BufferedRouteHandler) {
	rs, ok := t.bufferedByPath[path]
	if !ok {
		panic(fmt.Sprintf("transport: RegisterBufferedRoute: path %q not declared in bufferedRouteTable", path))
	}
	if h == nil {
		rs.handler.Store(nil)
		return
	}
	rs.handler.Store(&h)
}

// InboundNativeBuffered returns the number of native buffered-route dispatches
// served for path (positive dispatch signal for tests/observability). Unknown
// paths report 0.
func (t *HTTPTransport) InboundNativeBuffered(path string) uint64 {
	if rs, ok := t.bufferedByPath[path]; ok {
		return rs.served.Load()
	}
	return 0
}

// readBufferedPayload reads the request payload from the body, capped at
// maxPayloadSize (the codec's allocation guard). Local to the native
// primitives — deliberately NOT the tunnel's readReqBodyPayload, which N8
// deletes with the rest of the envelope plane.
func readBufferedPayload(ctx *app.RequestContext) ([]byte, error) {
	p, err := io.ReadAll(io.LimitReader(ctx.RequestBodyStream(), maxPayloadSize+1))
	if err != nil {
		return nil, fmt.Errorf("read request payload: %w", err)
	}
	if len(p) > maxPayloadSize {
		return nil, fmt.Errorf("request payload exceeds max %d", maxPayloadSize)
	}
	return p, nil
}

// handleBufferedRoute returns the Hertz handler for one buffered-Call route.
func (t *HTTPTransport) handleBufferedRoute(rs *bufferedRouteState) app.HandlerFunc {
	return func(c context.Context, ctx *app.RequestContext) {
		hp := rs.handler.Load()
		if hp == nil {
			ctx.SetStatusCode(consts.StatusServiceUnavailable)
			ctx.SetBodyString("buffered route handler not ready")
			return
		}
		payload, perr := readBufferedPayload(ctx)
		if perr != nil {
			ctx.SetStatusCode(consts.StatusBadRequest)
			ctx.SetBodyString(perr.Error())
			return
		}

		// Inbound admission AFTER reading the (bounded) payload, so a slow-body
		// peer cannot hold a traffic slot while we read its request — unlike the
		// streaming routes (which acquire before the large body and use
		// admissionMiddleware), buffered payloads are read up front here.
		release, aerr := t.acquireAdmission(c, rs.st)
		if aerr != nil {
			ctx.SetStatusCode(consts.StatusServiceUnavailable)
			ctx.SetBodyString("overloaded: " + aerr.Error())
			return
		}
		defer release()

		rs.served.Add(1)
		reply, herr := (*hp)(payload)
		if herr != nil {
			ctx.SetStatusCode(consts.StatusInternalServerError)
			ctx.SetBodyString(herr.Error())
			return
		}
		ctx.SetStatusCode(consts.StatusOK)
		if len(reply) > 0 {
			ctx.Response.SetBody(reply)
		}
	}
}

// CallBuffered POSTs payload to path on addr and returns the reply body.
// payload is a rewindable []byte body (SetBody, not SetBodyStream) →
// retryable on stale pooled conns (httpRetryIf), same as tunnel Call/
// CallPooled. A non-200 answer surfaces as an error carrying the capped
// response text (500 = handler error, 503 = not ready/overloaded).
func (t *HTTPTransport) CallBuffered(ctx context.Context, addr, path string, payload []byte) ([]byte, error) {
	c, err := t.httpClient()
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("call %s%s: %w", addr, path, err)
	}
	if sc := hresp.StatusCode(); sc != consts.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(hresp.BodyStream(), bufferedErrCap))
		return nil, fmt.Errorf("call %s%s: status %d: %s", addr, path, sc, msg)
	}
	// Cap the reply like the tunnel codec: WithResponseBodyStream bypasses
	// Hertz's MaxResponseBodySize, so a buggy peer could otherwise stream an
	// unbounded body into this io.ReadAll.
	reply, err := io.ReadAll(io.LimitReader(hresp.BodyStream(), maxPayloadSize+1))
	if err != nil {
		return nil, fmt.Errorf("call %s%s: read reply: %w", addr, path, err)
	}
	if len(reply) > maxPayloadSize {
		return nil, fmt.Errorf("call %s%s: reply exceeds max %d", addr, path, maxPayloadSize)
	}
	return reply, nil
}
