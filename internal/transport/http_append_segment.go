package transport

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// Native append-segment read route (Phase 8 N7-3 — the last streaming-read
// family).
//
//	GET /append-segment/read — non-owner → owner append-segment blob fetch
//
// The family's request metadata is a length-prefixed binary frame
// ([groupID][bucket][key][blobID][kind], cluster/append_segment_transport.go)
// — not flat scalars — so it rides a NAMED family header (base64, capped),
// the N7-2 convention for framed families. The reply metadata (a status byte,
// optionally followed by error text) rides the response header; the segment
// bytes stream as the body.
//
// HTTP status is TRANSPORT-ONLY: 400 bad/missing frame, 503 not-ready/
// overloaded, 500 handler failure. 200 carries EVERY application outcome —
// OK/ENOENT/ERROR are in-band in the reply frame (the client iterates peers
// on ENOENT), exactly as the tunnel delivered them.
const (
	httpAppendSegmentReadPath = "/append-segment/read"

	hdrAppendFrame = "X-Grainfs-Append-Frame" // request: base64 binary request frame
	hdrAppendReply = "X-Grainfs-Append-Reply" // response: base64 status frame (metadata; body = segment stream)
)

// maxAppendFrameBytes bounds the family frame (request and reply). Frames
// carry four ≤4 KiB fields + a kind byte, so KBs in practice; the cap mirrors
// maxForwardFrameBytes (the real wire bound is Hertz's ~1 MiB header limit —
// the client-side guard is the protection that produces a clear error, this
// server-side cap is defense-in-depth).
const maxAppendFrameBytes = 256 << 10

// AppendSegmentReadHandler is the consumer-registered native handler. reply is
// the family's status frame (always produced — ENOENT/ERROR are in-band);
// rbody (nil for in-band non-OK replies) streams as the response body. A
// non-nil error maps to transport-level 500.
type AppendSegmentReadHandler func(frame []byte) (reply []byte, rbody io.ReadCloser, err error)

// RegisterAppendSegmentReadHandler installs the native append-segment read
// handler. Same contract as RegisterForwardReadHandler: consumer-registered
// post-Listen, nil unregisters (route reverts to 503).
func (t *HTTPTransport) RegisterAppendSegmentReadHandler(h AppendSegmentReadHandler) {
	if h == nil {
		t.appendSegReadHandler.Store(nil)
		return
	}
	t.appendSegReadHandler.Store(&h)
}

// InboundNativeAppendSegmentReads counts native-route dispatches (positive
// dispatch signal for tests/observability).
func (t *HTTPTransport) InboundNativeAppendSegmentReads() uint64 {
	return t.nativeAppendSegReads.Load()
}

// handleAppendSegmentRead is the Hertz handler for GET /append-segment/read.
// Mirrors handleForwardRead.
func (t *HTTPTransport) handleAppendSegmentRead(c context.Context, ctx *app.RequestContext) {
	hp := t.appendSegReadHandler.Load()
	if hp == nil {
		ctx.SetStatusCode(consts.StatusServiceUnavailable)
		ctx.SetBodyString("append-segment read handler not ready")
		return
	}
	s := string(ctx.GetHeader(hdrAppendFrame))
	if s == "" {
		ctx.SetStatusCode(consts.StatusBadRequest)
		ctx.SetBodyString("missing " + hdrAppendFrame)
		return
	}
	frame, err := base64.StdEncoding.DecodeString(s)
	if err != nil || len(frame) == 0 || len(frame) > maxAppendFrameBytes {
		ctx.SetStatusCode(consts.StatusBadRequest)
		ctx.SetBodyString("bad " + hdrAppendFrame)
		return
	}

	// Inbound admission released when this handler returns — BEFORE Hertz
	// streams the response body — mirroring handleShardRead/handleForwardRead.
	t.mu.RLock()
	limiter := t.traffic
	t.mu.RUnlock()
	release, aerr := limiter.Acquire(c, StreamReadAppendSegment)
	if aerr != nil {
		ctx.SetStatusCode(consts.StatusServiceUnavailable)
		ctx.SetBodyString("overloaded: " + aerr.Error())
		return
	}
	defer release()

	t.nativeAppendSegReads.Add(1)
	reply, rbody, herr := (*hp)(frame)
	if herr != nil {
		ctx.SetStatusCode(consts.StatusInternalServerError)
		ctx.SetBodyString(herr.Error())
		return
	}
	ctx.SetStatusCode(consts.StatusOK)
	if len(reply) > 0 {
		ctx.Header(hdrAppendReply, base64.StdEncoding.EncodeToString(reply))
	}
	if rbody != nil {
		ctx.SetBodyStream(rbody, -1) // Hertz closes the io.Closer after writing
	}
}

// AppendSegmentRead fetches one append-segment blob from addr. On success the
// returned reply is the family's status frame and the ReadCloser streams the
// segment bytes (empty for in-band non-OK replies); the closer OWNS the pooled
// response (Close exactly once). Mirrors ForwardRead.
func (t *HTTPTransport) AppendSegmentRead(ctx context.Context, addr string, frame []byte) ([]byte, io.ReadCloser, error) {
	if len(frame) == 0 || len(frame) > maxAppendFrameBytes {
		return nil, nil, fmt.Errorf("append-segment read: frame size %d outside (0, %d]", len(frame), maxAppendFrameBytes)
	}
	c, err := t.httpClient()
	if err != nil {
		return nil, nil, err
	}
	hreq := protocol.AcquireRequest()
	hresp := protocol.AcquireResponse()
	hreq.SetMethod(consts.MethodGet)
	hreq.SetRequestURI("https://" + addr + httpAppendSegmentReadPath)
	hreq.Header.Set(hdrAppendFrame, base64.StdEncoding.EncodeToString(frame))

	if err := c.Do(ctx, hreq, hresp); err != nil {
		protocol.ReleaseRequest(hreq)
		protocol.ReleaseResponse(hresp)
		return nil, nil, fmt.Errorf("append-segment read %s: %w", addr, err)
	}
	protocol.ReleaseRequest(hreq)

	if sc := hresp.StatusCode(); sc != consts.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(hresp.BodyStream(), forwardErrCap))
		protocol.ReleaseResponse(hresp)
		return nil, nil, fmt.Errorf("append-segment read %s: status %d: %s", addr, sc, msg)
	}
	var reply []byte
	if s := hresp.Header.Get(hdrAppendReply); s != "" {
		reply, err = base64.StdEncoding.DecodeString(s)
		if err != nil || len(reply) > maxAppendFrameBytes {
			protocol.ReleaseResponse(hresp)
			return nil, nil, fmt.Errorf("append-segment read %s: bad reply header", addr)
		}
	}
	// Success: hresp ownership transfers to the closer (N7-1 lifecycle rule).
	return reply, newHTTPRespBody(hresp), nil
}
