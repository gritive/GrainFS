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

// Native forward streaming routes (Phase 8 N7-2 — the S3 forward data plane).
//
//	POST /forward/write — streamed-body S3 write forward (PutObjectStream,
//	                      AppendObject, UploadPart, …)
//	GET  /forward/read  — streamed-response S3 read forward (GetObject,
//	                      GetObjectVersion, ReadAt)
//
// The family's metadata is an FB forward frame ([groupID][op][args],
// cluster/forward_codec.go) — not flat scalars — so it rides a NAMED family
// header (base64, capped), exactly where the tunnel carried it. This is the
// convention for FB-framed families: a family-specific header carrying the
// family's own application payload, NOT a revival of the generic X-Gfs-*
// envelope (no StreamType, no Message.ID, no transport-status header).
//
// HTTP status is TRANSPORT-ONLY on these routes: 400 bad/missing frame, 503
// not-ready/overloaded, 500 handler failure. 200 carries EVERY application
// outcome — ForwardReceiver encodes NotVoter/NotLeader(+hint)/NoSuchBucket/OK
// inside the FB ForwardReply (errReply), and ForwardSender's hint-redirect
// retry loop reads that reply. Mapping those to HTTP codes would break the
// redirect protocol; they are application payload, not transport status.
const (
	httpForwardWritePath = "/forward/write"
	httpForwardReadPath  = "/forward/read"

	hdrForwardFrame = "X-Grainfs-Forward-Frame" // request: base64 FB forward frame
	hdrForwardReply = "X-Grainfs-Forward-Reply" // GET response: base64 FB ForwardReply (metadata; body = object stream)
)

// forwardErrCap bounds how much of a 4xx/5xx response body the client reads as
// the remote error text.
const forwardErrCap = 4 << 10

// maxForwardFrameBytes bounds the family frame. Stream-path frames carry
// groupID+op+bucket/key/metadata only (body bytes stream separately), so KBs in
// practice. The REAL wire bound is Hertz's ~1 MiB default request-header limit
// (an oversized header dies in Hertz parsing before any handler runs) — the
// client-side guard in ForwardWrite/ForwardRead is the protection that produces
// a clear error; this server-side cap is defense-in-depth.
const maxForwardFrameBytes = 256 << 10

// ForwardWriteHandler is the consumer-registered native handler for streamed-
// body forwards. reply is the FB ForwardReply (always produced — application
// errors are in-band); a non-nil error maps to transport-level 500.
type ForwardWriteHandler func(frame []byte, body io.Reader) (reply []byte, err error)

// ForwardReadHandler is the consumer-registered native handler for streamed-
// response forwards. reply is the FB ForwardReply metadata; rbody (may be nil
// for in-band error replies) streams as the response body.
type ForwardReadHandler func(frame []byte) (reply []byte, rbody io.ReadCloser, err error)

// RegisterForwardWriteHandler installs the native forward-write handler. Same
// contract as RegisterShardWriteHandler: consumer-registered post-Listen, nil
// unregisters (route reverts to 503).
func (t *HTTPTransport) RegisterForwardWriteHandler(h ForwardWriteHandler) {
	if h == nil {
		t.forwardWriteHandler.Store(nil)
		return
	}
	t.forwardWriteHandler.Store(&h)
}

// RegisterForwardReadHandler installs the native forward-read handler. Nil
// unregisters (route reverts to 503).
func (t *HTTPTransport) RegisterForwardReadHandler(h ForwardReadHandler) {
	if h == nil {
		t.forwardReadHandler.Store(nil)
		return
	}
	t.forwardReadHandler.Store(&h)
}

// InboundNativeForwardWrites / InboundNativeForwardReads count native-route
// dispatches (positive dispatch signal for tests/observability).
func (t *HTTPTransport) InboundNativeForwardWrites() uint64 { return t.nativeForwardWrites.Load() }
func (t *HTTPTransport) InboundNativeForwardReads() uint64  { return t.nativeForwardReads.Load() }

// decodeForwardFrameHeader extracts and bounds the family frame.
func decodeForwardFrameHeader(ctx *app.RequestContext) ([]byte, bool) {
	s := string(ctx.GetHeader(hdrForwardFrame))
	if s == "" {
		ctx.SetStatusCode(consts.StatusBadRequest)
		ctx.SetBodyString("missing " + hdrForwardFrame)
		return nil, false
	}
	frame, err := base64.StdEncoding.DecodeString(s)
	if err != nil || len(frame) == 0 || len(frame) > maxForwardFrameBytes {
		ctx.SetStatusCode(consts.StatusBadRequest)
		ctx.SetBodyString("bad " + hdrForwardFrame)
		return nil, false
	}
	return frame, true
}

// handleForwardWrite is the Hertz handler for POST /forward/write.
func (t *HTTPTransport) handleForwardWrite(c context.Context, ctx *app.RequestContext) {
	hp := t.forwardWriteHandler.Load()
	if hp == nil {
		ctx.SetStatusCode(consts.StatusServiceUnavailable)
		ctx.SetBodyString("forward write handler not ready")
		return
	}
	frame, ok := decodeForwardFrameHeader(ctx)
	if !ok {
		return
	}

	// Inbound admission: same class the tunnel used (StreamGroupForwardBody →
	// bulk; internal admission/metrics key only — no longer on the wire).
	t.mu.RLock()
	limiter := t.traffic
	t.mu.RUnlock()
	release, aerr := limiter.Acquire(c, StreamGroupForwardBody)
	if aerr != nil {
		ctx.SetStatusCode(consts.StatusServiceUnavailable)
		ctx.SetBodyString("overloaded: " + aerr.Error())
		return
	}
	defer release()

	t.nativeForwardWrites.Add(1)
	reply, herr := (*hp)(frame, ctx.RequestBodyStream())
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

// handleForwardRead is the Hertz handler for GET /forward/read.
func (t *HTTPTransport) handleForwardRead(c context.Context, ctx *app.RequestContext) {
	hp := t.forwardReadHandler.Load()
	if hp == nil {
		ctx.SetStatusCode(consts.StatusServiceUnavailable)
		ctx.SetBodyString("forward read handler not ready")
		return
	}
	frame, ok := decodeForwardFrameHeader(ctx)
	if !ok {
		return
	}

	// Inbound admission released when this handler returns — BEFORE Hertz
	// streams the response body — mirroring handleShardRead.
	t.mu.RLock()
	limiter := t.traffic
	t.mu.RUnlock()
	release, aerr := limiter.Acquire(c, StreamGroupForwardRead)
	if aerr != nil {
		ctx.SetStatusCode(consts.StatusServiceUnavailable)
		ctx.SetBodyString("overloaded: " + aerr.Error())
		return
	}
	defer release()

	t.nativeForwardReads.Add(1)
	reply, rbody, herr := (*hp)(frame)
	if herr != nil {
		ctx.SetStatusCode(consts.StatusInternalServerError)
		ctx.SetBodyString(herr.Error())
		return
	}
	ctx.SetStatusCode(consts.StatusOK)
	if len(reply) > 0 {
		ctx.Header(hdrForwardReply, base64.StdEncoding.EncodeToString(reply))
	}
	if rbody != nil {
		ctx.SetBodyStream(rbody, -1) // Hertz closes the io.Closer after writing
	}
}

// ForwardWrite streams one S3 write forward to addr. reply is the FB
// ForwardReply (application status in-band).
func (t *HTTPTransport) ForwardWrite(ctx context.Context, addr string, frame []byte, body io.Reader) ([]byte, error) {
	if len(frame) == 0 || len(frame) > maxForwardFrameBytes {
		return nil, fmt.Errorf("forward write: frame size %d outside (0, %d]", len(frame), maxForwardFrameBytes)
	}
	c, err := t.httpClient()
	if err != nil {
		return nil, err
	}
	hreq := protocol.AcquireRequest()
	hresp := protocol.AcquireResponse()
	defer protocol.ReleaseRequest(hreq)
	defer protocol.ReleaseResponse(hresp)
	hreq.SetMethod(consts.MethodPost)
	hreq.SetRequestURI("https://" + addr + httpForwardWritePath)
	hreq.Header.Set(hdrForwardFrame, base64.StdEncoding.EncodeToString(frame))
	// Streamed body — un-retryable by design (httpRetryIf refuses IsBodyStream),
	// identical to tunnel CallWithBody. hertzBodyReader loops past (0,nil) reads.
	hreq.SetBodyStream(hertzBodyReader{r: body}, -1)

	if err := c.Do(ctx, hreq, hresp); err != nil {
		return nil, fmt.Errorf("forward write %s: %w", addr, err)
	}
	if sc := hresp.StatusCode(); sc != consts.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(hresp.BodyStream(), forwardErrCap))
		return nil, fmt.Errorf("forward write %s: status %d: %s", addr, sc, msg)
	}
	reply, err := io.ReadAll(io.LimitReader(hresp.BodyStream(), maxPayloadSize+1))
	if err != nil {
		return nil, fmt.Errorf("forward write %s: read reply: %w", addr, err)
	}
	if len(reply) > maxPayloadSize {
		return nil, fmt.Errorf("forward write %s: reply exceeds max %d", addr, maxPayloadSize)
	}
	return reply, nil
}

// ForwardRead requests one S3 read forward from addr. On success the returned
// reply is the FB ForwardReply metadata and the ReadCloser streams the object
// bytes; the closer OWNS the pooled response (Close exactly once).
func (t *HTTPTransport) ForwardRead(ctx context.Context, addr string, frame []byte) ([]byte, io.ReadCloser, error) {
	if len(frame) == 0 || len(frame) > maxForwardFrameBytes {
		return nil, nil, fmt.Errorf("forward read: frame size %d outside (0, %d]", len(frame), maxForwardFrameBytes)
	}
	c, err := t.httpClient()
	if err != nil {
		return nil, nil, err
	}
	hreq := protocol.AcquireRequest()
	hresp := protocol.AcquireResponse()
	hreq.SetMethod(consts.MethodGet)
	hreq.SetRequestURI("https://" + addr + httpForwardReadPath)
	hreq.Header.Set(hdrForwardFrame, base64.StdEncoding.EncodeToString(frame))

	if err := c.Do(ctx, hreq, hresp); err != nil {
		protocol.ReleaseRequest(hreq)
		protocol.ReleaseResponse(hresp)
		return nil, nil, fmt.Errorf("forward read %s: %w", addr, err)
	}
	protocol.ReleaseRequest(hreq)

	if sc := hresp.StatusCode(); sc != consts.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(hresp.BodyStream(), forwardErrCap))
		protocol.ReleaseResponse(hresp)
		return nil, nil, fmt.Errorf("forward read %s: status %d: %s", addr, sc, msg)
	}
	var reply []byte
	if s := hresp.Header.Get(hdrForwardReply); s != "" {
		reply, err = base64.StdEncoding.DecodeString(s)
		if err != nil || len(reply) > maxPayloadSize {
			protocol.ReleaseResponse(hresp)
			return nil, nil, fmt.Errorf("forward read %s: bad reply header", addr)
		}
	}
	// Success: hresp ownership transfers to the closer (N7-1 lifecycle rule).
	return reply, newHTTPRespBody(hresp), nil
}
