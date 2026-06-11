package transport

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// HTTP data-plane RPC (S8-2). The transport.Message frame travels in X-Gfs-*
// headers; bodies are pure raw byte streams. The transport is a generic
// StreamType-routed tunnel (it never parses the shard envelope) — the same routing
// the TCP transport does via StreamRouter — carrying shard write/read, group
// forward, and append-segment body RPCs. See the S8-2 plan for the endpoint
// decision (generic /_grainfs/rpc, not RESTful /shard/{...}).
const (
	httpRPCPath = "/_grainfs/rpc"

	hdrGfsType    = "X-Gfs-Type"    // StreamType, decimal
	hdrGfsID      = "X-Gfs-Id"      // request ID, uint64 decimal
	hdrGfsStatus  = "X-Gfs-Status"  // MessageStatus, decimal (responses)
	hdrGfsPayload = "X-Gfs-Payload" // base64 of Message.Payload (request frame; streaming-response metadata)
)

// --- server registration (mirror TCPTransport; reuse the shared StreamRouter) ---

func (t *HTTPTransport) Handle(st StreamType, h StreamHandler)         { t.router.Handle(st, h) }
func (t *HTTPTransport) HandleBody(st StreamType, h StreamBodyHandler) { t.router.HandleBody(st, h) }
func (t *HTTPTransport) HandleRead(st StreamType, h StreamReadHandler) { t.router.HandleRead(st, h) }

func (t *HTTPTransport) SetStreamHandler(h StreamHandler) {
	t.mu.Lock()
	t.streamHandler = h
	t.mu.Unlock()
}

// parseReqMeta reads the request frame metadata (Type, ID) from the X-Gfs-*
// headers. The payload is NOT read here — its location depends on the call shape
// (see handleRPC): a CallWithBody envelope rides the X-Gfs-Payload header while the
// raw body streams; a Call/CallRead payload (possibly large, e.g. entries-AE or an
// InstallSnapshot) is the request BODY.
func parseReqMeta(ctx *app.RequestContext) (StreamType, uint64, error) {
	typeStr := string(ctx.GetHeader(hdrGfsType))
	if typeStr == "" {
		return 0, 0, errors.New("missing X-Gfs-Type")
	}
	typ, err := strconv.ParseUint(typeStr, 10, 8)
	if err != nil {
		return 0, 0, fmt.Errorf("bad X-Gfs-Type: %w", err)
	}
	var id uint64
	if s := string(ctx.GetHeader(hdrGfsID)); s != "" {
		if id, err = strconv.ParseUint(s, 10, 64); err != nil {
			return 0, 0, fmt.Errorf("bad X-Gfs-Id: %w", err)
		}
	}
	return StreamType(typ), id, nil
}

// readReqBodyPayload reads a Call/CallRead request payload from the request BODY,
// capped at maxPayloadSize (the codec's allocation guard). This keeps large raft
// payloads (entries-AE, InstallSnapshot) off the header.
func readReqBodyPayload(ctx *app.RequestContext) ([]byte, error) {
	p, err := io.ReadAll(io.LimitReader(ctx.RequestBodyStream(), maxPayloadSize+1))
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	if len(p) > maxPayloadSize {
		return nil, fmt.Errorf("request payload exceeds max %d", maxPayloadSize)
	}
	return p, nil
}

func decodePayloadHeader(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("bad X-Gfs-Payload: %w", err)
	}
	if len(b) > maxPayloadSize {
		return nil, fmt.Errorf("payload size %d exceeds max %d", len(b), maxPayloadSize)
	}
	return b, nil
}

// writeRespMeta sets the response frame metadata headers (Type/ID/Status) and a 200.
func writeRespMeta(ctx *app.RequestContext, msg *Message) {
	ctx.Header(hdrGfsType, strconv.FormatUint(uint64(msg.Type), 10))
	ctx.Header(hdrGfsID, strconv.FormatUint(msg.ID, 10))
	ctx.Header(hdrGfsStatus, strconv.FormatUint(uint64(msg.Status), 10))
	ctx.SetStatusCode(consts.StatusOK)
}

// handleRPC dispatches one HTTP RPC by StreamType (mirrors TCP serveConn routing).
// For a streaming (HandleRead) response it writes metadata + a small payload header
// and streams the body; for all others it writes metadata to headers and the
// (possibly large) payload to the response BODY.
func (t *HTTPTransport) handleRPC(_ context.Context, ctx *app.RequestContext) {
	typ, id, err := parseReqMeta(ctx)
	if err != nil {
		ctx.SetStatusCode(consts.StatusBadRequest)
		return
	}

	// HandleBody (CallWithBody): the small FB envelope rides the X-Gfs-Payload
	// header and the request body is the raw stream handed to the handler.
	if h, ok := t.router.LookupBody(typ); ok {
		payload, perr := decodePayloadHeader(string(ctx.GetHeader(hdrGfsPayload)))
		if perr != nil {
			ctx.SetStatusCode(consts.StatusBadRequest)
			return
		}
		req := &Message{Type: typ, ID: id, Payload: payload}
		writeBufferedResp(ctx, req, h(req, ctx.RequestBodyStream()))
		return
	}

	// Call / CallRead / catch-all: the payload (possibly large — entries-AE,
	// InstallSnapshot) is the request BODY.
	payload, perr := readReqBodyPayload(ctx)
	if perr != nil {
		ctx.SetStatusCode(consts.StatusBadRequest)
		return
	}
	req := &Message{Type: typ, ID: id, Payload: payload}

	if h, ok := t.router.LookupRead(typ); ok {
		resp, rbody := h(req)
		if resp == nil {
			resp = NewErrorResponse(req, StatusError, errors.New("nil read response"))
		}
		writeRespMeta(ctx, resp)
		// Streaming-response metadata payload (small OK/Error envelope) rides a header
		// so the body stays a pure raw stream.
		if len(resp.Payload) > 0 {
			ctx.Header(hdrGfsPayload, base64.StdEncoding.EncodeToString(resp.Payload))
		}
		if rbody != nil {
			ctx.SetBodyStream(rbody, -1) // Hertz closes the io.Closer after writing
		}
		return
	}
	if h, ok := t.router.Lookup(typ); ok {
		writeBufferedResp(ctx, req, h(req))
		return
	}
	t.mu.RLock()
	sh := t.streamHandler
	t.mu.RUnlock()
	if sh != nil {
		writeBufferedResp(ctx, req, sh(req))
		return
	}
	writeBufferedResp(ctx, req, NewErrorResponse(req, StatusError, errors.New("no handler for stream type")))
}

// writeBufferedResp writes a non-streaming response: metadata to headers, the
// (possibly large, e.g. ReadShard data) payload to the response BODY.
func writeBufferedResp(ctx *app.RequestContext, req *Message, resp *Message) {
	if resp == nil {
		resp = NewErrorResponse(req, StatusError, errors.New("nil response"))
	}
	writeRespMeta(ctx, resp)
	if len(resp.Payload) > 0 {
		ctx.Response.SetBody(resp.Payload)
	}
}

// --- client ---

// doRPC performs one HTTP RPC.
//
// Payload placement (large-safe): when body == nil (Call/CallFlatBuffer/CallRead) the
// request Message.Payload — which may be LARGE (entries-bearing AppendEntries,
// InstallSnapshot) — is sent as the request BODY. When body != nil (CallWithBody) the
// small FB envelope rides the X-Gfs-Payload header and `body` is the raw stream. A
// payload in a header would blow Hertz's header-size limit for big raft payloads.
//
// When stream is true the response body is returned as a ReadCloser the caller must
// Close (which releases the pooled Hertz response); otherwise the full response body
// is read into the reply Message.Payload and the response is released here.
func (t *HTTPTransport) doRPC(ctx context.Context, addr string, req *Message, body io.Reader, stream bool) (*Message, io.ReadCloser, error) {
	c, err := t.httpClient()
	if err != nil {
		return nil, nil, err
	}
	hreq := protocol.AcquireRequest()
	hresp := protocol.AcquireResponse()
	hreq.SetMethod(consts.MethodPost)
	hreq.SetRequestURI("https://" + addr + httpRPCPath)
	hreq.Header.Set(hdrGfsType, strconv.FormatUint(uint64(req.Type), 10))
	hreq.Header.Set(hdrGfsID, strconv.FormatUint(req.ID, 10))
	if body != nil {
		// CallWithBody: small envelope in the header, raw stream as the body.
		if len(req.Payload) > 0 {
			hreq.Header.Set(hdrGfsPayload, base64.StdEncoding.EncodeToString(req.Payload))
		}
		hreq.SetBodyStream(body, -1)
	} else if len(req.Payload) > 0 {
		// Call/CallFlatBuffer/CallRead: payload (possibly large) is the request body.
		hreq.SetBodyStream(bytes.NewReader(req.Payload), len(req.Payload))
	}

	if err := c.Do(ctx, hreq, hresp); err != nil {
		protocol.ReleaseRequest(hreq)
		protocol.ReleaseResponse(hresp)
		return nil, nil, fmt.Errorf("http rpc %s: %w", addr, err)
	}
	protocol.ReleaseRequest(hreq)

	if hresp.StatusCode() != consts.StatusOK {
		code := hresp.StatusCode()
		protocol.ReleaseResponse(hresp)
		return nil, nil, fmt.Errorf("http rpc %s: status %d", addr, code)
	}
	msg, err := respMeta(hresp)
	if err != nil {
		protocol.ReleaseResponse(hresp)
		return nil, nil, err
	}

	if stream {
		// Streaming-response payload (small metadata) is in the header; the body is
		// the stream the caller reads.
		msg.Payload, err = decodePayloadHeader(hresp.Header.Get(hdrGfsPayload))
		if err != nil {
			protocol.ReleaseResponse(hresp)
			return nil, nil, err
		}
		// Map RPC-level status to a Go error BEFORE handing back a body (mirror TCP
		// CallRead's checkResponseStatus at tcp_call.go:311): a StatusError reply has
		// no body to stream, and consumers rely on err, not msg.Status.
		if _, serr := checkResponseStatus(addr, msg); serr != nil {
			protocol.ReleaseResponse(hresp)
			return nil, nil, serr
		}
		return msg, newHTTPRespBody(hresp), nil
	}

	// Non-streaming: the payload is the (possibly large, e.g. ReadShard data) response
	// body. Cap it like the TCP codec (codec.go:105) — WithResponseBodyStream bypasses
	// Hertz's MaxResponseBodySize, so a buggy/hostile peer could otherwise stream an
	// unbounded body into this io.ReadAll.
	payload, err := io.ReadAll(io.LimitReader(hresp.BodyStream(), maxPayloadSize+1))
	protocol.ReleaseResponse(hresp)
	if err != nil {
		return nil, nil, fmt.Errorf("http rpc %s: read body: %w", addr, err)
	}
	if len(payload) > maxPayloadSize {
		return nil, nil, fmt.Errorf("http rpc %s: response payload exceeds max %d", addr, maxPayloadSize)
	}
	msg.Payload = payload
	// Map RPC-level status to a Go error, mirroring every TCP Call* path
	// (checkResponseStatus): a StatusError/StatusOverloaded reply must surface as err,
	// not a Message with err==nil that consumers (shard_service.Ping, quorum_meta) take
	// as success and then mis-parse the error string as an envelope.
	if _, serr := checkResponseStatus(addr, msg); serr != nil {
		return nil, nil, serr
	}
	return msg, nil, nil
}

// respMeta reads the response frame metadata (Type/ID/Status) from headers.
func respMeta(resp *protocol.Response) (*Message, error) {
	typeStr := resp.Header.Get(hdrGfsType)
	if typeStr == "" {
		return nil, errors.New("missing X-Gfs-Type in response")
	}
	typ, err := strconv.ParseUint(typeStr, 10, 8)
	if err != nil {
		return nil, fmt.Errorf("bad X-Gfs-Type in response: %w", err)
	}
	var id uint64
	if s := resp.Header.Get(hdrGfsID); s != "" {
		if id, err = strconv.ParseUint(s, 10, 64); err != nil {
			return nil, fmt.Errorf("bad X-Gfs-Id in response: %w", err)
		}
	}
	var status uint64
	if s := resp.Header.Get(hdrGfsStatus); s != "" {
		if status, err = strconv.ParseUint(s, 10, 8); err != nil {
			return nil, fmt.Errorf("bad X-Gfs-Status in response: %w", err)
		}
	}
	return &Message{Type: StreamType(typ), ID: id, Status: MessageStatus(status)}, nil
}

// httpRespBody adapts a streaming Hertz response body to io.ReadCloser. Close
// releases the response (returning the conn to the pool) exactly once.
//
// NOTE — deferred parity gap (idle read deadline): tcpReadCloser arms a
// reset-per-Read conn deadline so a server that stalls mid-body cannot pin this
// goroutine + pooled conn forever (the S3b-cbd hardening). There is no safe
// equivalent here yet: Hertz forbids calling CloseBodyStream() and
// BodyStream().Read() concurrently (response.go), so a watchdog that Closes from a
// second goroutine is a data race + use-after-pool; and a conn-level read deadline
// can't be plumbed cleanly because Hertz reads the body through its own buffered
// Reader (Peek/fill), not net.Conn.Read, and SetReadTimeout is a one-shot deadline
// (not reset-per-Read). The correct in-Read-goroutine idle bound is designed in S8-3
// when this transport is wired and the consumers' read patterns are concrete
// (tracked in TODOS.md). Until then a CallRead body read can block on a stalled peer
// for as long as the caller's ctx/socket allows — acceptable while dormant.
type httpRespBody struct {
	resp *protocol.Response
	r    io.Reader
	once sync.Once
}

func newHTTPRespBody(resp *protocol.Response) *httpRespBody {
	return &httpRespBody{resp: resp, r: resp.BodyStream()}
}

func (b *httpRespBody) Read(p []byte) (int, error) { return b.r.Read(p) }

func (b *httpRespBody) Close() error {
	var err error
	b.once.Do(func() {
		err = b.resp.CloseBodyStream()
		protocol.ReleaseResponse(b.resp)
	})
	return err
}

// Call sends a request with no body and a buffered reply (mirrors TCP Call).
func (t *HTTPTransport) Call(ctx context.Context, addr string, req *Message) (*Message, error) {
	msg, _, err := t.doRPC(ctx, addr, req, nil, false)
	return msg, err
}

// CallPooled is Call (the Hertz client already pools connections).
func (t *HTTPTransport) CallPooled(ctx context.Context, addr string, req *Message) (*Message, error) {
	return t.Call(ctx, addr, req)
}

// CallWithBody streams body as the request body and returns the buffered reply.
func (t *HTTPTransport) CallWithBody(ctx context.Context, addr string, req *Message, body io.Reader) (*Message, error) {
	msg, _, err := t.doRPC(ctx, addr, req, body, false)
	return msg, err
}

// CallRead sends a request and returns the reply frame + a streaming response body
// the caller must Close.
func (t *HTTPTransport) CallRead(ctx context.Context, addr string, req *Message) (*Message, io.ReadCloser, error) {
	return t.doRPC(ctx, addr, req, nil, true)
}

// CallFlatBuffer encodes the FlatBuffers writer into a Message and calls it.
func (t *HTTPTransport) CallFlatBuffer(ctx context.Context, addr string, fw *FlatBuffersWriter) (*Message, error) {
	req := &Message{Type: fw.Typ, ID: fw.ID, Status: fw.Status, Payload: fw.Builder.FinishedBytes()}
	return t.Call(ctx, addr, req)
}
