package transport

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// Native shard-read route (Phase 8 N7-1 — second native family; pins the
// streaming-RESPONSE conventions for the remaining read-shaped families).
//
// Wire: GET /shard/read?bucket=<esc>&key=<esc>&idx=<n>[&offset=<n>&length=<n>]
//   - offset+length BOTH present = bounded range read (ReadShardRangeStream
//     semantics); BOTH absent = whole shard (ReadShard); one alone = 400
//   - success = 200 + raw streaming body (no metadata envelope — the tunnel's
//     success envelope carried nothing)
//   - errors = 400 malformed params; 503 not ready / overloaded; 500 handler
//     error with the error text as the body
//
// GET has no request body, so the request is retryable on a stale pooled conn
// (httpRetryIf), matching tunnel CallRead. The returned body stream is bounded
// per-Read by the dialed conn's idle deadline (idleReadConn), same as the tunnel.
const httpShardReadPath = "/shard/read"

// shardReadErrCap bounds how much of a 4xx/5xx response body the client reads
// as the remote error text.
const shardReadErrCap = 4 << 10

// ShardReadRequest is the native shard-read metadata. Range=true reads
// [Offset, Offset+Length) of the stored shard stream; Range=false reads the
// whole shard.
type ShardReadRequest struct {
	Bucket   string
	Key      string
	ShardIdx int
	Range    bool
	Offset   int64
	Length   int64
}

// ShardReadHandler is the consumer-registered native handler. A nil error maps
// to 200 with the ReadCloser streamed as the body (the transport closes it);
// an error maps to 500 with the error text as the body.
type ShardReadHandler func(req ShardReadRequest) (io.ReadCloser, error)

// RegisterShardReadHandler installs the native shard-read handler. Same
// contract as RegisterShardWriteHandler: consumer-registered post-Listen, nil
// unregisters (route reverts to 503).
func (t *HTTPTransport) RegisterShardReadHandler(h ShardReadHandler) {
	if h == nil {
		t.shardReadHandler.Store(nil)
		return
	}
	t.shardReadHandler.Store(&h)
}

// InboundNativeShardReads counts requests dispatched through the native
// /shard/read route. Test/observability accessor (positive dispatch signal).
func (t *HTTPTransport) InboundNativeShardReads() uint64 {
	return t.nativeShardReads.Load()
}

// handleShardRead is the Hertz handler for GET /shard/read.
func (t *HTTPTransport) handleShardRead(c context.Context, ctx *app.RequestContext) {
	hp := t.shardReadHandler.Load()
	if hp == nil {
		ctx.SetStatusCode(consts.StatusServiceUnavailable)
		ctx.SetBodyString("shard read handler not ready")
		return
	}

	idx, err := strconv.Atoi(string(ctx.Query("idx")))
	if err != nil || idx < 0 {
		ctx.SetStatusCode(consts.StatusBadRequest)
		ctx.SetBodyString("bad idx")
		return
	}
	req := ShardReadRequest{
		Bucket:   string(ctx.Query("bucket")),
		Key:      string(ctx.Query("key")),
		ShardIdx: idx,
	}
	if req.Bucket == "" || req.Key == "" {
		ctx.SetStatusCode(consts.StatusBadRequest)
		ctx.SetBodyString("missing bucket/key")
		return
	}
	offStr, lenStr := string(ctx.Query("offset")), string(ctx.Query("length"))
	if (offStr == "") != (lenStr == "") {
		ctx.SetStatusCode(consts.StatusBadRequest)
		ctx.SetBodyString("offset and length must be given together")
		return
	}
	if offStr != "" {
		off, oerr := strconv.ParseInt(offStr, 10, 64)
		ln, lerr := strconv.ParseInt(lenStr, 10, 64)
		if oerr != nil || lerr != nil || off < 0 || ln < 0 {
			ctx.SetStatusCode(consts.StatusBadRequest)
			ctx.SetBodyString("bad offset/length")
			return
		}
		req.Range, req.Offset, req.Length = true, off, ln
	}

	// Inbound admission: same class the tunnel used (StreamShardReadBody →
	// bulk). For streaming responses, hold the slot until Hertz closes the body
	// so the limit covers active egress, not only handler setup.
	release, aerr := t.acquireAdmission(c, StreamShardReadBody)
	if aerr != nil {
		ctx.SetStatusCode(consts.StatusServiceUnavailable)
		ctx.SetBodyString("overloaded: " + aerr.Error())
		return
	}

	t.nativeShardReads.Add(1)
	rc, herr := (*hp)(req)
	if herr != nil {
		release()
		ctx.SetStatusCode(consts.StatusInternalServerError)
		ctx.SetBodyString(herr.Error())
		return
	}
	ctx.SetStatusCode(consts.StatusOK)
	ctx.SetBodyStream(holdAdmissionUntilClose(rc, release), -1) // Hertz closes the io.Closer after writing
}

// ShardRead fetches one shard (whole or bounded range) from addr over the
// native route. On success the returned ReadCloser streams the shard bytes and
// OWNS the pooled response — the caller must Close it exactly once.
func (t *HTTPTransport) ShardRead(ctx context.Context, addr string, req ShardReadRequest) (io.ReadCloser, error) {
	c, err := t.httpClient()
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	q.Set("bucket", req.Bucket)
	q.Set("key", req.Key)
	q.Set("idx", strconv.Itoa(req.ShardIdx))
	if req.Range {
		q.Set("offset", strconv.FormatInt(req.Offset, 10))
		q.Set("length", strconv.FormatInt(req.Length, 10))
	}

	hreq := protocol.AcquireRequest()
	hresp := protocol.AcquireResponse()
	hreq.SetMethod(consts.MethodGet)
	hreq.SetRequestURI("https://" + addr + httpShardReadPath + "?" + q.Encode())

	if err := c.Do(ctx, hreq, hresp); err != nil {
		protocol.ReleaseRequest(hreq)
		protocol.ReleaseResponse(hresp)
		return nil, fmt.Errorf("shard read %s: %w", addr, classifyShardClientErr(err))
	}
	protocol.ReleaseRequest(hreq)

	if sc := hresp.StatusCode(); sc != consts.StatusOK {
		msg, rerr := io.ReadAll(io.LimitReader(hresp.BodyStream(), shardReadErrCap))
		protocol.ReleaseResponse(hresp)
		if rerr != nil {
			return nil, fmt.Errorf("shard read %s: status %d (body unreadable: %v)", addr, sc, rerr)
		}
		return nil, fmt.Errorf("shard read %s: status %d: %s", addr, sc, msg)
	}
	// Success: ownership of hresp transfers to the ReadCloser (released on
	// Close, exactly once — newHTTPRespBody). NO defer ReleaseResponse here.
	return newHTTPRespBody(hresp), nil
}
