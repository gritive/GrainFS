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

// Native shard-write route (Phase 8 N6 — the first genuinely native family).
//
// Wire: POST /shard/write?bucket=<esc>&key=<esc>&idx=<n>&sealed=<0|1>
//   - metadata = URL query params (URL-encoded; S3 keys are arbitrary bytes)
//   - request body = raw shard bytes, streamed (the sealed path carries the
//     8-byte completeness trailer appended by the put pipeline's sink — body
//     framing against mid-stream aborts, independent of this envelope-free wire)
//   - reply = the HTTP status: 200 empty = OK; 400 malformed params;
//     503 overloaded / handler not ready; 500 handler error (text in the body)
//
// No envelope frame, no X-Gfs-* headers, no FlatBuffers RPC envelope, no
// StreamRouter. This is the N7 template: URL = operation, query = metadata,
// body = bytes, status = result, handler registered by the consumer.
const httpShardWritePath = "/shard/write"

// shardWriteErrCap bounds how much of a 5xx response body the client reads as
// the remote error text.
const shardWriteErrCap = 4 << 10

// ShardWriteRequest is the native shard-write metadata. Sealed selects the
// verbatim already-sealed write (WriteSealedShard semantics: GFSENC3 bytes +
// completeness trailer, stored verbatim) vs the plaintext streaming write
// (WriteShard semantics: destination seals).
type ShardWriteRequest struct {
	Bucket   string
	Key      string
	ShardIdx int
	Sealed   bool
}

// ShardWriteHandler is the consumer-registered native handler. A nil return
// maps to 200; an error maps to 500 with the error text as the body.
type ShardWriteHandler func(req ShardWriteRequest, body io.Reader) error

// RegisterShardWriteHandler installs the native shard-write handler. Called by
// the composition root (serveruntime boot) — mirrors HandleBody registration,
// but for the native route. Listen runs before registration (bootClusterTransport
// precedes bootStreamRouter), so the route 503s until this is called. A nil h
// unregisters (the route reverts to 503) — never stored as a non-nil pointer to
// a nil func, which would dodge the not-ready check and panic at call time.
func (t *HTTPTransport) RegisterShardWriteHandler(h ShardWriteHandler) {
	if h == nil {
		t.shardWriteHandler.Store(nil)
		return
	}
	t.shardWriteHandler.Store(&h)
}

// InboundNativeShardWrites counts requests dispatched through the native
// /shard/write route (accepted by admission and handed to the handler).
// Test/observability accessor — the positive signal that the native route, not
// the tunnel, served the family (S5b lesson: prove dispatch, don't infer it).
func (t *HTTPTransport) InboundNativeShardWrites() uint64 {
	return t.nativeShardWrites.Load()
}

// handleShardWrite is the Hertz handler for POST /shard/write.
func (t *HTTPTransport) handleShardWrite(c context.Context, ctx *app.RequestContext) {
	hp := t.shardWriteHandler.Load()
	if hp == nil {
		ctx.SetStatusCode(consts.StatusServiceUnavailable)
		ctx.SetBodyString("shard write handler not ready")
		return
	}

	idx, err := strconv.Atoi(string(ctx.Query("idx")))
	if err != nil || idx < 0 {
		// Negative idx must be rejected here: it would reach storage as
		// s.dataDirs[shardIdx%len(s.dataDirs)] (shard_service.go getShardDir)
		// where a negative modulus panics/misroutes.
		ctx.SetStatusCode(consts.StatusBadRequest)
		ctx.SetBodyString("bad idx")
		return
	}
	sealedStr := string(ctx.Query("sealed"))
	if sealedStr != "0" && sealedStr != "1" {
		ctx.SetStatusCode(consts.StatusBadRequest)
		ctx.SetBodyString("bad sealed flag (want 0 or 1)")
		return
	}
	req := ShardWriteRequest{
		Bucket:   string(ctx.Query("bucket")),
		Key:      string(ctx.Query("key")),
		ShardIdx: idx,
		Sealed:   sealedStr == "1",
	}
	if req.Bucket == "" || req.Key == "" {
		ctx.SetStatusCode(consts.StatusBadRequest)
		ctx.SetBodyString("missing bucket/key")
		return
	}

	// Inbound admission: same traffic class the tunnel used for this family
	// (StreamShardWriteBody → bulk). The StreamType is an internal admission/
	// metrics key here — it is no longer on the wire; it dies with the tunnel in N8.
	t.mu.RLock()
	limiter := t.traffic
	t.mu.RUnlock()
	release, aerr := limiter.Acquire(c, StreamShardWriteBody)
	if aerr != nil {
		ctx.SetStatusCode(consts.StatusServiceUnavailable)
		ctx.SetBodyString("overloaded: " + aerr.Error())
		return
	}
	defer release()

	t.nativeShardWrites.Add(1)
	if herr := (*hp)(req, ctx.RequestBodyStream()); herr != nil {
		ctx.SetStatusCode(consts.StatusInternalServerError)
		ctx.SetBodyString(herr.Error())
		return
	}
	ctx.SetStatusCode(consts.StatusOK)
}

// ShardWrite streams one shard write to addr over the native route. The typed
// client method for the shard-write family (the N7 template for per-family
// client methods).
func (t *HTTPTransport) ShardWrite(ctx context.Context, addr string, req ShardWriteRequest, body io.Reader) error {
	c, err := t.httpClient()
	if err != nil {
		return err
	}
	q := url.Values{}
	q.Set("bucket", req.Bucket)
	q.Set("key", req.Key)
	q.Set("idx", strconv.Itoa(req.ShardIdx))
	if req.Sealed {
		q.Set("sealed", "1")
	} else {
		q.Set("sealed", "0")
	}

	hreq := protocol.AcquireRequest()
	hresp := protocol.AcquireResponse()
	defer protocol.ReleaseRequest(hreq)
	defer protocol.ReleaseResponse(hresp)
	hreq.SetMethod(consts.MethodPost)
	hreq.SetRequestURI("https://" + addr + httpShardWritePath + "?" + q.Encode())
	// Streamed request body (size unknown — the put pipeline streams through an
	// io.Pipe). hertzBodyReader loops past (0,nil) reads (Hertz panics on them).
	// A streamed body is un-retryable by design (httpRetryIf refuses IsBodyStream),
	// identical to the tunnel CallWithBody semantics.
	hreq.SetBodyStream(hertzBodyReader{r: body}, -1)

	if err := c.Do(ctx, hreq, hresp); err != nil {
		return fmt.Errorf("shard write %s: %w", addr, err)
	}
	if sc := hresp.StatusCode(); sc != consts.StatusOK {
		msg, rerr := io.ReadAll(io.LimitReader(hresp.BodyStream(), shardWriteErrCap))
		if rerr != nil {
			return fmt.Errorf("shard write %s: status %d (body unreadable: %v)", addr, sc, rerr)
		}
		return fmt.Errorf("shard write %s: status %d: %s", addr, sc, msg)
	}
	// Drain (empty on success) so the conn returns to the pool clean.
	_, _ = io.Copy(io.Discard, hresp.BodyStream())
	return nil
}
