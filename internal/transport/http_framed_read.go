package transport

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// framedReadClient bundles the per-family knobs for a streamed-response framed
// read (the GET /forward/read and GET /append-segment/read clients share an
// otherwise byte-identical skeleton). It is a small fixed struct passed by value
// from a package-level var, so dispatch adds no heap alloc on the hot path.
type framedReadClient struct {
	path      string // route path, e.g. httpForwardReadPath
	frameHdr  string // request header carrying the base64 frame
	replyHdr  string // response header carrying the base64 reply metadata
	maxFrame  int    // request frame upper bound
	maxReply  int    // reply metadata upper bound (ASYMMETRIC per family — see vars)
	errPrefix string // error-message prefix, e.g. "forward read"
}

// forwardReadClient / appendSegReadClient pin each family's knobs. The reply
// bound is INTENTIONALLY asymmetric — Forward replies may be up to maxPayloadSize
// (64MB) while append-segment replies are status frames capped at
// maxAppendFrameBytes (256KiB). Collapsing maxReply to a single value would
// silently shrink the Forward reply ceiling, a behavior change. Do NOT merge.
var (
	forwardReadClient = framedReadClient{
		path:      httpForwardReadPath,
		frameHdr:  hdrForwardFrame,
		replyHdr:  hdrForwardReply,
		maxFrame:  maxForwardFrameBytes,
		maxReply:  maxPayloadSize,
		errPrefix: "forward read",
	}
	appendSegReadClient = framedReadClient{
		path:      httpAppendSegmentReadPath,
		frameHdr:  hdrAppendFrame,
		replyHdr:  hdrAppendReply,
		maxFrame:  maxAppendFrameBytes,
		maxReply:  maxAppendFrameBytes,
		errPrefix: "append-segment read",
	}
)

// framedRead performs one streamed-response framed read against addr. On success
// the returned reply is the family's reply-metadata frame and the ReadCloser
// streams the response body; the closer OWNS the pooled response (Close exactly
// once).
//
// Ownership contract (landmine — preserved exactly): the request is always
// released; on a non-200 status the error body is read and the response released
// before returning; on success hresp ownership transfers to newHTTPRespBody and
// there is NO defer ReleaseResponse on the success path (ForwardWrite defers
// because it fully buffers; a streamed reply must not, or it would double-free /
// leak the pooled conn).
func (t *HTTPTransport) framedRead(ctx context.Context, fc framedReadClient, addr string, frame []byte) ([]byte, io.ReadCloser, error) {
	if len(frame) == 0 || len(frame) > fc.maxFrame {
		return nil, nil, fmt.Errorf("%s: frame size %d outside (0, %d]", fc.errPrefix, len(frame), fc.maxFrame)
	}
	c, err := t.httpClient()
	if err != nil {
		return nil, nil, err
	}
	hreq := protocol.AcquireRequest()
	hresp := protocol.AcquireResponse()
	hreq.SetMethod(consts.MethodGet)
	hreq.SetRequestURI("https://" + addr + fc.path)
	hreq.Header.Set(fc.frameHdr, base64.StdEncoding.EncodeToString(frame))

	if err := c.Do(ctx, hreq, hresp); err != nil {
		protocol.ReleaseRequest(hreq)
		protocol.ReleaseResponse(hresp)
		return nil, nil, fmt.Errorf("%s %s: %w", fc.errPrefix, addr, err)
	}
	protocol.ReleaseRequest(hreq)

	if sc := hresp.StatusCode(); sc != consts.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(hresp.BodyStream(), forwardErrCap))
		protocol.ReleaseResponse(hresp)
		return nil, nil, fmt.Errorf("%s %s: status %d: %s", fc.errPrefix, addr, sc, msg)
	}
	var reply []byte
	if s := hresp.Header.Get(fc.replyHdr); s != "" {
		reply, err = base64.StdEncoding.DecodeString(s)
		if err != nil || len(reply) > fc.maxReply {
			protocol.ReleaseResponse(hresp)
			return nil, nil, fmt.Errorf("%s %s: bad reply header", fc.errPrefix, addr)
		}
	}
	// Success: hresp ownership transfers to the closer (N7-1 lifecycle rule).
	// No defer ReleaseResponse — the closer owns it.
	return reply, newHTTPRespBody(hresp), nil
}
