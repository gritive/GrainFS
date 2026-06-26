package transport

import (
	"io"
	"sync"

	"github.com/cloudwego/hertz/pkg/protocol"
)

// Shared HTTP client helpers used by the native per-family routes
// (http_shard_write.go, http_shard_read.go, http_forward.go,
// http_append_segment.go).

// maxPayloadSize bounds buffered request/reply payloads (allocation guard;
// WithResponseBodyStream bypasses Hertz's MaxResponseBodySize, so the native
// routes enforce this cap on every buffered io.ReadAll).
const maxPayloadSize = 64 * 1024 * 1024 // 64MB

// hertzBodyReader adapts an arbitrary request-body io.Reader for the Hertz
// chunked body writer, which PANICS on a (0, nil) read ("BUG: io.Reader returned
// 0, nil", ext/common.go WriteBodyChunked). The cluster PUT pipeline streams a
// sealed shard through an io.Pipe, and a zero-length pipe Write surfaces to the
// reader as (0, nil) — legal per the io.Reader contract ("callers should treat a
// return of 0 and nil as indicating that nothing happened"; callers MUST tolerate
// it) but fatal to Hertz. The TCP transport's chunked writer tolerated it; this
// restores parity by looping past empty non-EOF reads (the io.Pipe only yields
// (0,nil) per zero-length Write, then blocks for the next Write — no busy spin).
type hertzBodyReader struct{ r io.Reader }

func (b hertzBodyReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		n, err := b.r.Read(p)
		if n > 0 || err != nil {
			return n, err
		}
		// n == 0 && err == nil: "nothing happened" — read again, don't hand Hertz a (0,nil).
	}
}

// httpRespBody adapts a streaming Hertz response body to io.ReadCloser. Close
// releases the response (returning the conn to the pool) exactly once.
//
// The reset-per-Read IDLE bound (tcpReadCloser parity, S3b-cbd) is armed one layer
// down by idleReadConn (http_transport.go): Hertz reads the body through the dialed
// conn's Peek/Read, and idleReadConn re-arms SetReadTimeout(clientBodyTimeout)
// before each, so a server that stalls mid-body surfaces as a timeout error here
// (in this goroutine) rather than pinning it forever. This avoids the cross-
// goroutine CloseBodyStream-vs-Read watchdog Hertz forbids (the S8-2 BLOCKER).
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

type admissionReadCloser struct {
	io.ReadCloser
	release func()
	once    sync.Once
}

func holdAdmissionUntilClose(rc io.ReadCloser, release func()) io.ReadCloser {
	if release == nil {
		return rc
	}
	if rc == nil {
		release()
		return nil
	}
	return &admissionReadCloser{ReadCloser: rc, release: release}
}

func (r *admissionReadCloser) Close() error {
	var err error
	r.once.Do(func() {
		err = r.ReadCloser.Close()
		r.release()
	})
	return err
}
