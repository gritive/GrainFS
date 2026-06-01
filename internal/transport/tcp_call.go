package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// connPool is a minimal per-peer free-list of idle data-plane conns. S3a: no cap,
// no queue, no eviction — grow by dialing on an empty list, shrink by discarding
// (closing) any conn left dirty by an error/cancel. The elastic cap+queue policy
// is S3b.
type connPool struct {
	mu   sync.Mutex
	idle map[string][]net.Conn
}

func newConnPool() *connPool { return &connPool{idle: make(map[string][]net.Conn)} }

func (p *connPool) checkout(addr string) (net.Conn, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	q := p.idle[addr]
	if len(q) == 0 {
		return nil, false
	}
	c := q[len(q)-1]
	p.idle[addr] = q[:len(q)-1]
	return c, true
}

func (p *connPool) checkin(addr string, c net.Conn) {
	p.mu.Lock()
	p.idle[addr] = append(p.idle[addr], c)
	p.mu.Unlock()
}

func (p *connPool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, q := range p.idle {
		for _, c := range q {
			_ = c.Close()
		}
		delete(p.idle, addr)
	}
}

// getDataConn returns an idle pooled conn for addr or dials a fresh one.
func (t *TCPTransport) getDataConn(ctx context.Context, addr string) (net.Conn, error) {
	if c, ok := t.pool.checkout(addr); ok {
		return c, nil
	}
	return t.dial(ctx, addr)
}

// dial opens a fresh TLS-over-TCP connection to addr and completes the handshake
// (running SPKI pinning). connection-per-RPC: callers own and close the conn.
func (t *TCPTransport) dial(ctx context.Context, addr string) (*tls.Conn, error) {
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	conn := tls.Client(raw, t.clientTLS)
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("tls handshake %s: %w", addr, err)
	}
	return conn, nil
}

// applyCtx wires ctx cancellation/deadline to conn: a deadline sets the conn
// deadline; cancellation closes the conn (spec §4b: cancel = discard conn). The
// returned stop func must be called when the RPC completes to release the watcher.
func (t *TCPTransport) applyCtx(ctx context.Context, conn net.Conn) func() {
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			// Prefer done: if the RPC already completed (stop() closed done),
			// do NOT close the conn — CallRead may have handed it to the caller.
			select {
			case <-done:
			default:
				_ = conn.Close()
			}
		case <-done:
		}
	}()
	return func() { close(done) }
}

// Call dials a peer, writes the request frame, and reads the framed response.
// connection-per-RPC: a new conn per call, closed on return.
func (t *TCPTransport) Call(ctx context.Context, addr string, req *Message) (*Message, error) {
	conn, err := t.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stop := t.applyCtx(ctx, conn)
	defer stop()

	if err := t.codec.Encode(conn, req); err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	// No CloseWrite (framing invariant): the request frame is self-delimiting, so
	// the server needs no EOF to know the request ended. Sending close_notify
	// would leave unread data in the server's recv buffer => RST on its close.
	resp, err := t.codec.Decode(conn)
	if err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}
	return checkResponseStatus(addr, resp)
}

// CallWithBody checks out a pooled (or fresh) data-plane conn, writes the request
// frame + chunked body, reads the framed response, and returns the conn to the
// pool ONLY after a fully-clean cycle; any error/cancel discards it.
func (t *TCPTransport) CallWithBody(ctx context.Context, addr string, req *Message, body io.Reader) (*Message, error) {
	conn, err := t.getDataConn(ctx, addr)
	if err != nil {
		return nil, err
	}
	clean := false
	// Defer LIFO is load-bearing: stop() (registered LAST, runs FIRST) tears down
	// the ctx cancel-watcher, THEN this checkin/close defer runs — so by the time
	// the conn re-enters the pool the watcher can no longer close it. Do not reorder.
	defer func() {
		if clean {
			// Clear the per-call deadline before reuse: a checked-in conn that kept
			// it would fail the NEXT transfer's first I/O once the deadline passes
			// (idle conns sit in the pool), silently turning reuse into churn.
			_ = conn.SetDeadline(time.Time{})
			t.pool.checkin(addr, conn)
		} else {
			_ = conn.Close() // dirty/unknown state → never reuse
		}
	}()
	stop := t.applyCtx(ctx, conn)
	defer stop()

	if err := t.codec.Encode(conn, req); err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	if err := writeChunkedBody(conn, bodyOrEmpty(body)); err != nil {
		return nil, fmt.Errorf("stream body to %s: %w", addr, err)
	}
	resp, err := t.codec.Decode(conn)
	if err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}
	resp, err = checkResponseStatus(addr, resp)
	if err != nil {
		// A StatusError response is a complete frame with no body → conn is clean
		// and reusable; mark clean and surface the error.
		clean = true
		return nil, err
	}
	clean = true
	return resp, nil
}

// bodyOrEmpty returns an empty reader for a nil body so writeChunkedBody still
// emits the terminator, keeping the wire format uniform.
func bodyOrEmpty(r io.Reader) io.Reader {
	if r == nil {
		return bytes.NewReader(nil)
	}
	return r
}

// tcpReadCloser exposes the chunked response body after the metadata frame. On
// Close it returns the conn to the pool iff the body was fully drained to the
// terminator (clean), else closes it (dirty/early-close → discard).
type tcpReadCloser struct {
	t    *TCPTransport
	addr string
	conn net.Conn
	body *chunkedBodyReader
	once sync.Once
}

func (r *tcpReadCloser) Read(p []byte) (int, error) { return r.body.Read(p) }
func (r *tcpReadCloser) Close() error {
	var err error
	r.once.Do(func() {
		if r.body.done {
			// Fully drained → clean → reuse. The per-call deadline was already
			// cleared at handoff in CallRead and never re-armed during the body
			// read, so the checked-in conn has no deadline.
			r.t.pool.checkin(r.addr, r.conn)
		} else {
			err = r.conn.Close() // closed early / mid-stream → discard
		}
	})
	return err
}

// CallRead dials, writes the request frame, reads the framed metadata response,
// then returns the remaining conn bytes as the response body. The caller MUST
// Close the returned ReadCloser to release the connection.
//
// Note: the dial-time ctx deadline is cleared before the body ReadCloser is
// returned, so body reads are unbounded by the RPC ctx (matches QUIC). Callers
// needing a body-read timeout must set their own; the pooled data plane (S3)
// revisits this.
func (t *TCPTransport) CallRead(ctx context.Context, addr string, req *Message) (*Message, io.ReadCloser, error) {
	conn, err := t.getDataConn(ctx, addr)
	if err != nil {
		return nil, nil, err
	}
	// On any error before returning the body, close the conn (discard, never
	// pool a mid-cycle conn); on success the caller owns it via tcpReadCloser.
	ok := false
	defer func() {
		if !ok {
			_ = conn.Close()
		}
	}()
	stop := t.applyCtx(ctx, conn)

	if err := t.codec.Encode(conn, req); err != nil {
		stop()
		return nil, nil, fmt.Errorf("encode request: %w", err)
	}
	// No request body (CallRead): the request frame is self-delimiting; the server
	// replies with a metadata frame followed by the chunked response body.
	resp, err := t.codec.Decode(conn)
	if err != nil {
		stop()
		return nil, nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}
	if resp, err = checkResponseStatus(addr, resp); err != nil {
		stop()
		return nil, nil, err
	}
	// Hand the conn to the caller. Release the ctx watcher (stop) first, then
	// guard the handoff: if ctx ended around the same time the RPC succeeded, the
	// watcher may already have closed the conn (the prefer-done recheck only
	// covers the post-stop window). Refuse to hand off a possibly-doomed conn.
	stop()
	if cerr := ctx.Err(); cerr != nil {
		return nil, nil, fmt.Errorf("context done before body handoff from %s: %w", addr, cerr)
	}
	// Clear the dial deadline so the body read is unbounded by the RPC ctx
	// (matches QUIC). The body lifetime is owned by the caller's Close.
	_ = conn.SetDeadline(time.Time{})
	ok = true
	return resp, &tcpReadCloser{t: t, addr: addr, conn: conn, body: &chunkedBodyReader{r: conn}}, nil
}

// CallFlatBuffer sends a FlatBuffers-framed request (zero-copy from the builder)
// and reads the framed response. Builder must stay alive until this returns.
func (t *TCPTransport) CallFlatBuffer(ctx context.Context, addr string, fw *FlatBuffersWriter) (*Message, error) {
	conn, err := t.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stop := t.applyCtx(ctx, conn)
	defer stop()

	if err := t.codec.EncodeWriterTo(conn, fw); err != nil {
		return nil, fmt.Errorf("encode flatbuffer: %w", err)
	}
	// No CloseWrite (framing invariant): self-delimiting frame, no body (see Call).
	resp, err := t.codec.Decode(conn)
	if err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}
	return checkResponseStatus(addr, resp)
}

// Send delivers a fire-and-forget message (no response). connection-per-RPC:
// dial, write the frame, close. Used by gossip/receipt-gossip.
func (t *TCPTransport) Send(ctx context.Context, addr string, msg *Message) error {
	conn, err := t.dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := t.applyCtx(ctx, conn)
	defer stop()

	if err := t.codec.Encode(conn, msg); err != nil {
		return fmt.Errorf("encode message to %s: %w", addr, err)
	}
	// No CloseWrite (framing invariant): self-delimiting frame, no body, no
	// response. The deferred conn.Close flushes the frame before FIN (TCP
	// ordering); the server reads the frame, then EOF.
	return nil
}
