package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// errStalePreBody marks a data-plane RPC that failed on a REUSED pooled conn
// before any request-body byte was written. The body io.Reader is still pristine
// in that window, so the RPC can be transparently retried once on a fresh dial —
// closing the stale-pooled-conn failure mode that pooling introduces over the
// S1 connection-per-RPC model.
var errStalePreBody = errors.New("transport: reused conn failed before body write")

// getDataConn returns a pooled (reused) conn or dials a fresh one. The bool
// reports whether the conn was reused — only a reused conn is eligible for the
// stale-conn retry-once (a fresh dial that fails is a real connectivity error).
func (t *TCPTransport) getDataConn(ctx context.Context, addr string) (net.Conn, bool, error) {
	c, err := t.pool.checkout(ctx, addr)
	if err != nil {
		return nil, false, err
	}
	if c != nil {
		return c, true, nil // reused pooled conn
	}
	// checkout granted a (counted) dial slot — dial it. Capture the identity
	// generation BEFORE dialing: if a recycle/revoke fires DURING the handshake, the
	// pre-dial stamp is below the bumped gen, so this conn (whose handshake ran under
	// the now-stale identity) is discarded on checkin instead of re-pooled (S5a).
	gen := t.pool.genSnapshot(addr)
	dialed, derr := t.dial(ctx, addr)
	if derr != nil {
		t.pool.dialFailed(addr) // release the counted slot + wake a waiter
		return nil, false, derr
	}
	t.pool.stampWith(dialed, gen)
	return dialed, false, nil
}

// dial opens a fresh TLS-over-TCP connection to addr and completes the handshake
// (running SPKI pinning). The raw TCP conn is tuned (NODELAY + buffers) before TLS.
func (t *TCPTransport) dial(ctx context.Context, addr string) (*tls.Conn, error) {
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	t.tuneTCP(raw)
	conn := tls.Client(raw, t.buildClientTLS())
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("tls handshake %s: %w", addr, err)
	}
	return conn, nil
}

// dialMux opens a fresh TLS-over-TCP conn using the mux-ONLY client config and fails
// closed if the peer did not negotiate the mux ALPN (gate-check #1: a mux carrier
// must never carry a data-plane-protocol conn). Used by tcpOutboundMuxCarrier.
func (t *TCPTransport) dialMux(ctx context.Context, addr string) (*tls.Conn, error) {
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial mux %s: %w", addr, err)
	}
	t.tuneTCP(raw)
	conn := tls.Client(raw, t.buildMuxClientTLS())
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("tls handshake mux %s: %w", addr, err)
	}
	if p := conn.ConnectionState().NegotiatedProtocol; p != tcpMuxALPN {
		_ = conn.Close()
		return nil, fmt.Errorf("peer at %s negotiated %q (expected mux %q)", addr, p, tcpMuxALPN)
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
// connection-per-RPC: a new conn per call, closed on return (NOT pooled).
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
// pool ONLY after a fully-clean cycle. A reused conn that dies before the body is
// written is retried once on a fresh dial (the body is still pristine).
func (t *TCPTransport) CallWithBody(ctx context.Context, addr string, req *Message, body io.Reader) (*Message, error) {
	for attempt := 0; ; attempt++ {
		conn, reused, err := t.getDataConn(ctx, addr)
		if err != nil {
			return nil, err
		}
		resp, rerr := t.callWithBodyOnce(ctx, addr, conn, req, body)
		if rerr != nil && reused && attempt == 0 && errors.Is(rerr, errStalePreBody) {
			continue // the stale conn was discarded inside callWithBodyOnce; dial fresh
		}
		return resp, rerr
	}
}

func (t *TCPTransport) callWithBodyOnce(ctx context.Context, addr string, conn net.Conn, req *Message, body io.Reader) (*Message, error) {
	clean := false
	// Defer LIFO: stop() (registered LAST, runs FIRST) closes the watcher's done
	// channel; THEN this checkin/discard defer runs.
	defer func() {
		// Pool the conn only if the cycle was clean AND ctx never fired. stop() is
		// an ASYNC signal — if ctx.Err()!=nil the watcher may still close this conn,
		// so pooling it would let a later transfer check out a doomed conn.
		if clean && ctx.Err() == nil {
			// Clear the per-call deadline before reuse: a checked-in conn that kept
			// it would fail the NEXT transfer's first I/O once the deadline passes.
			_ = conn.SetDeadline(time.Time{})
			t.pool.checkin(addr, conn)
		} else {
			t.pool.discard(addr, conn) // dirty / cancelled / stale → close + free slot
		}
	}()
	stop := t.applyCtx(ctx, conn)
	defer stop()

	req.ID = t.nextDataPlaneID() // stamp for desync detection (resp must echo it)
	if err := t.codec.Encode(conn, req); err != nil {
		// Pre-body failure: body untouched → safe to retry on a fresh conn.
		return nil, fmt.Errorf("%w: encode request to %s: %v", errStalePreBody, addr, err)
	}
	if err := writeChunkedBody(conn, bodyOrEmpty(body)); err != nil {
		return nil, fmt.Errorf("stream body to %s: %w", addr, err)
	}
	resp, err := t.codec.Decode(conn)
	if err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}
	if resp.ID != req.ID {
		// Pooled-conn desync: the response belongs to a different transfer. Surface
		// it LOUD (do not retry — this is a protocol/handler bug, not a dead conn).
		return nil, fmt.Errorf("transport: response id %d != request id %d from %s (conn desync)", resp.ID, req.ID, addr)
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
// terminator (clean), else discards it (dirty/early-close → free slot).
type tcpReadCloser struct {
	t    *TCPTransport
	addr string
	conn net.Conn
	body *chunkedBodyReader
	once sync.Once
}

func (r *tcpReadCloser) Read(p []byte) (int, error) {
	// Arm an IDLE read deadline before each delegated body read (QUIC-parity bound):
	// CallRead cleared the per-call deadline at handoff so the body read is unbounded
	// by the RPC ctx, but a server that stalls mid-body (without closing) would
	// otherwise pin this goroutine + the pooled slot forever. Re-arming per Read gives
	// idle semantics — a progressing transfer keeps resetting the window, only a
	// genuine stall trips it.
	if d := r.t.cfg.ClientBodyTimeout; d > 0 {
		_ = r.conn.SetReadDeadline(time.Now().Add(d))
	}
	return r.body.Read(p)
}
func (r *tcpReadCloser) Close() error {
	r.once.Do(func() {
		if r.body.done {
			// Fully drained → clean → reuse. Clear the idle read deadline armed during
			// the body read before checkin, so the next transfer on this pooled conn
			// is not killed by a stale deadline (the S3b stale-deadline reuse class).
			_ = r.conn.SetDeadline(time.Time{})
			r.t.pool.checkin(r.addr, r.conn)
		} else {
			r.t.pool.discard(r.addr, r.conn) // closed early / mid-stream → discard + free slot
		}
	})
	return nil
}

// CallRead dials/checks out a conn, writes the request frame, reads the framed
// metadata response, then returns the remaining conn bytes as the response body.
// The caller MUST Close the returned ReadCloser to release the connection. A
// reused conn that fails before the body handoff is retried once on a fresh dial
// (CallRead has no request body, so any pre-handoff failure is safely retryable).
func (t *TCPTransport) CallRead(ctx context.Context, addr string, req *Message) (*Message, io.ReadCloser, error) {
	for attempt := 0; ; attempt++ {
		conn, reused, err := t.getDataConn(ctx, addr)
		if err != nil {
			return nil, nil, err
		}
		resp, rc, rerr := t.callReadOnce(ctx, addr, conn, req)
		if rerr != nil && reused && attempt == 0 && errors.Is(rerr, errStalePreBody) {
			continue
		}
		return resp, rc, rerr
	}
}

func (t *TCPTransport) callReadOnce(ctx context.Context, addr string, conn net.Conn, req *Message) (*Message, io.ReadCloser, error) {
	// On any non-handoff exit, discard the conn (never pool a mid-cycle conn); on
	// success the caller owns it via tcpReadCloser.
	handedOff := false
	defer func() {
		if !handedOff {
			t.pool.discard(addr, conn)
		}
	}()
	stop := t.applyCtx(ctx, conn)

	req.ID = t.nextDataPlaneID()
	if err := t.codec.Encode(conn, req); err != nil {
		stop()
		return nil, nil, fmt.Errorf("%w: encode request to %s: %v", errStalePreBody, addr, err)
	}
	resp, err := t.codec.Decode(conn)
	if err != nil {
		stop()
		return nil, nil, fmt.Errorf("%w: decode response from %s: %v", errStalePreBody, addr, err)
	}
	if resp.ID != req.ID {
		stop()
		return nil, nil, fmt.Errorf("transport: response id %d != request id %d from %s (conn desync)", resp.ID, req.ID, addr)
	}
	if resp, err = checkResponseStatus(addr, resp); err != nil {
		stop()
		return nil, nil, err // StatusError: not retryable (real server error, clean frame)
	}
	// Hand the conn to the caller. Release the ctx watcher (stop) first, then guard
	// the handoff: if ctx ended around the same time the RPC succeeded, the watcher
	// may already have closed the conn. Refuse to hand off a possibly-doomed conn.
	stop()
	if cerr := ctx.Err(); cerr != nil {
		return nil, nil, fmt.Errorf("context done before body handoff from %s: %w", addr, cerr)
	}
	// Clear the dial deadline so the body read is unbounded by the RPC ctx (matches
	// QUIC). The body lifetime is owned by the caller's Close.
	_ = conn.SetDeadline(time.Time{})
	handedOff = true
	return resp, &tcpReadCloser{t: t, addr: addr, conn: conn, body: &chunkedBodyReader{r: conn}}, nil
}

// CallFlatBuffer sends a FlatBuffers-framed request (zero-copy from the builder)
// and reads the framed response. Builder must stay alive until this returns.
//
// Pooled, like CallWithBody: it checks out a reused (or fresh) data-plane conn
// and returns it to the pool only after a fully-clean request/response cycle.
// This is the shard-write hot path (ShardService.WriteShard); leaving it
// connection-per-RPC made every EC shard write to a peer pay a fresh TLS
// handshake. The request is a self-delimiting frame with no body, so a clean
// cycle leaves the conn at a frame boundary (safe to reuse, HTTP-keep-alive
// style). A reused conn that dies before the request frame is written is retried
// once on a fresh dial (the builder is still pristine).
//
// Intentional divergence from CallWithBody: no response-ID desync guard. The
// BinaryCodec is strictly length-prefixed — Decode reads a fixed 14-byte header
// then exactly payloadLen bytes (codec.go:94, two io.ReadFull, no trailing
// bytes) — so a clean cycle always leaves the conn on a frame boundary and a
// pooled conn cannot mis-frame. CallWithBody's resp.ID==req.ID check is extra
// belt over the same framing; CallFlatBuffer's request carries no app-stamped ID
// to echo, so it relies on the framing guarantee alone.
func (t *TCPTransport) CallFlatBuffer(ctx context.Context, addr string, fw *FlatBuffersWriter) (*Message, error) {
	for attempt := 0; ; attempt++ {
		conn, reused, err := t.getDataConn(ctx, addr)
		if err != nil {
			return nil, err
		}
		resp, rerr := t.callFlatBufferOnce(ctx, addr, conn, fw)
		if rerr != nil && reused && attempt == 0 && errors.Is(rerr, errStalePreBody) {
			continue // stale reused conn discarded inside callFlatBufferOnce; dial fresh
		}
		return resp, rerr
	}
}

func (t *TCPTransport) callFlatBufferOnce(ctx context.Context, addr string, conn net.Conn, fw *FlatBuffersWriter) (*Message, error) {
	clean := false
	defer func() {
		// Pool only on a clean cycle with no ctx fire (matches callWithBodyOnce):
		// a checked-in conn must be frame-aligned for the next transfer.
		if clean && ctx.Err() == nil {
			_ = conn.SetDeadline(time.Time{})
			t.pool.checkin(addr, conn)
		} else {
			t.pool.discard(addr, conn)
		}
	}()
	stop := t.applyCtx(ctx, conn)
	defer stop()

	if err := t.codec.EncodeWriterTo(conn, fw); err != nil {
		// Pre-response failure: the request frame is untouched/undelivered (a
		// reused-but-dead conn fails the write fast), so retry on a fresh dial is
		// safe. errStalePreBody gates the single retry in CallFlatBuffer.
		return nil, fmt.Errorf("%w: encode flatbuffer to %s: %v", errStalePreBody, addr, err)
	}
	// No CloseWrite (framing invariant): self-delimiting frame, no body (see Call).
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

// CallPooled is Call over a pooled (reused) connection. Same clean request →
// framed response cycle as Call, but it checks a conn out of the data-plane pool
// and returns it only after a fully-clean cycle (so the conn stays frame-aligned
// for the next transfer). Used by hot-path control RPCs that are clean
// request/response with no body — e.g. ShardService.SendRequest, which forwards
// every PUT's index/group proposal to the leader; leaving that on Call made each
// forward pay a fresh TLS handshake. Framing safety + clean-cycle discipline are
// identical to CallFlatBuffer (length-prefixed BinaryCodec, no trailing bytes);
// no resp-ID desync guard for the same reason documented there. A reused conn
// that dies before the request frame is written is retried once on a fresh dial.
func (t *TCPTransport) CallPooled(ctx context.Context, addr string, req *Message) (*Message, error) {
	for attempt := 0; ; attempt++ {
		conn, reused, err := t.getDataConn(ctx, addr)
		if err != nil {
			return nil, err
		}
		resp, rerr := t.callPooledOnce(ctx, addr, conn, req)
		if rerr != nil && reused && attempt == 0 && errors.Is(rerr, errStalePreBody) {
			continue // stale reused conn discarded inside; dial fresh
		}
		return resp, rerr
	}
}

func (t *TCPTransport) callPooledOnce(ctx context.Context, addr string, conn net.Conn, req *Message) (*Message, error) {
	clean := false
	defer func() {
		if clean && ctx.Err() == nil {
			_ = conn.SetDeadline(time.Time{})
			t.pool.checkin(addr, conn)
		} else {
			t.pool.discard(addr, conn)
		}
	}()
	stop := t.applyCtx(ctx, conn)
	defer stop()

	if err := t.codec.Encode(conn, req); err != nil {
		// Pre-response failure: nothing useful delivered (a reused-but-dead conn
		// fails the write fast) → safe to retry once on a fresh dial.
		return nil, fmt.Errorf("%w: encode request to %s: %v", errStalePreBody, addr, err)
	}
	resp, err := t.codec.Decode(conn)
	if err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}
	resp, err = checkResponseStatus(addr, resp)
	if err != nil {
		clean = true // complete frame, no body → conn clean/reusable
		return nil, err
	}
	clean = true
	return resp, nil
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
