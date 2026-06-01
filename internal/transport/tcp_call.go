package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

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
			_ = conn.Close()
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

// CallWithBody dials, writes the request frame, streams body behind it, half-
// closes (so the server's body reader sees io.EOF), then reads the framed response.
func (t *TCPTransport) CallWithBody(ctx context.Context, addr string, req *Message, body io.Reader) (*Message, error) {
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
	if body != nil {
		if _, err := io.Copy(conn, body); err != nil {
			return nil, fmt.Errorf("stream body to %s: %w", addr, err)
		}
	}
	// Legitimate CloseWrite (framing invariant): delimits the EOF-terminated
	// upload body so the server's body handler sees io.EOF. The server reads the
	// body to EOF (consuming this close_notify) before responding, so no unread
	// data forces an RST.
	if err := conn.CloseWrite(); err != nil {
		return nil, fmt.Errorf("close write to %s: %w", addr, err)
	}
	resp, err := t.codec.Decode(conn)
	if err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}
	return checkResponseStatus(addr, resp)
}

// tcpReadCloser exposes the response-body bytes remaining on the conn after the
// metadata frame. Close closes the underlying conn (connection-per-RPC), once.
type tcpReadCloser struct {
	conn net.Conn
	once sync.Once
}

func (r *tcpReadCloser) Read(p []byte) (int, error) { return r.conn.Read(p) }
func (r *tcpReadCloser) Close() error {
	var err error
	r.once.Do(func() { err = r.conn.Close() })
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
	conn, err := t.dial(ctx, addr)
	if err != nil {
		return nil, nil, err
	}
	// On any error before returning the body, close the conn; on success the
	// caller owns it via tcpReadCloser.
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
	// No CloseWrite on the request (framing invariant): it is a self-delimiting
	// frame with no body. Sending close_notify would leave unread data in the
	// server's buffer => RST on the server's close, truncating the response body.
	// The SERVER CloseWrites after the body so this client sees a clean io.EOF.
	resp, err := t.codec.Decode(conn)
	if err != nil {
		stop()
		return nil, nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}
	if resp, err = checkResponseStatus(addr, resp); err != nil {
		stop()
		return nil, nil, err
	}
	// Hand the conn to the caller. Release the ctx watcher (stop) and clear the
	// dial deadline so the body read is unbounded by the RPC ctx (matches QUIC,
	// whose body read is also unbounded after a successful CallRead return). The
	// body lifetime is owned by the caller's Close.
	stop()
	_ = conn.SetDeadline(time.Time{})
	ok = true
	return resp, &tcpReadCloser{conn: conn}, nil
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
