package transport

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTCPMux_SessionInitRoundTrip(t *testing.T) {
	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()

	sid := newMuxSessionID()
	go func() {
		require.NoError(t, writeMuxSessionInit(c, sid))
		// A raft frame would follow on the wire; emulate one trailing byte.
		_, _ = c.Write([]byte{0xAB})
	}()

	require.NoError(t, s.SetReadDeadline(time.Now().Add(2*time.Second)))
	got, err := readMuxSessionInit(s)
	require.NoError(t, err)
	assert.Equal(t, sid, got)

	// CRITICAL (gate-check #2): the init read must NOT have consumed the trailing
	// byte — it belongs to the next (raft) frame.
	one := make([]byte, 1)
	require.NoError(t, s.SetReadDeadline(time.Now().Add(2*time.Second)))
	n, err := s.Read(one)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assert.Equal(t, byte(0xAB), one[0])
}

func TestTCPMux_SessionInitBadMagic(t *testing.T) {
	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()
	go func() { _, _ = c.Write(bytes.Repeat([]byte{0xFF}, muxSessionInitLen)) }()
	require.NoError(t, s.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err := readMuxSessionInit(s)
	assert.Error(t, err)
}

func TestTCPMux_SessionIDsUnique(t *testing.T) {
	a := newMuxSessionID()
	b := newMuxSessionID()
	assert.NotEqual(t, a, b)
	assert.NotEqual(t, muxSessionID{}, a)
}

func TestTCPMux_ServerAdvertisesBothALPNs(t *testing.T) {
	tr := MustNewTCPTransport("alpn-key")
	defer tr.Close()
	assert.Equal(t, []string{tcpMuxALPN, tcpALPN}, tr.serverTLS.NextProtos)
	assert.Equal(t, []string{tcpALPN}, tr.clientTLS.NextProtos)       // gate-check #1
	assert.Equal(t, []string{tcpMuxALPN}, tr.muxClientTLS.NextProtos) // gate-check #1
}

func TestTCPMux_OutboundCarrier_OpenAndClose(t *testing.T) {
	const psk = "mux-out-key"
	srv := startTCP(t, psk)
	accepted := make(chan io.ReadWriteCloser, 8)
	srv.SetMuxConnHandler(func(ctx context.Context, carrier MuxCarrier) {
		for {
			s, err := carrier.AcceptStream(ctx)
			if err != nil {
				return
			}
			accepted <- s
		}
	})

	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	carrier, err := cli.GetOrConnectMux(ctx, srv.LocalAddr())
	require.NoError(t, err)
	require.NotNil(t, carrier)

	s1, err := carrier.OpenStream(ctx)
	require.NoError(t, err)
	s2, err := carrier.OpenStream(ctx)
	require.NoError(t, err)

	// Both reached the SAME inbound carrier (one session) → 2 accepted streams.
	for i := 0; i < 2; i++ {
		select {
		case <-accepted:
		case <-time.After(3 * time.Second):
			t.Fatalf("acceptor did not receive stream %d", i+1)
		}
	}

	// AcceptStream on an OUTBOUND carrier is unsupported.
	_, err = carrier.AcceptStream(ctx)
	assert.Error(t, err)

	// Close tears down both dialed conns.
	require.NoError(t, carrier.Close(nil))
	_, err = s1.Write([]byte{1})
	assert.Error(t, err)
	_, err = s2.Write([]byte{1})
	assert.Error(t, err)
}

func TestTCPMux_GetOrConnectMux_FreshEachCall(t *testing.T) {
	const psk = "mux-fresh-key"
	srv := startTCP(t, psk)
	srv.SetMuxConnHandler(func(ctx context.Context, c MuxCarrier) {})
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })

	ctx := context.Background()
	a, err := cli.GetOrConnectMux(ctx, srv.LocalAddr())
	require.NoError(t, err)
	b, err := cli.GetOrConnectMux(ctx, srv.LocalAddr())
	require.NoError(t, err)
	// Fresh carrier per call (race-loser owns its own conns; no addr-cache sharing).
	assert.NotSame(t, a, b)

	// EvictMux drops a from the reap-set; Close must not double-close it.
	cli.EvictMux(srv.LocalAddr(), a)
	require.NoError(t, a.Close(nil))
}

func TestTCPMux_InboundDemux_OneCarrierPerSession(t *testing.T) {
	const psk = "mux-demux-key"
	srv := startTCP(t, psk)
	var handlerCalls int32
	streamCh := make(chan io.ReadWriteCloser, 8)
	srv.SetMuxConnHandler(func(ctx context.Context, carrier MuxCarrier) {
		atomic.AddInt32(&handlerCalls, 1)
		for {
			s, err := carrier.AcceptStream(ctx)
			if err != nil {
				return
			}
			streamCh <- s
		}
	})

	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	carrier, err := cli.GetOrConnectMux(ctx, srv.LocalAddr())
	require.NoError(t, err)
	_, err = carrier.OpenStream(ctx) // conn 1, session S
	require.NoError(t, err)
	_, err = carrier.OpenStream(ctx) // conn 2, SAME session S
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		select {
		case <-streamCh:
		case <-time.After(3 * time.Second):
			t.Fatalf("stream %d not delivered", i+1)
		}
	}
	// Gate-check #6: ONE carrier (=> handler called once) for the two same-session conns.
	time.Sleep(100 * time.Millisecond) // allow a spurious 2nd handler call to surface
	assert.Equal(t, int32(1), atomic.LoadInt32(&handlerCalls))
}

func TestTCPMux_DataPlaneConnStillNonMux(t *testing.T) {
	const psk = "mux-neutral-key"
	srv := startTCP(t, psk)
	srv.SetMuxConnHandler(func(ctx context.Context, c MuxCarrier) {
		t.Error("data-plane conn routed to mux handler")
	})
	srv.Handle(StreamAdmin, func(req *Message) *Message { return NewResponse(req, []byte("ok")) })

	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Call(ctx, srv.LocalAddr(), &Message{Type: StreamAdmin, Payload: []byte("x")})
	require.NoError(t, err)
	assert.Equal(t, StatusOK, resp.Status) // gate-check #5: data-plane path unaffected
}

func TestTCPMux_InboundNoSessionInitDropped(t *testing.T) {
	const psk = "mux-noinit-key"
	// Config must be set BEFORE Listen (setConfigForTest rebuilds cfg-derived state,
	// racy against a running acceptLoop), so build the server manually.
	srv := MustNewTCPTransport(psk)
	srv.setConfigForTest(TCPTransportConfig{ServerIdleTimeout: 150 * time.Millisecond})
	srv.SetMuxConnHandler(func(ctx context.Context, c MuxCarrier) {
		t.Error("handler fired without session-init")
	})
	require.NoError(t, srv.Listen(context.Background(), "127.0.0.1:0"))
	t.Cleanup(func() { _ = srv.Close() })

	// Hand-dial a mux-ALPN conn but never send session-init.
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := cli.dialMux(ctx, srv.LocalAddr())
	require.NoError(t, err)
	defer conn.Close()
	// The server's session-init read deadline (ServerIdleTimeout) must drop this conn.
	buf := make([]byte, 1)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, rerr := conn.Read(buf)
	assert.Error(t, rerr) // server closed the conn after the init deadline
}

var _ = io.EOF
