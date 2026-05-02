package transport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQUICTransport_ALPNRouting verifies that the listener accepts both legacy
// and mux ALPNs and dispatches connections to the correct handler based on the
// negotiated protocol.
func TestQUICTransport_ALPNRouting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := NewQUICTransport("test-psk")
	defer server.Close()

	var muxCalls atomic.Int64
	server.SetMuxConnHandler(func(conn *quic.Conn) {
		muxCalls.Add(1)
		// Accept one stream so the dialer's OpenStreamSync returns; no further
		// processing needed for this test.
		s, err := conn.AcceptStream(context.Background())
		if err == nil {
			_ = s.Close()
		}
	})

	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))

	// Legacy client (Connect uses legacy ALPN).
	legacyClient := NewQUICTransport("test-psk")
	defer legacyClient.Close()
	require.NoError(t, legacyClient.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, legacyClient.Connect(ctx, server.LocalAddr()))

	msg := &Message{Type: StreamControl, Payload: []byte("hello")}
	require.NoError(t, legacyClient.Send(ctx, server.LocalAddr(), msg))

	select {
	case recv := <-server.Receive():
		assert.Equal(t, "hello", string(recv.Message.Payload))
	case <-time.After(2 * time.Second):
		t.Fatal("legacy message not received")
	}

	// Mux client (GetOrConnectMux uses mux ALPN).
	muxClient := NewQUICTransport("test-psk")
	defer muxClient.Close()
	require.NoError(t, muxClient.Listen(ctx, "127.0.0.1:0"))

	conn, err := muxClient.GetOrConnectMux(ctx, server.LocalAddr())
	require.NoError(t, err)
	require.NotNil(t, conn)
	// Server's mux handler accepts a stream; trigger that here so the test
	// observes the handler running.
	stream, err := conn.OpenStreamSync(ctx)
	require.NoError(t, err)
	_, _ = stream.Write([]byte{0})

	// Wait briefly for handler to run.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if muxCalls.Load() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(t, int64(1), muxCalls.Load(), "mux handler should run for mux conn")
}

// TestQUICTransport_MuxRejectedWithoutHandler verifies that mux connections
// are rejected (closed) when no muxHandler is registered. This protects
// servers that have not opted into the mux feature from accidentally receiving
// mux connections.
func TestQUICTransport_MuxRejectedWithoutHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := NewQUICTransport("test-psk")
	defer server.Close()
	// No SetMuxConnHandler — server does not accept mux.
	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))

	client := NewQUICTransport("test-psk")
	defer client.Close()
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))

	// Mux dial succeeds (TLS handshake completes — server advertises both ALPNs).
	conn, err := client.GetOrConnectMux(ctx, server.LocalAddr())
	require.NoError(t, err)

	// But the connection is closed by the server right after handshake.
	// Detect by trying to open a stream.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := conn.OpenStreamSync(ctx)
		if err != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	_, err = conn.OpenStreamSync(ctx)
	assert.Error(t, err, "expected stream open to fail after server rejects mux")
}

// TestQUICTransport_MuxALPNFormat verifies the mux ALPN includes the PSK hash.
func TestQUICTransport_MuxALPNFormat(t *testing.T) {
	t1 := NewQUICTransport("psk-A")
	t2 := NewQUICTransport("psk-B")
	t3 := NewQUICTransport("")

	// Same PSK → same ALPN.
	t1b := NewQUICTransport("psk-A")
	assert.Equal(t, t1.MuxALPN(), t1b.MuxALPN())

	// Different PSK → different ALPN.
	assert.NotEqual(t, t1.MuxALPN(), t2.MuxALPN())

	// Empty PSK → "grainfs-mux-v1".
	assert.Equal(t, "grainfs-mux-v1", t3.MuxALPN())

	// Mux ALPN starts with "grainfs-mux-v1-" for non-empty PSK.
	assert.True(t, len(t1.MuxALPN()) > len("grainfs-mux-v1-"))
}

// TestQUICTransport_PSKMismatch_Mux verifies that two transports with different
// PSKs cannot establish a mux connection (TLS ALPN mismatch).
func TestQUICTransport_PSKMismatch_Mux(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := NewQUICTransport("psk-server")
	defer server.Close()
	server.SetMuxConnHandler(func(conn *quic.Conn) {})
	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))

	client := NewQUICTransport("psk-client")
	defer client.Close()
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))

	dialCtx, dialCancel := context.WithTimeout(ctx, 2*time.Second)
	defer dialCancel()
	_, err := client.GetOrConnectMux(dialCtx, server.LocalAddr())
	assert.Error(t, err, "expected dial to fail with PSK mismatch")
}
