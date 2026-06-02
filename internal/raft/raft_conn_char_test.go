package raft

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/transport"
)

// quicCarrierForTest adapts a raw *quic.Conn to transport.MuxCarrier for the
// characterization harness (the production quicMuxCarrier is unexported in the
// transport package). It mirrors quicMuxCarrier exactly.
type quicCarrierForTest struct{ conn *quic.Conn }

func (c quicCarrierForTest) OpenStream(ctx context.Context) (io.ReadWriteCloser, error) {
	return c.conn.OpenStreamSync(ctx)
}
func (c quicCarrierForTest) AcceptStream(ctx context.Context) (io.ReadWriteCloser, error) {
	return c.conn.AcceptStream(ctx)
}
func (c quicCarrierForTest) RemoteAddr() string { return c.conn.RemoteAddr().String() }
func (c quicCarrierForTest) Close(cause error) error {
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	return c.conn.CloseWithError(0, msg)
}

var _ transport.MuxCarrier = quicCarrierForTest{}

// newRaftConnPairQUIC brings up a QUIC loopback pair and builds a client
// (dialer) + server (acceptor) RaftConn with the given handler configs.
// TASK 0 form: uses the CURRENT NewRaftConn(conn,cfg)+Open/AcceptInboundStreams
// API. Task 1 updates ONLY this helper to the carrier-agnostic API; the test
// bodies below (the behavioral assertions) do not change.
func newRaftConnPairQUIC(t *testing.T, poolSize int, clientCfg, serverCfg RaftConnConfig) (*RaftConn, *RaftConn) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	listener, err := quic.ListenAddr("127.0.0.1:0", benchmarkRaftTLSConfig(t, true), benchmarkRaftQUICConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan *quic.Conn, 1)
	go func() {
		conn, aerr := listener.Accept(ctx)
		if aerr == nil {
			accepted <- conn
		} else {
			close(accepted)
		}
	}()

	clientConn, err := quic.DialAddr(ctx, listener.Addr().String(), benchmarkRaftTLSConfig(t, false), benchmarkRaftQUICConfig())
	require.NoError(t, err)

	serverConn := <-accepted
	require.NotNil(t, serverConn)

	clientStreams, err := openMuxStreams(ctx, quicCarrierForTest{clientConn}, poolSize)
	require.NoError(t, err)
	serverStreams, err := acceptMuxStreams(ctx, quicCarrierForTest{serverConn}, poolSize)
	require.NoError(t, err)
	client := NewRaftConn(clientConn.RemoteAddr().String(), clientStreams,
		func(c error) error { return clientConn.CloseWithError(0, c.Error()) }, clientCfg)
	server := NewRaftConn(serverConn.RemoteAddr().String(), serverStreams,
		func(c error) error { return serverConn.CloseWithError(0, c.Error()) }, serverCfg)
	client.StartReaders()
	server.StartReaders()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return client, server
}

// newRaftConnPairTCP brings up two real TCPTransports and builds a client+server
// RaftConn over a TCP MuxCarrier (S2b-2). This is the deferred S2b-1 proof that the
// carrier abstraction is genuinely carrier-agnostic: a non-QUIC carrier yields N
// streams the same way. The dialer flow mirrors GroupRaftQUICMux.muxConnFor; the
// acceptor flow mirrors handleInboundMuxConn.
func newRaftConnPairTCP(t *testing.T, poolSize int, clientCfg, serverCfg RaftConnConfig) (*RaftConn, *RaftConn) {
	t.Helper()
	const psk = "raftconn-tcp-pair-key"
	srv := transport.MustNewTCPTransport(psk)
	require.NoError(t, srv.Listen(context.Background(), "127.0.0.1:0"))
	t.Cleanup(func() { _ = srv.Close() })

	serverRCCh := make(chan *RaftConn, 1)
	srv.SetMuxConnHandler(func(ctx context.Context, carrier transport.MuxCarrier) {
		streams, err := acceptMuxStreams(ctx, carrier, poolSize)
		if err != nil {
			return
		}
		rc := NewRaftConn(carrier.RemoteAddr(), streams,
			func(c error) error { return carrier.Close(c) }, serverCfg)
		rc.StartReaders()
		serverRCCh <- rc
	})

	cli := transport.MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	carrier, err := cli.GetOrConnectMux(ctx, srv.LocalAddr())
	require.NoError(t, err)
	streams, err := openMuxStreams(ctx, carrier, poolSize)
	require.NoError(t, err)
	client := NewRaftConn(carrier.RemoteAddr(), streams,
		func(c error) error { return carrier.Close(c) }, clientCfg)
	client.StartReaders()

	var server *RaftConn
	select {
	case server = <-serverRCCh:
	case <-time.After(3 * time.Second):
		t.Fatal("server RaftConn not established over TCP carrier")
	}
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return client, server
}

func TestRaftConnChar_TCP_CallRoundtrip(t *testing.T) {
	server := RaftConnConfig{RPCHandler: func(p []byte) ([]byte, error) {
		return append([]byte("echo:"), p...), nil
	}}
	client, _ := newRaftConnPairTCP(t, 4, RaftConnConfig{}, server)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Call(ctx, []byte("ping"))
	require.NoError(t, err)
	assert.Equal(t, "echo:ping", string(resp))
}

func TestRaftConnChar_TCP_ConcurrentCorrID(t *testing.T) {
	server := RaftConnConfig{RPCHandler: func(p []byte) ([]byte, error) {
		return append([]byte("echo:"), p...), nil
	}}
	client, _ := newRaftConnPairTCP(t, 4, RaftConnConfig{}, server)

	const n = 50
	var wg sync.WaitGroup
	errs := make([]error, n)
	resps := make([][]byte, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resps[i], errs[i] = client.Call(ctx, []byte(fmt.Sprintf("m%d", i)))
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		require.NoErrorf(t, errs[i], "call %d", i)
		assert.Equalf(t, fmt.Sprintf("echo:m%d", i), string(resps[i]), "call %d", i)
	}
}

func TestRaftConnChar_CallRoundtrip(t *testing.T) {
	server := RaftConnConfig{RPCHandler: func(p []byte) ([]byte, error) {
		return append([]byte("echo:"), p...), nil
	}}
	client, _ := newRaftConnPairQUIC(t, 4, RaftConnConfig{}, server)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Call(ctx, []byte("ping"))
	require.NoError(t, err)
	assert.Equal(t, "echo:ping", string(resp))
}

func TestRaftConnChar_ConcurrentCorrID(t *testing.T) {
	server := RaftConnConfig{RPCHandler: func(p []byte) ([]byte, error) {
		return append([]byte("echo:"), p...), nil
	}}
	client, _ := newRaftConnPairQUIC(t, 4, RaftConnConfig{}, server)

	const n = 50
	var wg sync.WaitGroup
	errs := make([]error, n)
	resps := make([][]byte, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resps[i], errs[i] = client.Call(ctx, []byte(fmt.Sprintf("m%d", i)))
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		require.NoErrorf(t, errs[i], "call %d", i)
		assert.Equalf(t, fmt.Sprintf("echo:m%d", i), string(resps[i]), "call %d corrID dispatch", i)
	}
}

func TestRaftConnChar_MarkBrokenFansOut(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	entered := make(chan struct{})
	var once sync.Once
	server := RaftConnConfig{RPCHandler: func(p []byte) ([]byte, error) {
		once.Do(func() { close(entered) }) // request reached server => client Call is in pending
		<-block                            // never responds until cleanup
		return p, nil
	}}
	client, serverRC := newRaftConnPairQUIC(t, 4, RaftConnConfig{}, server)

	errCh := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), []byte("x"))
		errCh <- err
	}()
	<-entered            // deterministic: pending.Store precedes sendFrame, so registration is done
	_ = serverRC.Close() // break the conn => client readLoop EOF => markBroken fans out

	select {
	case err := <-errCh:
		require.Error(t, err) // pending call must surface a broken-conn error
	case <-time.After(3 * time.Second):
		t.Fatal("pending call did not error after the conn broke")
	}
}

func TestRaftConnChar_HeartbeatBatchReply(t *testing.T) {
	server := RaftConnConfig{HBBatchHandler: func(p []byte) []byte {
		return append([]byte("reply:"), p...)
	}}
	replyCh := make(chan struct{}, 1)
	var gotCorr uint64
	var gotPayload []byte
	client := RaftConnConfig{HBReplyHandler: func(corrID uint64, payload []byte) {
		gotCorr = corrID
		gotPayload = payload
		replyCh <- struct{}{}
	}}
	clientRC, _ := newRaftConnPairQUIC(t, 4, client, server)

	corr := clientRC.NextHeartbeatCorrID()
	require.NoError(t, clientRC.SendHeartbeatBatchWithCorrID(corr, []byte("hb")))
	select {
	case <-replyCh: // channel send happens-after the field writes
		assert.Equal(t, corr, gotCorr)
		assert.Equal(t, "reply:hb", string(gotPayload))
	case <-time.After(3 * time.Second):
		t.Fatal("no heartbeat reply received")
	}
}

func TestRaftConnChar_HandlerOverload(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	entered := make(chan struct{})
	var once sync.Once
	server := RaftConnConfig{
		HandlerPoolSize: 1,
		RPCHandler: func(p []byte) ([]byte, error) {
			once.Do(func() { close(entered) }) // the one handler slot is acquired before the handler runs
			<-block
			return p, nil
		},
	}
	client, _ := newRaftConnPairQUIC(t, 1, RaftConnConfig{}, server)

	// First call occupies the single handler slot and blocks.
	go func() { _, _ = client.Call(context.Background(), []byte("a")) }()
	<-entered // deterministic: the single handler slot is now held

	// Second call must be rejected with the overloaded error from the server.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := client.Call(ctx, []byte("b"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overloaded")
}
