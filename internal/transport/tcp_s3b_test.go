package transport

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setConfigForTest overrides the resource/pool config before Listen. Test-only.
func (t *TCPTransport) setConfigForTest(c TCPTransportConfig) {
	t.applyConfig(c.withDefaults())
}

// acceptedConnCount reports how many accepted (in-flight) conns the server tracks.
func (t *TCPTransport) acceptedConnCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.conns)
}

// listenTCP brings up a transport with an explicit config, listening on loopback.
func listenTCP(t *testing.T, psk string, cfg TCPTransportConfig) *TCPTransport {
	t.Helper()
	tr := MustNewTCPTransport(psk)
	tr.setConfigForTest(cfg)
	require.NoError(t, tr.Listen(context.Background(), "127.0.0.1:0"))
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

// --- Task 0: config defaults ------------------------------------------------

func TestTCPConfig_Defaults(t *testing.T) {
	tr := MustNewTCPTransport("cfg")
	t.Cleanup(func() { _ = tr.Close() })
	assert.Equal(t, defaultServerIdleTimeout, tr.cfg.ServerIdleTimeout)
	assert.Equal(t, defaultServerBodyTimeout, tr.cfg.ServerBodyTimeout)
	assert.Equal(t, defaultMaxConnsPerPeer, tr.cfg.MaxConnsPerPeer)
	assert.Equal(t, defaultPoolIdleTimeout, tr.cfg.PoolIdleTimeout)
}

// --- Task 1: server read deadline -------------------------------------------

func TestTCPDeadline_IdlePooledConnReaped(t *testing.T) {
	srv := listenTCP(t, "idle", TCPTransportConfig{ServerIdleTimeout: 150 * time.Millisecond})
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		_, _ = io.Copy(io.Discard, body)
		return NewResponse(req, nil)
	})
	cli := MustNewTCPTransport("idle")
	t.Cleanup(func() { _ = cli.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := cli.CallWithBody(ctx, srv.LocalAddr(), &Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("x")))
	require.NoError(t, err)
	// The server conn for that transfer is pooled on the client; idle on the server.
	// The server's idle read deadline must reap it (untrackConn) within idle+slack.
	require.Eventually(t, func() bool { return srv.acceptedConnCount() == 0 },
		1*time.Second, 20*time.Millisecond, "idle server conn must be reaped by the read deadline")
}

// infiniteReader always returns data and never EOFs, so writeChunkedBody keeps
// writing until the (non-reading) client's recv buffer fills and the server's
// Write blocks — guaranteeing the write deadline is the thing that frees it.
type infiniteReader struct{}

func (infiniteReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

func TestTCPDeadline_SlowResponseReaderReaped(t *testing.T) {
	srv := listenTCP(t, "slowread", TCPTransportConfig{ServerBodyTimeout: 150 * time.Millisecond})
	srv.HandleRead(StreamShardReadBody, func(req *Message) (*Message, io.ReadCloser) {
		return NewResponse(req, nil), io.NopCloser(infiniteReader{})
	})
	cli := MustNewTCPTransport("slowread")
	t.Cleanup(func() { _ = cli.Close() })
	resp, body, err := cli.CallRead(context.Background(), srv.LocalAddr(), &Message{Type: StreamShardReadBody})
	require.NoError(t, err)
	require.NotNil(t, resp)
	// Deliberately do NOT read body: the server blocks in writeChunkedBody until the
	// write deadline trips, drops the conn, and untracks it.
	require.Eventually(t, func() bool { return srv.acceptedConnCount() == 0 },
		1*time.Second, 20*time.Millisecond, "slow response reader must be reaped by the write deadline")
	_ = body.Close()
}

// --- Task 2: Close reaps in-flight conns ------------------------------------

func TestTCPClose_ReapsInFlightConns(t *testing.T) {
	srv := MustNewTCPTransport("reap")
	require.NoError(t, srv.Listen(context.Background(), "127.0.0.1:0"))
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		_, _ = io.Copy(io.Discard, body)
		return NewResponse(req, nil)
	})
	cli := MustNewTCPTransport("reap")
	t.Cleanup(func() { _ = cli.Close() })
	// Pool an idle server-side conn (the server goroutine blocks in Decode).
	_, err := cli.CallWithBody(context.Background(), srv.LocalAddr(), &Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("x")))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return srv.acceptedConnCount() >= 1 }, time.Second, 10*time.Millisecond)
	require.NoError(t, srv.Close())
	assert.Eventually(t, func() bool { return srv.acceptedConnCount() == 0 }, time.Second, 10*time.Millisecond,
		"Close must reap accepted conns")
}

// --- Task 3: desync detection -----------------------------------------------

func TestTCPDesync_ServerEchoesRequestID(t *testing.T) {
	srv := startTCP(t, "echo")
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		_, _ = io.Copy(io.Discard, body)
		return &Message{Type: StreamData} // ID=0, mimics shard okResponse (no req echo)
	})
	cli := MustNewTCPTransport("echo")
	t.Cleanup(func() { _ = cli.Close() })
	_, err := cli.CallWithBody(context.Background(), srv.LocalAddr(),
		&Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("a")))
	require.NoError(t, err, "serveOne must echo req.ID so the client's verify passes")
}

func TestTCPDesync_HappyPathStampsAndMatches(t *testing.T) {
	srv := startTCP(t, "desync")
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		_, _ = io.Copy(io.Discard, body)
		return NewResponse(req, []byte("ok"))
	})
	cli := MustNewTCPTransport("desync")
	t.Cleanup(func() { _ = cli.Close() })
	// Two sequential transfers reuse the pooled conn; both must match their stamped IDs.
	for i := 0; i < 2; i++ {
		resp, err := cli.CallWithBody(context.Background(), srv.LocalAddr(),
			&Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("a")))
		require.NoError(t, err)
		require.Equal(t, []byte("ok"), resp.Payload)
	}
}

// --- Task 4: nil-resp semantics ---------------------------------------------

func TestTCPNilResp_BodyHandlerReturnsError(t *testing.T) {
	srv := startTCP(t, "nilresp")
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		_, _ = io.Copy(io.Discard, body)
		return nil // misbehaving handler
	})
	cli := MustNewTCPTransport("nilresp")
	t.Cleanup(func() { _ = cli.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := cli.CallWithBody(ctx, srv.LocalAddr(), &Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("x")))
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
}

func TestTCPNilResp_TypeHandlerReturnsError(t *testing.T) {
	srv := startTCP(t, "nilresp2")
	srv.Handle(StreamData, func(req *Message) *Message { return nil })
	cli := MustNewTCPTransport("nilresp2")
	t.Cleanup(func() { _ = cli.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := cli.Call(ctx, srv.LocalAddr(), &Message{Type: StreamData})
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
}

// --- Task 6: socket tuning --------------------------------------------------

func TestTCPTune_AppliedOnDialAndAccept(t *testing.T) {
	cfg := TCPTransportConfig{ReadBufferBytes: 256 << 10, WriteBufferBytes: 256 << 10}
	srv := listenTCP(t, "tune", cfg)
	srv.HandleBody(StreamShardWriteBody, func(req *Message, b io.Reader) *Message {
		_, _ = io.Copy(io.Discard, b)
		return NewResponse(req, nil)
	})
	cli := MustNewTCPTransport("tune")
	cli.setConfigForTest(cfg)
	t.Cleanup(func() { _ = cli.Close() })
	_, err := cli.CallWithBody(context.Background(), srv.LocalAddr(),
		&Message{Type: StreamShardWriteBody}, bytes.NewReader(make([]byte, 1<<20)))
	require.NoError(t, err) // tuned conns still transfer correctly
}

// --- Task 7: admission control ----------------------------------------------

func TestTCPAdmission_OverloadedWhenDataClassSaturated(t *testing.T) {
	srv := listenTCP(t, "adm", TCPTransportConfig{TrafficLimits: TrafficLimits{Data: 1}})
	entered := make(chan struct{})
	hold := make(chan struct{})
	var once sync.Once
	srv.Handle(StreamData, func(req *Message) *Message {
		once.Do(func() { close(entered) })
		<-hold // block, holding the single Data-class permit
		return NewResponse(req, nil)
	})
	defer close(hold)
	cli := MustNewTCPTransport("adm")
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()
	// First RPC acquires the permit and blocks in the handler.
	go func() { _, _ = cli.Call(context.Background(), addr, &Message{Type: StreamData}) }()
	<-entered
	// Second RPC cannot get a Data permit within the bulk acquire timeout → overloaded.
	_, err := cli.Call(context.Background(), addr, &Message{Type: StreamData})
	require.Error(t, err)
	require.Contains(t, err.Error(), "overload")
}

// --- Task 8: retry-once on a stale reused conn ------------------------------

// breakPooledConnForTest closes the single idle pooled conn for addr WITHOUT
// removing it from the pool, simulating a server-side reap between transfers.
func breakPooledConnForTest(t *testing.T, cli *TCPTransport, addr string) {
	t.Helper()
	cli.pool.mu.Lock()
	defer cli.pool.mu.Unlock()
	q := cli.pool.idle[addr]
	require.NotEmpty(t, q, "expected a pooled conn to break")
	_ = q[len(q)-1].c.Close()
}

func TestTCPPool_RetryOnceOnStaleReusedConn(t *testing.T) {
	srv := startTCP(t, "stale")
	srv.HandleBody(StreamShardWriteBody, func(req *Message, b io.Reader) *Message {
		_, _ = io.Copy(io.Discard, b)
		return NewResponse(req, []byte("ok"))
	})
	cli := MustNewTCPTransport("stale")
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()
	_, err := cli.CallWithBody(context.Background(), addr, &Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("a")))
	require.NoError(t, err)
	require.Equal(t, 1, poolLen(cli, addr))
	breakPooledConnForTest(t, cli, addr)
	// Transfer 2 checks out the dead conn; its first Encode fails → retry-once dials fresh.
	resp, err := cli.CallWithBody(context.Background(), addr, &Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("b")))
	require.NoError(t, err, "a stale reused conn must be retried once on a fresh dial")
	require.Equal(t, []byte("ok"), resp.Payload)
}
