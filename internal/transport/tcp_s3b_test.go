package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
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
	assert.Equal(t, defaultClientBodyTimeout, tr.cfg.ClientBodyTimeout)
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

// TestTCPDeadline_ActiveConnNotKilledByStaleWriteDeadline is the code-gate BLOCKER
// guard: the handshake SetDeadline arms an ABSOLUTE write deadline, and the loop
// re-arms only the read deadline. A conn actively reused past that instant must NOT
// have its response Encode (a write) killed by the stale handshake write deadline.
func TestTCPDeadline_ActiveConnNotKilledByStaleWriteDeadline(t *testing.T) {
	srv := listenTCP(t, "wdl", TCPTransportConfig{
		ServerIdleTimeout: 250 * time.Millisecond, // short, so the bug (if present) fires fast
		ServerBodyTimeout: 10 * time.Second,
	})
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		_, _ = io.Copy(io.Discard, body)
		return NewResponse(req, []byte("ok"))
	})
	cli := MustNewTCPTransport("wdl")
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()
	// Back-to-back transfers, each gap < idle timeout (conn stays active, read
	// deadline never trips) but total wall-clock > idle timeout. With the stale
	// write-deadline bug, the server's response Encode fails once age > 250ms.
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		resp, err := cli.CallWithBody(context.Background(), addr, &Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("x")))
		require.NoError(t, err, "an actively-reused conn must not be killed by a stale server write deadline")
		require.Equal(t, []byte("ok"), resp.Payload)
		require.Equal(t, 1, poolLen(cli, addr), "the conn must stay pooled (reused), not churned")
		time.Sleep(60 * time.Millisecond)
	}
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

// TestTCPDesync_MismatchedRespIDIsCaught is the FIRING test for desync detection
// (the prevention header-flag was dropped because this mechanism makes a desync
// loud — so the mechanism doing its job must be tested, not just the happy path).
// A hand-rolled server echoes the WRONG id; the client must error loudly and
// discard the conn rather than pool a desynced one.
func TestTCPDesync_MismatchedRespIDIsCaught(t *testing.T) {
	const psk = "desync-fire"
	srvTr := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = srvTr.Close() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tln := tls.NewListener(ln, srvTr.buildServerTLS())
	t.Cleanup(func() { _ = tln.Close() })
	go func() {
		conn, aerr := tln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		codec := &BinaryCodec{}
		req, derr := codec.Decode(conn)
		if derr != nil {
			return
		}
		// Drain the chunked request body to its terminator so the frame is consumed.
		_, _ = io.Copy(io.Discard, &chunkedBodyReader{r: conn})
		// Respond with a deliberately mismatched ID → the client must flag a desync.
		_ = codec.Encode(conn, &Message{Type: req.Type, ID: req.ID + 1, Status: StatusOK})
	}()
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })
	addr := ln.Addr().String()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = cli.CallWithBody(ctx, addr, &Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("payload")))
	require.Error(t, err)
	require.Contains(t, err.Error(), "desync")
	require.Equal(t, 0, poolLen(cli, addr), "a desynced conn must be discarded, not pooled")
}

// TestTCPClientBody_StalledServerReadTimesOut is the QUIC-parity gap guard (TODOS
// S3b residual line 87): at body handoff CallRead clears the per-call deadline so the
// response-body read is unbounded by the RPC ctx (matches QUIC). The server side IS
// bounded (ServerBodyTimeout), but a server that sends the metadata frame then stalls
// mid-body (without closing) must NOT pin the client read goroutine + pooled slot
// forever. A short ClientBodyTimeout arms an idle read deadline that trips the stall.
func TestTCPClientBody_StalledServerReadTimesOut(t *testing.T) {
	const psk = "client-body-stall"
	srvTr := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = srvTr.Close() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tln := tls.NewListener(ln, srvTr.buildServerTLS())
	t.Cleanup(func() { _ = tln.Close() })

	stall := make(chan struct{})
	t.Cleanup(func() { close(stall) })
	go func() {
		conn, aerr := tln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		codec := &BinaryCodec{}
		req, derr := codec.Decode(conn)
		if derr != nil {
			return
		}
		// Metadata response (echo id) + ONE chunk header promising 64 bytes but only 8
		// sent, then stall forever (never terminator, never close).
		if eerr := codec.Encode(conn, &Message{Type: req.Type, ID: req.ID, Status: StatusOK}); eerr != nil {
			return
		}
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], 64)
		_, _ = conn.Write(hdr[:])
		_, _ = conn.Write(make([]byte, 8))
		<-stall // hold the conn open without making progress
	}()

	cli := MustNewTCPTransport(psk)
	cli.setConfigForTest(TCPTransportConfig{ClientBodyTimeout: 150 * time.Millisecond})
	t.Cleanup(func() { _ = cli.Close() })

	resp, body, err := cli.CallRead(context.Background(), ln.Addr().String(), &Message{Type: StreamShardReadBody})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Draining the stalled body must error (timeout) rather than block forever.
	done := make(chan error, 1)
	go func() {
		_, derr := io.Copy(io.Discard, body)
		done <- derr
	}()
	select {
	case derr := <-done:
		require.Error(t, derr, "a stalled mid-body server must trip the client idle read deadline")
	case <-time.After(2 * time.Second):
		t.Fatal("client body read blocked past the idle deadline (goroutine + pooled slot pinned)")
	}
	_ = body.Close()
}

// TestTCPClientBody_CleanDrainClearsDeadlineBeforeReuse is the FIRING guard for the
// clean-branch clear in tcpReadCloser.Close: a fully-drained body-read conn carries an
// absolute read deadline (T_lastread + ClientBodyTimeout) from the per-Read idle
// arming, and Close MUST clear it before pool checkin. The clear is correctness-
// load-bearing (not mere hygiene): a CallRead→CallWithBody reuse writes its body first
// (writes ignore a stale READ deadline), then the response Decode hits the expired
// deadline — a plain error that is NOT errStalePreBody, so it is NOT retried. With
// ClientBodyTimeout < PoolIdleTimeout the conn is reused after its deadline expires but
// before idle eviction, so removing the clear makes this RED.
func TestTCPClientBody_CleanDrainClearsDeadlineBeforeReuse(t *testing.T) {
	srv := startTCP(t, "bodyclear")
	srv.HandleRead(StreamShardReadBody, func(req *Message) (*Message, io.ReadCloser) {
		return NewResponse(req, nil), io.NopCloser(bytes.NewReader([]byte("hello")))
	})
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		_, _ = io.Copy(io.Discard, body)
		return NewResponse(req, []byte("ok"))
	})
	cli := MustNewTCPTransport("bodyclear")
	cli.setConfigForTest(TCPTransportConfig{ClientBodyTimeout: 50 * time.Millisecond})
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()

	// 1) CallRead + full drain pools the conn carrying a read deadline of ~T+50ms.
	resp, body, err := cli.CallRead(context.Background(), addr, &Message{Type: StreamShardReadBody})
	require.NoError(t, err)
	require.NotNil(t, resp)
	_, err = io.Copy(io.Discard, body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, 1, poolLen(cli, addr), "the cleanly-drained conn must be pooled for reuse")

	// 2) Wait past ClientBodyTimeout (but under PoolIdleTimeout) so a NOT-cleared
	//    deadline is now in the past, then reuse the SAME conn via CallWithBody (a
	//    Background ctx is required — a ctx deadline would re-arm SetDeadline and mask
	//    the bug). Only the Close-time clear keeps the reuse alive.
	time.Sleep(150 * time.Millisecond)
	wresp, err := cli.CallWithBody(context.Background(), addr,
		&Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("x")))
	require.NoError(t, err, "a conn reused past ClientBodyTimeout must not fail on a stale read deadline")
	require.Equal(t, []byte("ok"), wresp.Payload)
	require.Equal(t, 1, poolLen(cli, addr), "the conn must stay pooled (reused, not churned)")
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

// countingReader counts how many times it is fully consumed (EOF returned). A
// wrongly-retried body would be read a second time → fullReads > 1.
type countingReader struct {
	data      []byte
	off       int
	fullReads int
}

func (c *countingReader) Read(p []byte) (int, error) {
	if c.off >= len(c.data) {
		c.fullReads++
		return 0, io.EOF
	}
	n := copy(p, c.data[c.off:])
	c.off += n
	return n, nil
}

// TestTCPPool_NoRetryAfterBodyConsumed guards the retry-once invariant's dangerous
// edge: a reused conn that fails AFTER the body has been written must NOT retry, or
// the (now-consumed) body would replay truncated. A persistent server serves one OK
// transfer (pooling the conn), then reads transfer 2's req+body and closes without
// responding → the client's post-body Decode fails on the reused conn. The body
// must have been consumed exactly once and no retry attempted.
func TestTCPPool_NoRetryAfterBodyConsumed(t *testing.T) {
	const psk = "noretry"
	srvTr := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = srvTr.Close() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tln := tls.NewListener(ln, srvTr.buildServerTLS())
	t.Cleanup(func() { _ = tln.Close() })
	go func() {
		conn, aerr := tln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		codec := &BinaryCodec{}
		// Transfer 1: respond OK (echoing the id) so the client pools the conn.
		req1, derr := codec.Decode(conn)
		if derr != nil {
			return
		}
		_, _ = io.Copy(io.Discard, &chunkedBodyReader{r: conn})
		_ = codec.Encode(conn, &Message{Type: req1.Type, ID: req1.ID, Status: StatusOK})
		// Transfer 2: read req + body, then close WITHOUT responding → client Decode fails.
		if _, derr = codec.Decode(conn); derr != nil {
			return
		}
		_, _ = io.Copy(io.Discard, &chunkedBodyReader{r: conn})
		// defer conn.Close() — no response
	}()
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })
	addr := ln.Addr().String()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = cli.CallWithBody(ctx, addr, &Message{Type: StreamShardWriteBody}, bytes.NewReader([]byte("a")))
	require.NoError(t, err) // transfer 1 pooled the conn
	require.Equal(t, 1, poolLen(cli, addr))

	body := &countingReader{data: []byte("payload")}
	_, err = cli.CallWithBody(ctx, addr, &Message{Type: StreamShardWriteBody}, body)
	require.Error(t, err)                          // post-body Decode failure on the reused conn
	require.NotContains(t, err.Error(), "context") // not a retry-induced hang to the ctx deadline
	require.Equal(t, 1, body.fullReads, "body must be consumed exactly once (no retry-replay)")
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
