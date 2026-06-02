package serveruntime

import (
	"context"
	"crypto/tls"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/transport"
)

// delayedReadStream wraps a join stream and delays ONLY the first read (the reply
// read in inviteJoinDialWith). This forces a deterministic ORDERING: the joiner
// reads the reply strictly AFTER the leader has written it, half-closed, and the
// listener's handleConn has drained + full-closed. It proves the consumer
// choreography survives reading past the leader's close — it does NOT, on its own,
// distinguish a clean FIN from an abortive RST (see the test's boundary note).
type delayedReadStream struct {
	inner io.ReadWriteCloser
	delay time.Duration
	once  sync.Once
}

func (s *delayedReadStream) Read(p []byte) (int, error) {
	s.once.Do(func() { time.Sleep(s.delay) })
	return s.inner.Read(p)
}
func (s *delayedReadStream) Write(p []byte) (int, error) { return s.inner.Write(p) }
func (s *delayedReadStream) Close() error                { return s.inner.Close() }

// latencyJoinTCPDialer is a joinDialer that dials over TCP (the dormant S4 path)
// and wraps the stream so the reply read is delayed.
func latencyJoinTCPDialer(readDelay time.Duration) joinDialer {
	return func(ctx context.Context, addr string, serverSPKI [32]byte, clientCert tls.Certificate) (io.ReadWriteCloser, []byte, func() error, error) {
		stream, bind, closeConn, err := transport.DialJoinTCP(ctx, addr, serverSPKI, clientCert)
		if err != nil {
			return nil, nil, nil, err
		}
		return &delayedReadStream{inner: stream, delay: readDelay}, bind, closeConn, nil
	}
}

// TestInviteJoinDialWith_RealConsumerOverDormantTCP partially discharges S5
// acceptance criterion 3 (deferred from S4). It drives the REAL invite-join
// consumer body (inviteJoinDialWith — the same code production reaches via
// inviteJoinDial) over the dormant TCP join transport (transport.DialJoinTCP)
// against NewTCPJoinListener, with the reply read forced to happen strictly after
// the leader's drain + full close. It proves end-to-end over TCP:
//   - the real consumer's choreography (write request → half-close → read reply),
//   - the tcpJoinStream half-close (CloseWrite) keeps the read side open, and
//   - the full JoinReply decodes when read after the leader has already closed.
//
// BOUNDARY — what this does NOT prove (stays an S5 criterion): the leader-side
// drain (tcp_join.go handleConn) and its RST-robustness are NOT discriminated
// here. On in-process loopback the tiny reply is buffered in the joiner's socket
// before the leader closes, so a clean FIN and an abortive RST are
// indistinguishable — removing the drain keeps this test green. An empirical
// "no RST-truncation under real latency" proof requires netem / multi-node that
// forces the abortive-close path to surface as a read error. That is S5.
func TestInviteJoinDialWith_RealConsumerOverDormantTCP(t *testing.T) {
	srvCert, srvSPKI, err := transport.GenerateNodeIdentity("cid", "leader")
	require.NoError(t, err)

	gotReq := make(chan struct{}, 1)
	ln, err := transport.NewTCPJoinListener("127.0.0.1:0", srvCert,
		func(_ context.Context, _ [32]byte, _ []byte, stream io.ReadWriteCloser) {
			// Real receiver wire choreography: read the framed JoinRequest to EOF,
			// then write a framed JoinReply and half-close. The listener's handleConn
			// drains + full-closes after this returns.
			if _, rerr := transport.JoinReadFields(stream, 1); rerr != nil {
				return
			}
			blob, eerr := cluster.EncodeJoinReplyForTest(cluster.JoinReply{Accepted: true, Status: cluster.JoinStatusOK})
			if eerr != nil {
				return
			}
			_, _ = stream.Write(transport.JoinPutField(nil, blob))
			_ = stream.Close() // half-close write
			gotReq <- struct{}{}
		})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	cliCert, _, err := transport.GenerateNodeIdentity("cid", "joiner")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// 150ms read delay >> the leader's microsecond-scale drain+close, so the joiner
	// reliably reads the reply AFTER the leader has fully closed (ordering, not RST).
	reply, err := inviteJoinDialWith(ctx, latencyJoinTCPDialer(150*time.Millisecond), ln.Addr(), srvSPKI, cliCert,
		func([]byte) (cluster.JoinRequest, error) {
			return cluster.JoinRequest{JoinPhase: 1, NodeID: "joiner", Address: "127.0.0.1:1"}, nil
		})
	require.NoError(t, err, "real consumer failed to read+decode the full reply over TCP after the leader closed")
	assert.True(t, reply.Accepted)
	assert.Equal(t, cluster.JoinStatusOK, reply.Status)

	select {
	case <-gotReq:
	case <-time.After(2 * time.Second):
		t.Fatal("leader handler never completed")
	}
}

// TestSelectJoinDialer_PicksTCPForTCPCluster proves the joiner dials over the same
// transport the leader's join listener runs: selectJoinDialer(true) drives a real
// join over a NewTCPJoinListener (succeeds), while selectJoinDialer(false) (the
// QUIC dialer) cannot reach the TCP listener.
func TestSelectJoinDialer_PicksTCPForTCPCluster(t *testing.T) {
	srvCert, srvSPKI, err := transport.GenerateNodeIdentity("cid", "leader")
	require.NoError(t, err)
	ln, err := transport.NewTCPJoinListener("127.0.0.1:0", srvCert,
		func(_ context.Context, _ [32]byte, _ []byte, stream io.ReadWriteCloser) {
			if _, rerr := transport.JoinReadFields(stream, 1); rerr != nil {
				return
			}
			blob, eerr := cluster.EncodeJoinReplyForTest(cluster.JoinReply{Accepted: true, Status: cluster.JoinStatusOK})
			if eerr != nil {
				return
			}
			_, _ = stream.Write(transport.JoinPutField(nil, blob))
			_ = stream.Close()
		})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	cliCert, _, err := transport.GenerateNodeIdentity("cid", "joiner")
	require.NoError(t, err)
	buildReq := func([]byte) (cluster.JoinRequest, error) {
		return cluster.JoinRequest{JoinPhase: 1, NodeID: "joiner", Address: "127.0.0.1:1"}, nil
	}

	// TCP dialer → real join over the TCP listener succeeds.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reply, err := inviteJoinDialWith(ctx, selectJoinDialer(true), ln.Addr(), srvSPKI, cliCert, buildReq)
	require.NoError(t, err, "selectJoinDialer(true) must dial the TCP join listener over TCP")
	assert.True(t, reply.Accepted)

	// QUIC dialer → cannot reach the TCP listener (UDP vs TCP); bounded by a short ctx.
	nctx, ncancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer ncancel()
	_, qerr := inviteJoinDialWith(nctx, selectJoinDialer(false), ln.Addr(), srvSPKI, cliCert, buildReq)
	require.Error(t, qerr, "selectJoinDialer(false) (QUIC) must not reach a TCP join listener")
}

// TestJoinDialerHelpers_RouteTCPSourceToTCPDialer closes the untested-wiring gap:
// the two per-call-site helpers (Phase-1 joinDialerForOpts, Phase-2
// joinDialerForConfig) must route a TCP-selecting source to DialJoinTCP. A
// hardcoded QUIC dialer at either site cannot reach a TCP join listener, so these
// positive joins go RED — proving the wiring actually consults the flag.
func TestJoinDialerHelpers_RouteTCPSourceToTCPDialer(t *testing.T) {
	srvCert, srvSPKI, err := transport.GenerateNodeIdentity("cid", "leader")
	require.NoError(t, err)
	ln, err := transport.NewTCPJoinListener("127.0.0.1:0", srvCert,
		func(_ context.Context, _ [32]byte, _ []byte, stream io.ReadWriteCloser) {
			if _, rerr := transport.JoinReadFields(stream, 1); rerr != nil {
				return
			}
			blob, eerr := cluster.EncodeJoinReplyForTest(cluster.JoinReply{Accepted: true, Status: cluster.JoinStatusOK})
			if eerr != nil {
				return
			}
			_, _ = stream.Write(transport.JoinPutField(nil, blob))
			_ = stream.Close()
		})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	cliCert, _, err := transport.GenerateNodeIdentity("cid", "joiner")
	require.NoError(t, err)
	buildReq := func([]byte) (cluster.JoinRequest, error) {
		return cluster.JoinRequest{JoinPhase: 1, NodeID: "joiner", Address: "127.0.0.1:1"}, nil
	}
	dialOK := func(d joinDialer) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		reply, derr := inviteJoinDialWith(ctx, d, ln.Addr(), srvSPKI, cliCert, buildReq)
		if derr == nil && !reply.Accepted {
			t.Fatal("join not accepted")
		}
		return derr
	}

	require.NoError(t, dialOK(joinDialerForOpts(&ServeOptions{Transport: "tcp"})),
		"Phase-1 joinDialerForOpts must dial the TCP listener over TCP")
	require.NoError(t, dialOK(joinDialerForConfig(Config{useTCPTransport: true})),
		"Phase-2 joinDialerForConfig must dial the TCP listener over TCP")
}
