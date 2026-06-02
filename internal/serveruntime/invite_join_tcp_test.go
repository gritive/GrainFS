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

// delayedReadStream wraps a join stream and delays ONLY the first read (the
// reply read in inviteJoinDialWith). This forces the joiner to read the reply
// strictly AFTER the leader has written it, half-closed, drained the joiner's
// close_notify, and full-closed — the worst case for RST-truncation. If the
// leader's full close were abortive (no drain), the delayed read would see a
// truncated/failed reply; the leader drain makes the close clean (FIN).
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

// TestInviteJoinDialWith_TCPReadsFullReplyAfterLeaderClose discharges S5
// acceptance criterion 3 (deferred from S4) within dormant scope: drive the REAL
// invite-join consumer (inviteJoinDialWith) over the dormant TCP join transport
// against NewTCPJoinListener, with a latency-wrapped read so the joiner reads the
// reply strictly after the leader's drain + full close. The full reply must
// decode (no RST-truncation) — a stronger robustness signal than the S4
// immediate-read loopback test.
//
// NOTE (boundary): this proves the full reply survives a delayed read after the
// leader's clean close. The kernel-hard "no RST under real network latency" proof
// stays an S5 criterion (it needs netem / multi-node, not in-process loopback).
func TestInviteJoinDialWith_TCPReadsFullReplyAfterLeaderClose(t *testing.T) {
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
	// reliably reads after the leader has fully closed.
	reply, err := inviteJoinDialWith(ctx, latencyJoinTCPDialer(150*time.Millisecond), ln.Addr(), srvSPKI, cliCert,
		func([]byte) (cluster.JoinRequest, error) {
			return cluster.JoinRequest{JoinPhase: 1, NodeID: "joiner", Address: "127.0.0.1:1"}, nil
		})
	require.NoError(t, err, "joiner failed to read the full reply after the leader's drain+close (RST-truncation?)")
	assert.True(t, reply.Accepted)
	assert.Equal(t, cluster.JoinStatusOK, reply.Status)

	select {
	case <-gotReq:
	case <-time.After(2 * time.Second):
		t.Fatal("leader handler never completed")
	}
}
