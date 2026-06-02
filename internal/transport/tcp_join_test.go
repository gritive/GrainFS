package transport

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTCPJoin_RoundTripReadsFullReply exercises the EXACT consumer choreography
// (write request → stream.Close() [half-close write] → read reply). If the TCP
// stream's Close() were a full close, the reply read would fail — this is the
// discriminating test for the half-close contract.
func TestTCPJoin_RoundTripReadsFullReply(t *testing.T) {
	srvCert, srvSPKI := newTestJoinCert(t, "join-leader")
	cliCert, cliSPKI := newTestJoinCert(t, "join-joiner")

	gotSPKI := make(chan [32]byte, 1)
	ln, err := NewTCPJoinListener("127.0.0.1:0", srvCert, func(_ context.Context, peerSPKI [32]byte, _ []byte, stream io.ReadWriteCloser) {
		gotSPKI <- peerSPKI
		// Leader: read the FULL 1-field request BEFORE replying (RST invariant),
		// write a 1-field reply, then half-close the write side.
		if _, rerr := JoinReadFields(stream, 1); rerr != nil {
			return
		}
		_, _ = stream.Write(JoinPutField(nil, []byte("join-reply-ok")))
		_ = stream.Close() // half-close write (the listener drains+full-closes after)
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, _, closeConn, err := DialJoinTCP(ctx, ln.Addr(), srvSPKI, cliCert)
	require.NoError(t, err)
	defer func() { _ = closeConn() }()

	_, err = stream.Write(JoinPutField(nil, []byte("join-request")))
	require.NoError(t, err)
	_ = stream.Close() // HALF-close write side: read MUST still work
	// The joiner reads the FULL reply, NOT ECONNRESET (half-close + leader drain).
	reply, err := JoinReadFields(stream, 1)
	require.NoError(t, err, "joiner failed to read reply after half-close (full close bug?)")
	assert.Equal(t, "join-reply-ok", string(reply[0]))

	select {
	case spki := <-gotSPKI:
		assert.Equal(t, cliSPKI, spki) // peer SPKI captured from the joiner's client cert
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}
}

// TestTCPJoin_IntraConnBindingSymmetry asserts the joiner and the leader derive
// the SAME RFC5705 binding on ONE TCP join connection (RFC 8446 §7.5). This is
// NOT a cross-transport comparison — a join uses exactly one transport.
func TestTCPJoin_IntraConnBindingSymmetry(t *testing.T) {
	srvCert, srvSPKI := newTestJoinCert(t, "join-leader")
	cliCert, _ := newTestJoinCert(t, "join-joiner")

	leaderBindCh := make(chan []byte, 1)
	ln, err := NewTCPJoinListener("127.0.0.1:0", srvCert, func(_ context.Context, _ [32]byte, bind []byte, stream io.ReadWriteCloser) {
		leaderBindCh <- append([]byte(nil), bind...)
		_, _ = JoinReadFields(stream, 1) // drain request before close (RST invariant)
		_ = stream.Close()
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, joinerBind, closeConn, err := DialJoinTCP(ctx, ln.Addr(), srvSPKI, cliCert)
	require.NoError(t, err)
	defer func() { _ = closeConn() }()
	_, _ = stream.Write(JoinPutField(nil, []byte("x")))
	_ = stream.Close()

	var leaderBind []byte
	select {
	case leaderBind = <-leaderBindCh:
	case <-time.After(3 * time.Second):
		t.Fatal("leader did not produce a binding")
	}
	require.Len(t, joinerBind, JoinBindingLen)
	assert.True(t, bytes.Equal(joinerBind, leaderBind), "intra-conn binding asymmetry")
}

// TestTCPJoin_DialRejectsWrongServerSPKI proves the joiner pins the leader: a
// mismatched server SPKI fails the dial (relay/MITM defense).
func TestTCPJoin_DialRejectsWrongServerSPKI(t *testing.T) {
	srvCert, _ := newTestJoinCert(t, "join-leader")
	cliCert, _ := newTestJoinCert(t, "join-joiner")
	ln, err := NewTCPJoinListener("127.0.0.1:0", srvCert, func(context.Context, [32]byte, []byte, io.ReadWriteCloser) {})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wrong [32]byte
	wrong[0] = 0xFF
	_, _, _, err = DialJoinTCP(ctx, ln.Addr(), wrong, cliCert)
	assert.Error(t, err)
}

// TestTCPJoin_DoubleCloseSafe verifies the stream/conn collapse on TCP is safe:
// stream.Close() (half-close write) followed by closeConn() (full) — and either
// called twice — must not panic or hang.
func TestTCPJoin_DoubleCloseSafe(t *testing.T) {
	srvCert, srvSPKI := newTestJoinCert(t, "join-leader")
	cliCert, _ := newTestJoinCert(t, "join-joiner")
	ln, err := NewTCPJoinListener("127.0.0.1:0", srvCert, func(_ context.Context, _ [32]byte, _ []byte, stream io.ReadWriteCloser) {
		_, _ = JoinReadFields(stream, 1)
		_ = stream.Close()
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, _, closeConn, err := DialJoinTCP(ctx, ln.Addr(), srvSPKI, cliCert)
	require.NoError(t, err)
	_, _ = stream.Write(JoinPutField(nil, []byte("x")))
	assert.NotPanics(t, func() {
		_ = stream.Close() // CloseWrite (half)
		_ = stream.Close() // again
		_ = closeConn()    // full
		_ = closeConn()    // again
	})
}
