package transport

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTPJoin_ProofTLSStateThroughHertz is the G1 proof: a real Hertz join
// server must surface a usable per-request tls.ConnectionState so the handler
// can capture the peer SPKI (PeerCertificates) AND derive the RFC5705 channel
// binding (ExportKeyingMaterial). If Hertz's standard transport did not expose
// a live exporter, the whole conversion would be building on sand — so this
// asserts the security primitive end-to-end before anything else.
func TestHTTPJoin_ProofTLSStateThroughHertz(t *testing.T) {
	srvCert, srvSPKI := newTestJoinCert(t, "join-leader")
	cliCert, cliSPKI := newTestJoinCert(t, "join-joiner")

	type captured struct {
		spki [32]byte
		bind []byte
	}
	gotCh := make(chan captured, 1)
	ln, err := NewHTTPJoinListener("127.0.0.1:0", srvCert,
		func(_ context.Context, peerSPKI [32]byte, bind []byte, req []byte) ([]byte, error) {
			gotCh <- captured{spki: peerSPKI, bind: bind}
			return append([]byte("echo:"), req...), nil
		})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var clientBind []byte
	reply, err := DialJoinHTTP(ctx, ln.Addr(), srvSPKI, cliCert, func(bind []byte) ([]byte, error) {
		clientBind = bind
		return []byte("join-request"), nil
	})
	require.NoError(t, err)
	assert.Equal(t, "echo:join-request", string(reply))

	select {
	case got := <-gotCh:
		// PeerCertificates capture works through Hertz.
		assert.Equal(t, cliSPKI, got.spki, "server must capture the joiner's client-cert SPKI")
		// ExportKeyingMaterial works through Hertz AND matches the client's
		// binding on the same TLS session (32 bytes, non-empty).
		require.Len(t, got.bind, JoinBindingLen)
		require.Len(t, clientBind, JoinBindingLen)
		assert.Equal(t, clientBind, got.bind, "joiner and leader must derive the same RFC5705 binding")
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}
}

// TestHTTPJoin_PermissiveAcceptsUnknownClientCert asserts the deliberately
// permissive accept: a brand-new joiner whose self-signed SPKI is in nobody's
// accept-set still completes the handshake and reaches the handler.
func TestHTTPJoin_PermissiveAcceptsUnknownClientCert(t *testing.T) {
	srvCert, srvSPKI := newTestJoinCert(t, "join-leader")
	// A fresh joiner cert never seen before — must still be accepted.
	cliCert, _ := newTestJoinCert(t, "brand-new-joiner")

	ran := make(chan struct{}, 1)
	ln, err := NewHTTPJoinListener("127.0.0.1:0", srvCert,
		func(_ context.Context, _ [32]byte, _ []byte, _ []byte) ([]byte, error) {
			ran <- struct{}{}
			return []byte("ok"), nil
		})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = DialJoinHTTP(ctx, ln.Addr(), srvSPKI, cliCert, func([]byte) ([]byte, error) {
		return []byte("req"), nil
	})
	require.NoError(t, err)
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("permissive accept failed: handler never ran for an unknown client cert")
	}
}

// TestHTTPJoin_DialRejectsWrongServerSPKI asserts the client-side SPKI pin:
// dialing with the wrong expected server SPKI fails the handshake (relay/MITM
// defense). Mutation guard for the pin.
func TestHTTPJoin_DialRejectsWrongServerSPKI(t *testing.T) {
	srvCert, _ := newTestJoinCert(t, "join-leader")
	cliCert, _ := newTestJoinCert(t, "join-joiner")
	_, wrongSPKI := newTestJoinCert(t, "imposter")

	ln, err := NewHTTPJoinListener("127.0.0.1:0", srvCert,
		func(_ context.Context, _ [32]byte, _ []byte, _ []byte) ([]byte, error) {
			return []byte("ok"), nil
		})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = DialJoinHTTP(ctx, ln.Addr(), wrongSPKI, cliCert, func([]byte) ([]byte, error) {
		return []byte("req"), nil
	})
	require.Error(t, err, "dial must reject a server whose SPKI does not match the pin")
}

// TestHTTPJoin_HandlerErrorIsNon200 asserts a handler error surfaces as a
// non-200 the client maps to a Go error (e.g. the not-leader path returns a
// 200 with a structured reply; a true error returns 500).
func TestHTTPJoin_HandlerErrorReplyMappedToError(t *testing.T) {
	srvCert, srvSPKI := newTestJoinCert(t, "join-leader")
	cliCert, _ := newTestJoinCert(t, "join-joiner")

	ln, err := NewHTTPJoinListener("127.0.0.1:0", srvCert,
		func(_ context.Context, _ [32]byte, _ []byte, _ []byte) ([]byte, error) {
			return nil, assertErr("boom")
		})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = DialJoinHTTP(ctx, ln.Addr(), srvSPKI, cliCert, func([]byte) ([]byte, error) {
		return []byte("req"), nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

type assertErr string

func (e assertErr) Error() string { return string(e) }
