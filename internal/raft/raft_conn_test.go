package raft

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testQUICPair sets up a loopback QUIC server+client and returns a connected
// pair (clientConn, serverConn). Both are *quic.Conn ready for stream open.
func testQUICPair(t *testing.T) (*quic.Conn, *quic.Conn, func()) {
	t.Helper()
	tlsCfg := newSelfSignedTLSConfig(t, "raft-conn-test")
	listener, err := quic.ListenAddr("127.0.0.1:0", tlsCfg, &quic.Config{
		MaxIdleTimeout:        30 * time.Second,
		KeepAlivePeriod:       5 * time.Second,
		MaxIncomingStreams:    1024,
		MaxIncomingUniStreams: 1024,
	})
	require.NoError(t, err)

	addr := listener.Addr().String()
	dialCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srvCh := make(chan *quic.Conn, 1)
	srvErrCh := make(chan error, 1)
	go func() {
		acceptCtx, acceptCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer acceptCancel()
		conn, err := listener.Accept(acceptCtx)
		if err != nil {
			srvErrCh <- err
			return
		}
		srvCh <- conn
	}()

	clientTLS := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"raft-conn-test"},
	}
	clientConn, err := quic.DialAddr(dialCtx, addr, clientTLS, &quic.Config{
		MaxIdleTimeout:        30 * time.Second,
		KeepAlivePeriod:       5 * time.Second,
		MaxIncomingStreams:    1024,
		MaxIncomingUniStreams: 1024,
	})
	require.NoError(t, err)

	var serverConn *quic.Conn
	select {
	case serverConn = <-srvCh:
	case err := <-srvErrCh:
		t.Fatalf("accept: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("accept timeout")
	}

	cleanup := func() {
		_ = clientConn.CloseWithError(0, "test cleanup")
		_ = serverConn.CloseWithError(0, "test cleanup")
		_ = listener.Close()
	}
	return clientConn, serverConn, cleanup
}

func newSelfSignedTLSConfig(t *testing.T, alpn string) *tls.Config {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "raft-conn-test"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{alpn},
		MinVersion:   tls.VersionTLS13,
	}
}

// setupRaftConnPair creates a server+client RaftConn pair sharing a QUIC connection.
// poolSize controls streams per side; rpcHandler is registered server-side.
func setupRaftConnPair(t *testing.T, poolSize int, rpcHandler RPCHandler) (*RaftConn, *RaftConn, func()) {
	t.Helper()
	clientQ, serverQ, qcleanup := testQUICPair(t)

	serverRC := NewRaftConn(serverQ, RaftConnConfig{
		PoolSize:        poolSize,
		HandlerPoolSize: 16,
		RPCHandler:      rpcHandler,
	})
	clientRC := NewRaftConn(clientQ, RaftConnConfig{
		PoolSize:        poolSize,
		HandlerPoolSize: 16,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	var clientErr, serverErr error
	go func() {
		defer wg.Done()
		clientErr = clientRC.OpenOutboundStreams(ctx)
	}()
	go func() {
		defer wg.Done()
		serverErr = serverRC.AcceptInboundStreams(ctx)
	}()
	wg.Wait()
	require.NoError(t, clientErr)
	require.NoError(t, serverErr)

	clientRC.StartReaders()
	serverRC.StartReaders()

	cleanup := func() {
		clientRC.Close()
		serverRC.Close()
		qcleanup()
	}
	return clientRC, serverRC, cleanup
}

// --- Tests ---

func TestRaftConn_BasicCall(t *testing.T) {
	handler := func(payload []byte) ([]byte, error) {
		// echo with prefix
		out := append([]byte("reply:"), payload...)
		return out, nil
	}
	client, _, cleanup := setupRaftConnPair(t, 4, handler)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.Call(ctx, []byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, "reply:hello", string(resp))
}

func TestRaftConn_ConcurrentCalls(t *testing.T) {
	handler := func(payload []byte) ([]byte, error) {
		// echo
		return payload, nil
	}
	client, _, cleanup := setupRaftConnPair(t, 4, handler)
	defer cleanup()

	// Bounded by handler pool size (16). Use 12 to leave headroom; test verifies
	// that distinct corrIDs are correctly demuxed across the 4 streams.
	const N = 12
	var wg sync.WaitGroup
	wg.Add(N)
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			payload := []byte{byte(i % 256), byte(i / 256)}
			resp, err := client.Call(ctx, payload)
			if err != nil {
				errs[i] = err
				return
			}
			if len(resp) != 2 || resp[0] != payload[0] || resp[1] != payload[1] {
				errs[i] = ErrFrameTruncated // misuse but signals mismatch
			}
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		require.NoError(t, e, "call %d failed", i)
	}
}

func TestRaftConn_HandlerError(t *testing.T) {
	handler := func(payload []byte) ([]byte, error) {
		return nil, assertError("bad input")
	}
	client, _, cleanup := setupRaftConnPair(t, 2, handler)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := client.Call(ctx, []byte("x"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad input")
}

func TestRaftConn_HandlerNilResponse(t *testing.T) {
	// Handler returning (nil, nil) must surface as ErrNoResponse, NOT a hang.
	handler := func(payload []byte) ([]byte, error) {
		return nil, nil
	}
	client, _, cleanup := setupRaftConnPair(t, 2, handler)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := client.Call(ctx, []byte("x"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil without error")
}

func TestRaftConn_StreamPoolRR(t *testing.T) {
	// Use a counting handler that records which stream the request came from.
	// Since server reads from each stream independently in its own goroutine,
	// we infer distribution by counting concurrent requests received.
	const poolSize = 4
	const N = 80
	var inflight atomic.Int64
	var maxInflight atomic.Int64

	handler := func(payload []byte) ([]byte, error) {
		cur := inflight.Add(1)
		// track max
		for {
			old := maxInflight.Load()
			if cur <= old || maxInflight.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond) // let concurrency build
		inflight.Add(-1)
		return payload, nil
	}
	client, _, cleanup := setupRaftConnPair(t, poolSize, handler)
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = client.Call(ctx, []byte{1})
		}()
	}
	wg.Wait()

	// Server-side handler pool is 16, so we expect maxInflight >= 4 (pool=4 × at least 1 per stream).
	assert.GreaterOrEqual(t, maxInflight.Load(), int64(4),
		"maxInflight=%d expected ≥ poolSize=%d (RR distribution)", maxInflight.Load(), poolSize)
}

func TestRaftConn_CtxCancel(t *testing.T) {
	// Slow handler — caller cancels mid-flight, expect ctx.Err()
	release := make(chan struct{})
	handler := func(payload []byte) ([]byte, error) {
		<-release
		return []byte("late"), nil
	}
	client, _, cleanup := setupRaftConnPair(t, 2, handler)
	defer cleanup()
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := client.Call(ctx, []byte("x"))
	require.Error(t, err)
	assert.True(t, err == context.DeadlineExceeded || err == context.Canceled,
		"expected ctx err, got %v", err)
}

func TestRaftConn_ConcurrentCloseCallRace(t *testing.T) {
	// In-flight Call when conn closed must return error, not panic.
	release := make(chan struct{})
	defer close(release)
	handler := func(payload []byte) ([]byte, error) {
		<-release
		return []byte("late"), nil
	}
	client, _, cleanup := setupRaftConnPair(t, 2, handler)
	defer cleanup()

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := client.Call(ctx, []byte("x"))
		done <- err
	}()
	time.Sleep(50 * time.Millisecond) // ensure call in flight
	client.Close()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Call did not return after Close")
	}
}

func TestRaftConn_FrameTooBig_Send(t *testing.T) {
	client, _, cleanup := setupRaftConnPair(t, 1, func(p []byte) ([]byte, error) {
		return p, nil
	})
	defer cleanup()

	huge := make([]byte, MaxFrameSize+1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := client.Call(ctx, huge)
	require.Error(t, err)
	assert.Equal(t, ErrFrameTooBig, err)
}

func TestRaftConn_Notify(t *testing.T) {
	got := make(chan []byte, 1)
	clientQ, serverQ, qcleanup := testQUICPair(t)

	serverRC := NewRaftConn(serverQ, RaftConnConfig{
		PoolSize:      2,
		NotifyHandler: func(p []byte) { got <- p },
	})
	clientRC := NewRaftConn(clientQ, RaftConnConfig{PoolSize: 2})
	defer func() {
		clientRC.Close()
		serverRC.Close()
		qcleanup()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, clientRC.OpenOutboundStreams(ctx))
	require.NoError(t, serverRC.AcceptInboundStreams(ctx))
	clientRC.StartReaders()
	serverRC.StartReaders()

	require.NoError(t, clientRC.Notify([]byte("ping")))

	select {
	case p := <-got:
		assert.Equal(t, "ping", string(p))
	case <-time.After(2 * time.Second):
		t.Fatal("notify not received")
	}
}

// assertError is a tiny helper used in handler tests.
type assertError string

func (e assertError) Error() string { return string(e) }
