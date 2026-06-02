package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/transport"
)

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
