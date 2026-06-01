package raft

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRaftConnPairPipe builds a client+server RaftConn over poolSize in-memory
// net.Pipe duplexes (one per stream slot) — no QUIC. closeHook is nil: net.Pipe
// streams are torn down by per-stream Close in markBroken. This is the carrier
// model S2b's TCP driver composes (N single-stream conns), exercised here as a
// multi-stream pair to prove RaftConn is genuinely carrier-agnostic.
func newRaftConnPairPipe(t *testing.T, poolSize int, clientCfg, serverCfg RaftConnConfig) (*RaftConn, *RaftConn) {
	t.Helper()
	clientStreams := make([]io.ReadWriteCloser, poolSize)
	serverStreams := make([]io.ReadWriteCloser, poolSize)
	for i := 0; i < poolSize; i++ {
		c, s := net.Pipe()
		clientStreams[i] = c
		serverStreams[i] = s
	}
	client := NewRaftConn("server-peer", clientStreams, nil, clientCfg)
	server := NewRaftConn("client-peer", serverStreams, nil, serverCfg)
	client.StartReaders()
	server.StartReaders()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return client, server
}

func TestRaftConnPipe_CallRoundtrip(t *testing.T) {
	server := RaftConnConfig{RPCHandler: func(p []byte) ([]byte, error) {
		return append([]byte("echo:"), p...), nil
	}}
	client, _ := newRaftConnPairPipe(t, 4, RaftConnConfig{}, server)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Call(ctx, []byte("ping"))
	require.NoError(t, err)
	assert.Equal(t, "echo:ping", string(resp))
}

func TestRaftConnPipe_ConcurrentCorrID(t *testing.T) {
	server := RaftConnConfig{RPCHandler: func(p []byte) ([]byte, error) {
		return append([]byte("echo:"), p...), nil
	}}
	client, _ := newRaftConnPairPipe(t, 4, RaftConnConfig{}, server)

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

func TestRaftConnPipe_MarkBrokenFansOut(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	entered := make(chan struct{})
	var once sync.Once
	server := RaftConnConfig{RPCHandler: func(p []byte) ([]byte, error) {
		once.Do(func() { close(entered) })
		<-block
		return p, nil
	}}
	client, serverRC := newRaftConnPairPipe(t, 4, RaftConnConfig{}, server)

	errCh := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), []byte("x"))
		errCh <- err
	}()
	<-entered
	_ = serverRC.Close()

	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("pending call did not error after the pipe conn broke")
	}
}
