package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sha256hex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

type assertErr struct{}

func (assertErr) Error() string { return "boom" }

// startTCP brings up a transport listening on loopback and registers cleanup.
func startTCP(t *testing.T, psk string) *TCPTransport {
	t.Helper()
	tr := MustNewTCPTransport(psk)
	require.NoError(t, tr.Listen(context.Background(), "127.0.0.1:0"))
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func TestNewTCPTransport_DerivesClusterIdentity(t *testing.T) {
	const psk = "test-cluster-key-123"
	tr, err := NewTCPTransport(psk)
	require.NoError(t, err)
	require.NotNil(t, tr)
	defer tr.Close()

	_, wantSPKI, err := DeriveClusterIdentity(psk)
	require.NoError(t, err)
	assert.Equal(t, wantSPKI, tr.snap.PresentSPKI)
	assert.True(t, tr.snap.Accepts(wantSPKI))
}

func TestNewTCPTransport_EmptyPSKRejected(t *testing.T) {
	_, err := NewTCPTransport("")
	assert.ErrorIs(t, err, ErrEmptyClusterKey)
}

func TestTCP_Call_FrameRoundTrip(t *testing.T) {
	const psk = "rt-key"
	srv := startTCP(t, psk)
	srv.Handle(StreamAdmin, func(req *Message) *Message {
		return NewResponse(req, append([]byte("echo:"), req.Payload...))
	})

	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Call(ctx, srv.LocalAddr(), &Message{Type: StreamAdmin, Payload: []byte("ping")})
	require.NoError(t, err)
	assert.Equal(t, StatusOK, resp.Status)
	assert.Equal(t, "echo:ping", string(resp.Payload))
}

func TestTCP_Call_StatusErrorPropagated(t *testing.T) {
	const psk = "err-key"
	srv := startTCP(t, psk)
	srv.Handle(StreamAdmin, func(req *Message) *Message {
		return NewErrorResponse(req, StatusError, assertErr{})
	})
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.Call(ctx, srv.LocalAddr(), &Message{Type: StreamAdmin})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestTCP_Call_SPKIMismatchRejected(t *testing.T) {
	srv := startTCP(t, "server-key")
	srv.Handle(StreamAdmin, func(req *Message) *Message { return NewResponse(req, nil) })

	cli := MustNewTCPTransport("different-key") // different cluster identity
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.Call(ctx, srv.LocalAddr(), &Message{Type: StreamAdmin})
	require.Error(t, err) // TLS handshake fails on SPKI mismatch
}

func TestTCP_CallWithBody_MultiMB(t *testing.T) {
	const psk = "body-key"
	srv := startTCP(t, psk)
	srv.HandleBody(StreamShardWriteBody, func(req *Message, body io.Reader) *Message {
		got, err := io.ReadAll(body)
		if err != nil {
			return NewErrorResponse(req, StatusError, err)
		}
		return NewResponse(req, []byte(sha256hex(got)))
	})

	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })

	payload := make([]byte, 5<<20) // 5 MiB
	_, _ = rand.Read(payload)
	want := sha256hex(payload)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cli.CallWithBody(ctx, srv.LocalAddr(),
		&Message{Type: StreamShardWriteBody}, bytes.NewReader(payload))
	require.NoError(t, err)
	assert.Equal(t, StatusOK, resp.Status)
	assert.Equal(t, want, string(resp.Payload))
}

func TestTCP_CallRead_BodyCleanEOF(t *testing.T) {
	const psk = "read-key"
	srv := startTCP(t, psk)
	respBody := make([]byte, 3<<20) // 3 MiB
	_, _ = rand.Read(respBody)
	wantHex := sha256hex(respBody)

	srv.HandleRead(StreamShardReadBody, func(req *Message) (*Message, io.ReadCloser) {
		return NewResponse(req, []byte("meta-ok")), io.NopCloser(bytes.NewReader(respBody))
	})

	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, rc, err := cli.CallRead(ctx, srv.LocalAddr(), &Message{Type: StreamShardReadBody})
	require.NoError(t, err)
	assert.Equal(t, "meta-ok", string(resp.Payload))

	// Read body to end: io.ReadAll treats io.EOF as nil; ErrUnexpectedEOF (RST
	// truncation) would surface as a non-nil error.
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	assert.Equal(t, wantHex, sha256hex(got))
}

func TestTCP_CallFlatBuffer_RoundTrip(t *testing.T) {
	const psk = "fb-key"
	srv := startTCP(t, psk)
	srv.Handle(StreamMetaRaft, func(req *Message) *Message {
		return NewResponse(req, append([]byte("fb:"), req.Payload...))
	})
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })

	b := flatbuffers.NewBuilder(0)
	off := b.CreateByteVector([]byte("payload-bytes"))
	b.Finish(off)
	fw := &FlatBuffersWriter{Typ: StreamMetaRaft, Builder: b}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.CallFlatBuffer(ctx, srv.LocalAddr(), fw)
	require.NoError(t, err)
	assert.Equal(t, StatusOK, resp.Status)
	assert.True(t, bytes.HasPrefix(resp.Payload, []byte("fb:")))
}

func TestTCP_SendReceive_FireAndForget(t *testing.T) {
	const psk = "gossip-key"
	srv := startTCP(t, psk) // no handler registered => flows to inbox
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, cli.Connect(ctx, srv.LocalAddr())) // no-op, nil
	require.NoError(t, cli.Send(ctx, srv.LocalAddr(),
		&Message{Type: StreamReceipt, Payload: []byte("gossip-msg")}))

	select {
	case rm := <-srv.Receive():
		assert.Equal(t, StreamReceipt, rm.Message.Type)
		assert.Equal(t, "gossip-msg", string(rm.Message.Payload))
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive gossip message on inbox")
	}
}

func TestTCP_CatchAllStreamHandler_RoutesAndPrioritizes(t *testing.T) {
	const psk = "catchall-key"
	srv := startTCP(t, psk)
	// Catch-all (boot wires StreamData shard RPCs through this); a per-type
	// handler for a different type must still take priority.
	srv.SetStreamHandler(func(req *Message) *Message {
		return NewResponse(req, []byte("catchall:"+string(req.Payload)))
	})
	srv.Handle(StreamAdmin, func(req *Message) *Message {
		return NewResponse(req, []byte("pertype:"+string(req.Payload)))
	})

	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// StreamData has no per-type handler => must reach the catch-all (this is the
	// shard-RPC routing path that the conformance assertions cannot verify).
	resp, err := cli.Call(ctx, srv.LocalAddr(), &Message{Type: StreamData, Payload: []byte("d")})
	require.NoError(t, err)
	assert.Equal(t, "catchall:d", string(resp.Payload))

	// StreamAdmin has a per-type handler => it wins over the catch-all.
	resp, err = cli.Call(ctx, srv.LocalAddr(), &Message{Type: StreamAdmin, Payload: []byte("a")})
	require.NoError(t, err)
	assert.Equal(t, "pertype:a", string(resp.Payload))
}

func TestTCP_ConcurrentCalls_OnePeer(t *testing.T) {
	const psk = "conc-key"
	srv := startTCP(t, psk)
	srv.Handle(StreamAdmin, func(req *Message) *Message {
		return NewResponse(req, req.Payload) // echo
	})
	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })

	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			payload := []byte(fmt.Sprintf("req-%d", i))
			resp, err := cli.Call(ctx, srv.LocalAddr(), &Message{Type: StreamAdmin, Payload: payload})
			if err != nil {
				errs[i] = err
				return
			}
			if string(resp.Payload) != string(payload) {
				errs[i] = fmt.Errorf("mismatch: got %q want %q", resp.Payload, payload)
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		assert.NoErrorf(t, err, "call %d", i)
	}
}
