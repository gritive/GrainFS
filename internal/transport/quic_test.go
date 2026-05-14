package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/prometheus/client_golang/prometheus/testutil"
	quic "github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/metrics"
)

func TestQUICTransport_SendReceive(t *testing.T) {
	ctx := context.Background()

	node1 := MustNewQUICTransport("test-cluster-psk")
	node2 := MustNewQUICTransport("test-cluster-psk")
	defer node1.Close()
	defer node2.Close()

	require.NoError(t, node1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, node2.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, node2.Connect(ctx, node1.LocalAddr()))

	msg := &Message{Type: StreamControl, Payload: []byte("hello-node1")}
	require.NoError(t, node2.Send(ctx, node1.LocalAddr(), msg))

	select {
	case recv := <-node1.Receive():
		assert.Equal(t, StreamControl, recv.Message.Type)
		assert.Equal(t, "hello-node1", string(recv.Message.Payload))
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestQUICTransport_StreamTypeMultiplexing(t *testing.T) {
	ctx := context.Background()

	sender := MustNewQUICTransport("test-cluster-psk")
	receiver := MustNewQUICTransport("test-cluster-psk")
	defer sender.Close()
	defer receiver.Close()

	require.NoError(t, receiver.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, sender.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, sender.Connect(ctx, receiver.LocalAddr()))

	messages := []*Message{
		{Type: StreamControl, Payload: []byte("raft-vote")},
		{Type: StreamData, Payload: []byte("shard-data")},
		{Type: StreamAdmin, Payload: []byte("health-check")},
	}

	for _, msg := range messages {
		require.NoError(t, sender.Send(ctx, receiver.LocalAddr(), msg))
	}

	received := make(map[StreamType]string)
	for i := 0; i < len(messages); i++ {
		select {
		case recv := <-receiver.Receive():
			received[recv.Message.Type] = string(recv.Message.Payload)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}

	assert.Equal(t, "raft-vote", received[StreamControl])
	assert.Equal(t, "shard-data", received[StreamData])
	assert.Equal(t, "health-check", received[StreamAdmin])
}

func TestQUICTransport_ThreeNodes(t *testing.T) {
	ctx := context.Background()

	nodes := make([]*QUICTransport, 3)
	for i := range nodes {
		nodes[i] = MustNewQUICTransport("test-cluster-psk")
		defer nodes[i].Close()
		require.NoError(t, nodes[i].Listen(ctx, "127.0.0.1:0"))
	}

	// Full mesh: each node connects to all others
	for i := range nodes {
		for j := range nodes {
			if i == j {
				continue
			}
			require.NoError(t, nodes[i].Connect(ctx, nodes[j].LocalAddr()))
		}
	}

	// node0 sends to node1 and node2
	require.NoError(t, nodes[0].Send(ctx, nodes[1].LocalAddr(), &Message{Type: StreamControl, Payload: []byte("to-node1")}))
	require.NoError(t, nodes[0].Send(ctx, nodes[2].LocalAddr(), &Message{Type: StreamControl, Payload: []byte("to-node2")}))

	for _, tc := range []struct {
		nodeIdx int
		want    string
	}{
		{1, "to-node1"},
		{2, "to-node2"},
	} {
		select {
		case recv := <-nodes[tc.nodeIdx].Receive():
			assert.Equal(t, tc.want, string(recv.Message.Payload))
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for message at node%d", tc.nodeIdx)
		}
	}
}

func TestQUICTransport_ConcurrentSends(t *testing.T) {
	ctx := context.Background()

	sender := MustNewQUICTransport("test-cluster-psk")
	receiver := MustNewQUICTransport("test-cluster-psk")
	defer sender.Close()
	defer receiver.Close()

	require.NoError(t, receiver.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, sender.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, sender.Connect(ctx, receiver.LocalAddr()))

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func(n int) {
			defer wg.Done()
			msg := &Message{Type: StreamControl, Payload: []byte(fmt.Sprintf("msg-%d", n))}
			assert.NoError(t, sender.Send(ctx, receiver.LocalAddr(), msg))
		}(i)
	}
	wg.Wait()

	received := 0
	timeout := time.After(10 * time.Second)
	for received < count {
		select {
		case <-receiver.Receive():
			received++
		case <-timeout:
			t.Fatalf("timeout: received %d/%d messages", received, count)
		}
	}
	assert.Equal(t, count, received)
}

func TestQUICTransport_Call(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("test-cluster-psk")
	client := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	defer client.Close()

	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Connect(ctx, server.LocalAddr()))

	// Server echoes back the payload with "reply:" prefix
	server.SetStreamHandler(func(reqMsg *Message) *Message {
		return NewResponse(reqMsg, append([]byte("reply:"), reqMsg.Payload...))
	})

	req := &Message{Type: StreamControl, Payload: []byte("ping")}
	resp, err := client.Call(ctx, server.LocalAddr(), req)
	require.NoError(t, err)
	assert.Equal(t, StreamControl, resp.Type)
	assert.Equal(t, "reply:ping", string(resp.Payload))
}

func TestQUICTransport_CallUnconnected(t *testing.T) {
	ctx := context.Background()
	node := MustNewQUICTransport("test-cluster-psk")
	defer node.Close()

	_, err := node.Call(ctx, "127.0.0.1:99999", &Message{Type: StreamControl, Payload: []byte("ping")})
	require.Error(t, err)
}

func TestQUICTransport_CallContextDeadlineDoesNotEvictHealthyConnection(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("test-cluster-psk")
	client := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	defer client.Close()

	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Connect(ctx, server.LocalAddr()))

	server.SetStreamHandler(func(reqMsg *Message) *Message {
		return &Message{Type: reqMsg.Type, Payload: []byte("ok")}
	})

	client.mu.RLock()
	cached := client.conns[server.LocalAddr()]
	client.mu.RUnlock()
	require.NotNil(t, cached)

	expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()

	_, err := client.Call(expired, server.LocalAddr(), &Message{Type: StreamControl, Payload: []byte("late")})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	client.mu.RLock()
	afterDeadline := client.conns[server.LocalAddr()]
	client.mu.RUnlock()
	require.Same(t, cached, afterDeadline, "caller-side timeout must not evict a healthy shared QUIC connection")

	resp, err := client.Call(ctx, server.LocalAddr(), &Message{Type: StreamControl, Payload: []byte("ping")})
	require.NoError(t, err)
	assert.Equal(t, "ok", string(resp.Payload))
}

func TestQUICTransport_AllowsBurstRPCStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server := MustNewQUICTransport("test-cluster-psk")
	client := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	defer client.Close()

	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Connect(ctx, server.LocalAddr()))

	const calls = 150
	var seen atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }

	server.SetStreamHandler(func(reqMsg *Message) *Message {
		seen.Add(1)
		<-release
		return &Message{Type: reqMsg.Type, Payload: []byte("ok")}
	})

	errCh := make(chan error, calls)
	var wg sync.WaitGroup
	wg.Add(calls)
	for i := 0; i < calls; i++ {
		go func() {
			defer wg.Done()
			_, err := client.Call(ctx, server.LocalAddr(), &Message{Type: StreamControl, Payload: []byte("ping")})
			errCh <- err
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		releaseAll()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	})

	require.Eventually(t, func() bool {
		return int(seen.Load()) == calls
	}, 2*time.Second, 20*time.Millisecond, "all burst RPCs should open streams without waiting for earlier responses")

	releaseAll()
	<-done
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
}

func TestQUICTransport_PSK_MatchingKey(t *testing.T) {
	ctx := context.Background()

	node1 := MustNewQUICTransport("secret-key")
	node2 := MustNewQUICTransport("secret-key")
	defer node1.Close()
	defer node2.Close()

	require.NoError(t, node1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, node2.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, node2.Connect(ctx, node1.LocalAddr()))

	msg := &Message{Type: StreamControl, Payload: []byte("authenticated")}
	require.NoError(t, node2.Send(ctx, node1.LocalAddr(), msg))

	select {
	case recv := <-node1.Receive():
		assert.Equal(t, "authenticated", string(recv.Message.Payload))
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

func TestQUICTransport_PSK_MismatchRejects(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("correct-key")
	client := MustNewQUICTransport("wrong-key")
	defer server.Close()
	defer client.Close()

	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))

	// Connect should fail due to ALPN mismatch
	err := client.Connect(ctx, server.LocalAddr())
	assert.Error(t, err, "PSK mismatch should reject connection")
}

func TestQUICTransport_SendToUnconnected(t *testing.T) {
	ctx := context.Background()
	node := MustNewQUICTransport("test-cluster-psk")
	defer node.Close()

	err := node.Send(ctx, "127.0.0.1:99999", &Message{Type: StreamControl, Payload: []byte("hello")})
	require.Error(t, err)
}

func TestQUICTransport_StreamIsolation(t *testing.T) {
	ctx := context.Background()

	node1 := MustNewQUICTransport("test-cluster-psk")
	node2 := MustNewQUICTransport("test-cluster-psk")
	defer node1.Close()
	defer node2.Close()

	require.NoError(t, node1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, node2.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, node2.Connect(ctx, node1.LocalAddr()))

	// Send a large data message followed by a small control message
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	go func() {
		_ = node2.Send(ctx, node1.LocalAddr(), &Message{Type: StreamData, Payload: largeData})
	}()

	time.Sleep(10 * time.Millisecond)
	require.NoError(t, node2.Send(ctx, node1.LocalAddr(), &Message{Type: StreamControl, Payload: []byte("priority")}))

	// Both messages should arrive (order may vary)
	received := 0
	timeout := time.After(10 * time.Second)
	for received < 2 {
		select {
		case <-node1.Receive():
			received++
		case <-timeout:
			t.Fatalf("timeout: received %d/2", received)
		}
	}
	assert.Equal(t, 2, received)
}

func TestQUICTransport_CallFlatBuffer(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("test-cluster-psk")
	client := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	defer client.Close()

	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Connect(ctx, server.LocalAddr()))

	// 서버: 받은 payload를 echo
	server.SetStreamHandler(func(reqMsg *Message) *Message {
		return NewResponse(reqMsg, append([]byte("echo:"), reqMsg.Payload...))
	})

	// FlatBuffer 빌드: byte vector 하나
	b := flatbuffers.NewBuilder(64)
	off := b.CreateByteVector([]byte("hello-fb"))
	b.Finish(off)

	fw := &FlatBuffersWriter{Typ: StreamData, Builder: b}
	resp, err := client.CallFlatBuffer(ctx, server.LocalAddr(), fw)
	require.NoError(t, err)
	assert.Equal(t, StreamData, resp.Type)
	assert.Contains(t, string(resp.Payload), "echo:")
}

func TestQUICTransport_CallFlatBuffer_ReleasesStreamCredit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	server := MustNewQUICTransport("test-cluster-psk")
	client := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	defer client.Close()

	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Connect(ctx, server.LocalAddr()))

	server.SetStreamHandler(func(reqMsg *Message) *Message {
		return NewResponse(reqMsg, []byte("ok"))
	})

	for i := 0; i < quicMaxRPCStreams+16; i++ {
		b := flatbuffers.NewBuilder(32)
		off := b.CreateByteVector([]byte("x"))
		b.Finish(off)

		resp, err := client.CallFlatBuffer(ctx, server.LocalAddr(), &FlatBuffersWriter{Typ: StreamData, Builder: b})
		require.NoError(t, err, "iteration %d", i)
		require.Equal(t, []byte("ok"), resp.Payload)
	}
}

func TestStreamClassOf(t *testing.T) {
	require.Equal(t, StreamClassControl, ClassOf(StreamControl))
	require.Equal(t, StreamClassMeta, ClassOf(StreamMetaRaft))
	require.Equal(t, StreamClassMeta, ClassOf(StreamMetaProposeForward))
	require.Equal(t, StreamClassData, ClassOf(StreamData))
	require.Equal(t, StreamClassBulk, ClassOf(StreamGroupForwardBody))
	require.Equal(t, StreamClassBulk, ClassOf(StreamGroupForwardRead))
}

func TestTrafficLimiter_BulkSaturationDoesNotBlockMeta(t *testing.T) {
	lim := NewTrafficLimiter(TrafficLimits{
		Bulk: 1,
	})
	releaseBulk, err := lim.Acquire(context.Background(), StreamGroupForwardBody)
	require.NoError(t, err)
	defer releaseBulk()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = lim.Acquire(ctx, StreamGroupForwardBody)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	releaseMeta, err := lim.Acquire(context.Background(), StreamMetaRaft)
	require.NoError(t, err)
	releaseMeta()
}

func TestQUICTransport_CallReturnsErrorForNonOKStatus(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("test-cluster-psk")
	client := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	defer client.Close()

	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Connect(ctx, server.LocalAddr()))

	server.SetStreamHandler(func(reqMsg *Message) *Message {
		return NewErrorResponse(reqMsg, StatusOverloaded, fmt.Errorf("busy"))
	})

	_, err := client.Call(ctx, server.LocalAddr(), &Message{Type: StreamMetaRaft, Payload: []byte("ping")})
	require.Error(t, err)
	require.Contains(t, err.Error(), "busy")
}

func TestQUICTransport_InboundBulkSaturationDoesNotBlockMeta(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("test-cluster-psk")
	client := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	defer client.Close()

	server.SetTrafficLimits(TrafficLimits{Bulk: 1})
	blockBulk := make(chan struct{})
	enteredBulk := make(chan struct{})
	var enteredOnce sync.Once

	server.HandleBody(StreamGroupForwardBody, func(req *Message, body io.Reader) *Message {
		enteredOnce.Do(func() { close(enteredBulk) })
		<-blockBulk
		_, _ = io.Copy(io.Discard, body)
		return NewResponse(req, []byte("bulk-ok"))
	})
	server.Handle(StreamMetaRaft, func(req *Message) *Message {
		return NewResponse(req, []byte("meta-ok"))
	})

	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Connect(ctx, server.LocalAddr()))

	firstDone := make(chan error, 1)
	go func() {
		_, err := client.CallWithBody(ctx, server.LocalAddr(), &Message{Type: StreamGroupForwardBody}, bytes.NewReader([]byte("first")))
		firstDone <- err
	}()

	select {
	case <-enteredBulk:
	case <-time.After(time.Second):
		t.Fatal("bulk handler did not start")
	}

	secondCtx, secondCancel := context.WithTimeout(ctx, 2*time.Second)
	defer secondCancel()
	_, err := client.CallWithBody(secondCtx, server.LocalAddr(), &Message{Type: StreamGroupForwardBody}, bytes.NewReader([]byte("second")))
	require.Error(t, err)

	resp, err := client.Call(ctx, server.LocalAddr(), &Message{Type: StreamMetaRaft, Payload: []byte("meta")})
	require.NoError(t, err)
	require.Equal(t, []byte("meta-ok"), resp.Payload)

	close(blockBulk)
	require.NoError(t, <-firstDone)
}

func TestQUICTransport_CallReadReturnsMetadataAndStreamingBody(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("test-cluster-psk")
	client := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	defer client.Close()

	body := bytes.Repeat([]byte("r"), 2*1024*1024)
	server.HandleRead(StreamGroupForwardRead, func(req *Message) (*Message, io.ReadCloser) {
		require.Equal(t, []byte("read-object"), req.Payload)
		return NewResponse(req, []byte("meta")), io.NopCloser(bytes.NewReader(body))
	})

	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Connect(ctx, server.LocalAddr()))

	resp, rc, err := client.CallRead(ctx, server.LocalAddr(), &Message{Type: StreamGroupForwardRead, Payload: []byte("read-object")})
	require.NoError(t, err)
	defer rc.Close()
	require.Equal(t, []byte("meta"), resp.Payload)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

func TestQUICTransport_CallReadContextDoesNotCancelReturnedBody(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("test-cluster-psk")
	client := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	defer client.Close()

	body := bytes.Repeat([]byte("r"), 64*1024)
	server.HandleRead(StreamGroupForwardRead, func(req *Message) (*Message, io.ReadCloser) {
		return NewResponse(req, []byte("meta")), io.NopCloser(bytes.NewReader(body))
	})

	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Connect(ctx, server.LocalAddr()))

	callCtx, cancel := context.WithCancel(ctx)
	resp, rc, err := client.CallRead(callCtx, server.LocalAddr(), &Message{Type: StreamGroupForwardRead})
	require.NoError(t, err)
	require.Equal(t, []byte("meta"), resp.Payload)
	cancel()
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

func TestMuxALPNConstant(t *testing.T) {
	tr := MustNewQUICTransport("test-cluster-psk")
	defer tr.Close()
	assert.Equal(t, ProtocolVersionMux, tr.MuxALPN())
}

func TestVersionHandshakeSuccess(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("test-cluster-psk")
	client := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	defer client.Close()

	connReady := make(chan struct{})
	server.SetMuxConnHandler(func(ctx context.Context, conn *quic.Conn) {
		close(connReady)
		<-ctx.Done()
	})

	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))

	conn, err := client.GetOrConnectMux(ctx, server.LocalAddr())
	require.NoError(t, err)
	require.NotNil(t, conn)

	select {
	case <-connReady:
	case <-time.After(3 * time.Second):
		t.Fatal("mux handler not called after capability exchange")
	}
}

// TestMixedVersionRejection verifies that a peer sending CE version=2
// receives an error response and the mux handler is not invoked.
func TestMixedVersionRejection(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()

	handlerCalled := make(chan struct{})
	server.SetMuxConnHandler(func(ctx context.Context, conn *quic.Conn) {
		close(handlerCalled)
		<-ctx.Done()
	})
	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))

	// Raw client: dial with mux ALPN but send CE version=2 (wrong)
	raw := MustNewQUICTransport("test-cluster-psk")
	defer raw.Close()

	tlsConf := raw.buildClientTLSConfig()
	tlsConf.NextProtos = []string{server.muxALPN()}
	conn, err := quic.DialAddr(ctx, server.LocalAddr(), tlsConf, defaultQUICConfig())
	require.NoError(t, err)
	defer conn.CloseWithError(0, "test done")

	stream, err := conn.OpenStreamSync(ctx)
	require.NoError(t, err)

	codec := &BinaryCodec{}
	require.NoError(t, codec.Encode(stream, &Message{
		Type:    StreamCapabilityExchange,
		Payload: []byte{0x02, 0x00}, // version=2, wrong
	}))

	resp, err := codec.Decode(stream)
	require.NoError(t, err)
	assert.NotEqual(t, StatusOK, resp.Status, "server must reject CE version mismatch")
	assert.Contains(t, string(resp.Payload), string(ceReasonVersionMismatch))

	// Mux handler must NOT be called
	select {
	case <-handlerCalled:
		t.Fatal("mux handler should not be called after CE version mismatch")
	case <-time.After(time.Second):
	}
}

// TestCapabilityExchangeTimeout verifies GetOrConnectMux fails and does not
// cache the connection when the server reads CE but never responds.
func TestCapabilityExchangeTimeout(t *testing.T) {
	ctx := context.Background()

	// Raw server: accepts mux conn, reads CE stream, never responds.
	rawServer := MustNewQUICTransport("test-cluster-psk")
	serverTLS := rawServer.buildServerTLSConfig()
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, defaultQUICConfig())
	require.NoError(t, err)
	defer listener.Close()

	serverAddr := listener.Addr().String()

	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		conn, err := listener.Accept(ctx)
		if err != nil {
			return
		}
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		codec := &BinaryCodec{}
		_, _ = codec.Decode(stream) // read CE but never write response
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	}()

	client := MustNewQUICTransport("test-cluster-psk")
	defer client.Close()
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))

	callCtx, cancel := context.WithTimeout(ctx, 600*time.Millisecond)
	defer cancel()

	_, err = client.GetOrConnectMux(callCtx, serverAddr)
	require.Error(t, err, "GetOrConnectMux must fail when CE response is not received")

	client.mu.RLock()
	_, cached := client.muxConns[serverAddr]
	client.mu.RUnlock()
	assert.False(t, cached, "timed-out CE must not cache the connection")
}

// TestCapabilityWrongFirstStream verifies the server sends an error and does
// not call muxHandler when the first mux stream type is not StreamCapabilityExchange.
func TestCapabilityWrongFirstStream(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()

	handlerCalled := make(chan struct{})
	server.SetMuxConnHandler(func(ctx context.Context, conn *quic.Conn) {
		close(handlerCalled)
		<-ctx.Done()
	})
	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))

	raw := MustNewQUICTransport("test-cluster-psk")
	defer raw.Close()

	tlsConf := raw.buildClientTLSConfig()
	tlsConf.NextProtos = []string{server.muxALPN()}
	conn, err := quic.DialAddr(ctx, server.LocalAddr(), tlsConf, defaultQUICConfig())
	require.NoError(t, err)
	defer conn.CloseWithError(0, "test done")

	// Open first stream with wrong type (StreamControl, not StreamCapabilityExchange)
	stream, err := conn.OpenStreamSync(ctx)
	require.NoError(t, err)

	codec := &BinaryCodec{}
	require.NoError(t, codec.Encode(stream, &Message{
		Type:    StreamControl,
		Payload: []byte("not-ce"),
	}))

	resp, err := codec.Decode(stream)
	require.NoError(t, err)
	assert.NotEqual(t, StatusOK, resp.Status, "server must reject wrong first stream type")

	// Mux handler must NOT be called
	select {
	case <-handlerCalled:
		t.Fatal("mux handler should not be called after CE stream type rejection")
	case <-time.After(time.Second):
	}
}

// TestCE_ShortPayload_Rejected verifies that a CE payload shorter than 2 bytes
// is rejected with payload_length reason (F1).
func TestCE_ShortPayload_Rejected(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()

	handlerCalled := make(chan struct{})
	server.SetMuxConnHandler(func(ctx context.Context, conn *quic.Conn) {
		close(handlerCalled)
		<-ctx.Done()
	})
	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))

	raw := MustNewQUICTransport("test-cluster-psk")
	defer raw.Close()

	tlsConf := raw.buildClientTLSConfig()
	tlsConf.NextProtos = []string{server.muxALPN()}
	conn, err := quic.DialAddr(ctx, server.LocalAddr(), tlsConf, defaultQUICConfig())
	require.NoError(t, err)
	defer conn.CloseWithError(0, "test done")

	stream, err := conn.OpenStreamSync(ctx)
	require.NoError(t, err)

	codec := &BinaryCodec{}
	// Send CE with only 1 byte payload (features byte missing).
	require.NoError(t, codec.Encode(stream, &Message{
		Type:    StreamCapabilityExchange,
		Payload: []byte{ceVersion},
	}))

	resp, err := codec.Decode(stream)
	require.NoError(t, err)
	assert.NotEqual(t, StatusOK, resp.Status, "server must reject short CE payload")
	assert.Contains(t, string(resp.Payload), string(ceReasonPayloadLength))

	// Mux handler must NOT be called.
	select {
	case <-handlerCalled:
		t.Fatal("mux handler should not be called after CE payload rejection")
	case <-time.After(time.Second):
	}
}

// TestCE_VersionMismatch_DistinctReason verifies the error payload contains
// the reason token version_mismatch (F3).
func TestCE_VersionMismatch_DistinctReason(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	server.SetMuxConnHandler(func(ctx context.Context, conn *quic.Conn) { <-ctx.Done() })
	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))

	raw := MustNewQUICTransport("test-cluster-psk")
	defer raw.Close()

	tlsConf := raw.buildClientTLSConfig()
	tlsConf.NextProtos = []string{server.muxALPN()}
	conn, err := quic.DialAddr(ctx, server.LocalAddr(), tlsConf, defaultQUICConfig())
	require.NoError(t, err)
	defer conn.CloseWithError(0, "test done")

	stream, err := conn.OpenStreamSync(ctx)
	require.NoError(t, err)

	codec := &BinaryCodec{}
	require.NoError(t, codec.Encode(stream, &Message{
		Type:    StreamCapabilityExchange,
		Payload: []byte{0xff, 0x00}, // bad version, valid length
	}))

	resp, err := codec.Decode(stream)
	require.NoError(t, err)
	assert.NotEqual(t, StatusOK, resp.Status)
	assert.Contains(t, string(resp.Payload), string(ceReasonVersionMismatch),
		"error payload must contain the reason token version_mismatch")
}

// TestCE_Counter_EmitsOnSuccess verifies grainfs_transport_ce_total increments
// for both dialer and acceptor on a successful handshake (F7).
func TestCE_Counter_EmitsOnSuccess(t *testing.T) {
	ctx := context.Background()

	dialerBefore := testutil.ToFloat64(
		metrics.TransportCECounter.WithLabelValues("dialer", "success", ""))
	acceptorBefore := testutil.ToFloat64(
		metrics.TransportCECounter.WithLabelValues("acceptor", "success", ""))

	server := MustNewQUICTransport("test-cluster-psk")
	client := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	defer client.Close()

	connReady := make(chan struct{})
	server.SetMuxConnHandler(func(ctx context.Context, conn *quic.Conn) {
		close(connReady)
		<-ctx.Done()
	})
	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))

	_, err := client.GetOrConnectMux(ctx, server.LocalAddr())
	require.NoError(t, err)
	<-connReady

	assert.Equal(t, dialerBefore+1,
		testutil.ToFloat64(metrics.TransportCECounter.WithLabelValues("dialer", "success", "")),
		"dialer success counter must increment by 1")
	assert.Equal(t, acceptorBefore+1,
		testutil.ToFloat64(metrics.TransportCECounter.WithLabelValues("acceptor", "success", "")),
		"acceptor success counter must increment by 1")
}

// TestCE_Counter_EmitsOnFailure verifies grainfs_transport_ce_total records a
// failure with the correct reason label when the peer sends a wrong version (F7).
func TestCE_Counter_EmitsOnFailure(t *testing.T) {
	ctx := context.Background()

	acceptorBefore := testutil.ToFloat64(
		metrics.TransportCECounter.WithLabelValues("acceptor", "failure", string(ceReasonVersionMismatch)))

	server := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	server.SetMuxConnHandler(func(ctx context.Context, conn *quic.Conn) { <-ctx.Done() })
	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))

	raw := MustNewQUICTransport("test-cluster-psk")
	defer raw.Close()

	tlsConf := raw.buildClientTLSConfig()
	tlsConf.NextProtos = []string{server.muxALPN()}
	conn, err := quic.DialAddr(ctx, server.LocalAddr(), tlsConf, defaultQUICConfig())
	require.NoError(t, err)
	defer conn.CloseWithError(0, "test done")

	stream, err := conn.OpenStreamSync(ctx)
	require.NoError(t, err)

	codec := &BinaryCodec{}
	require.NoError(t, codec.Encode(stream, &Message{
		Type:    StreamCapabilityExchange,
		Payload: []byte{0x02, 0x00},
	}))
	resp, err := codec.Decode(stream)
	require.NoError(t, err)
	assert.NotEqual(t, StatusOK, resp.Status)

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.TransportCECounter.WithLabelValues("acceptor", "failure", string(ceReasonVersionMismatch))) == acceptorBefore+1
	}, 2*time.Second, 10*time.Millisecond, "acceptor version_mismatch failure counter must increment by 1")
}

// TestCE_FeatureBit_Unsupported_Rejected verifies that a peer sending a
// reserved feature bit is rejected with feature_unsupported reason (F2).
func TestCE_FeatureBit_Unsupported_Rejected(t *testing.T) {
	ctx := context.Background()

	server := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()

	handlerCalled := make(chan struct{})
	server.SetMuxConnHandler(func(ctx context.Context, conn *quic.Conn) {
		close(handlerCalled)
		<-ctx.Done()
	})
	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))

	raw := MustNewQUICTransport("test-cluster-psk")
	defer raw.Close()

	tlsConf := raw.buildClientTLSConfig()
	tlsConf.NextProtos = []string{server.muxALPN()}
	conn, err := quic.DialAddr(ctx, server.LocalAddr(), tlsConf, defaultQUICConfig())
	require.NoError(t, err)
	defer conn.CloseWithError(0, "test done")

	stream, err := conn.OpenStreamSync(ctx)
	require.NoError(t, err)

	codec := &BinaryCodec{}
	// features=0x80 — reserved bit set.
	require.NoError(t, codec.Encode(stream, &Message{
		Type:    StreamCapabilityExchange,
		Payload: []byte{ceVersion, 0x80},
	}))

	resp, err := codec.Decode(stream)
	require.NoError(t, err)
	assert.NotEqual(t, StatusOK, resp.Status, "server must reject unsupported feature bits")
	assert.Contains(t, string(resp.Payload), string(ceReasonFeatureUnsup))

	select {
	case <-handlerCalled:
		t.Fatal("mux handler should not be called after feature rejection")
	case <-time.After(time.Second):
	}
}

// TestCE_ConcurrentDial_Dedup verifies that N concurrent GetOrConnectMux calls
// to the same address return the same cached connection (F6).
func TestCE_ConcurrentDial_Dedup(t *testing.T) {
	ctx := context.Background()

	var handlerCalls atomic.Int32
	server := MustNewQUICTransport("test-cluster-psk")
	defer server.Close()
	server.SetMuxConnHandler(func(ctx context.Context, conn *quic.Conn) {
		handlerCalls.Add(1)
		<-ctx.Done()
	})
	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))

	client := MustNewQUICTransport("test-cluster-psk")
	defer client.Close()
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))

	addr := server.LocalAddr()
	const N = 8
	conns := make([]*quic.Conn, N)
	errs := make([]error, N)

	var wg sync.WaitGroup
	for i := range N {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conns[i], errs[i] = client.GetOrConnectMux(ctx, addr)
		}(i)
	}
	wg.Wait()

	for i := range N {
		require.NoError(t, errs[i], "goroutine %d must not error", i)
	}

	// All goroutines must get back the same conn pointer.
	first := conns[0]
	for i := 1; i < N; i++ {
		assert.Equal(t, first, conns[i], "goroutine %d returned a different conn", i)
	}
}
