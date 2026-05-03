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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQUICTransport_SendReceive(t *testing.T) {
	ctx := context.Background()

	node1 := NewQUICTransport()
	node2 := NewQUICTransport()
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

	sender := NewQUICTransport()
	receiver := NewQUICTransport()
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
		nodes[i] = NewQUICTransport()
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

	sender := NewQUICTransport()
	receiver := NewQUICTransport()
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

	server := NewQUICTransport()
	client := NewQUICTransport()
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
	node := NewQUICTransport()
	defer node.Close()

	_, err := node.Call(ctx, "127.0.0.1:99999", &Message{Type: StreamControl, Payload: []byte("ping")})
	require.Error(t, err)
}

func TestQUICTransport_CallContextDeadlineDoesNotEvictHealthyConnection(t *testing.T) {
	ctx := context.Background()

	server := NewQUICTransport()
	client := NewQUICTransport()
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

	server := NewQUICTransport()
	client := NewQUICTransport()
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

	node1 := NewQUICTransport("secret-key")
	node2 := NewQUICTransport("secret-key")
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

	server := NewQUICTransport("correct-key")
	client := NewQUICTransport("wrong-key")
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
	node := NewQUICTransport()
	defer node.Close()

	err := node.Send(ctx, "127.0.0.1:99999", &Message{Type: StreamControl, Payload: []byte("hello")})
	require.Error(t, err)
}

func TestQUICTransport_StreamIsolation(t *testing.T) {
	ctx := context.Background()

	node1 := NewQUICTransport()
	node2 := NewQUICTransport()
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

	server := NewQUICTransport()
	client := NewQUICTransport()
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

func TestStreamClassOf(t *testing.T) {
	require.Equal(t, StreamClassControl, ClassOf(StreamControl))
	require.Equal(t, StreamClassMeta, ClassOf(StreamMetaRaft))
	require.Equal(t, StreamClassMeta, ClassOf(StreamMetaProposeForward))
	require.Equal(t, StreamClassData, ClassOf(StreamData))
	require.Equal(t, StreamClassBulk, ClassOf(StreamGroupForwardBody))
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

	server := NewQUICTransport()
	client := NewQUICTransport()
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

	server := NewQUICTransport()
	client := NewQUICTransport()
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
