package transport

import (
	"context"
	"fmt"
	"sync"
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
		return &Message{Type: reqMsg.Type, Payload: append([]byte("reply:"), reqMsg.Payload...)}
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
		return &Message{Type: reqMsg.Type, Payload: append([]byte("echo:"), reqMsg.Payload...)}
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
