package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/transport"
)

// mockTransport implements transport.Transport for gossip tests.
type mockTransport struct {
	mu   sync.Mutex
	sent []sentMsg
	recv chan *transport.ReceivedMessage
}

type sentMsg struct {
	to  string
	msg *transport.Message
}

func newMockTransport() *mockTransport {
	return &mockTransport{recv: make(chan *transport.ReceivedMessage, 64)}
}

func (m *mockTransport) Listen(_ context.Context, _ string) error { return nil }
func (m *mockTransport) Connect(_ context.Context, _ string) error { return nil }
func (m *mockTransport) Close() error                              { return nil }
func (m *mockTransport) Receive() <-chan *transport.ReceivedMessage { return m.recv }

func (m *mockTransport) Send(_ context.Context, addr string, msg *transport.Message) error {
	m.mu.Lock()
	m.sent = append(m.sent, sentMsg{to: addr, msg: msg})
	m.mu.Unlock()
	return nil
}

func (m *mockTransport) SentTo(addr string) []*transport.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*transport.Message
	for _, s := range m.sent {
		if s.to == addr {
			out = append(out, s.msg)
		}
	}
	return out
}

func (m *mockTransport) AllSent() []sentMsg {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]sentMsg, len(m.sent))
	copy(out, m.sent)
	return out
}

// inject simulates an incoming message from a remote node.
func (m *mockTransport) inject(from string, msg *transport.Message) {
	m.recv <- &transport.ReceivedMessage{From: from, Message: msg}
}

// statsGossipMsg encodes a NodeStatsMsg as a StreamAdmin transport.Message.
func statsGossipMsg(ns NodeStats) *transport.Message {
	b := flatbuffers.NewBuilder(64)
	nodeIDOff := b.CreateString(ns.NodeID)
	clusterpb.NodeStatsMsgStart(b)
	clusterpb.NodeStatsMsgAddNodeId(b, nodeIDOff)
	clusterpb.NodeStatsMsgAddDiskUsedPct(b, ns.DiskUsedPct)
	clusterpb.NodeStatsMsgAddDiskAvailBytes(b, ns.DiskAvailBytes)
	clusterpb.NodeStatsMsgAddRequestsPerSec(b, ns.RequestsPerSec)
	root := clusterpb.NodeStatsMsgEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	payload := make([]byte, len(raw))
	copy(payload, raw)
	return &transport.Message{Type: transport.StreamAdmin, Payload: payload}
}

// --- GossipSender tests ---

func TestGossipSender_BroadcastsToPeers(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 50.0})
	peers := []string{"node-b:9000", "node-c:9000"}

	sender := NewGossipSender("node-a", peers, tr, store, 30*time.Second)

	// Broadcast once synchronously
	sender.broadcastOnce(context.Background())

	for _, peer := range peers {
		msgs := tr.SentTo(peer)
		require.Len(t, msgs, 1, "expected exactly 1 message to %s", peer)
		assert.Equal(t, transport.StreamAdmin, msgs[0].Type)
	}
}

func TestGossipSender_PayloadDecodable(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 70.0, RequestsPerSec: 120.0})
	peers := []string{"node-b:9000"}

	sender := NewGossipSender("node-a", peers, tr, store, 30*time.Second)
	sender.broadcastOnce(context.Background())

	msgs := tr.SentTo("node-b:9000")
	require.Len(t, msgs, 1)

	pb := clusterpb.GetRootAsNodeStatsMsg(msgs[0].Payload, 0)
	assert.Equal(t, "node-a", string(pb.NodeId()))
	assert.Equal(t, 70.0, pb.DiskUsedPct())
	assert.Equal(t, 120.0, pb.RequestsPerSec())
}

func TestGossipSender_SkipsBroadcastWhenNoLocalStats(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	// Do NOT set any stats for "node-a" → cold-start scenario

	sender := NewGossipSender("node-a", []string{"node-b:9000"}, tr, store, 30*time.Second)
	sender.broadcastOnce(context.Background())

	assert.Empty(t, tr.AllSent(), "should not broadcast DiskUsedPct=0 before local stats are ready")
}

func TestGossipSender_NoPeers(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)

	sender := NewGossipSender("node-a", nil, tr, store, 30*time.Second)
	sender.broadcastOnce(context.Background()) // should not panic

	assert.Empty(t, tr.AllSent())
}

// --- GossipReceiver tests ---

func TestGossipReceiver_UpdatesStore(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recv := NewGossipReceiver(tr, store)
	go recv.Run(ctx)

	tr.inject("node-b:9000", statsGossipMsg(NodeStats{
		NodeID:         "node-b",
		DiskUsedPct:    55.0,
		RequestsPerSec: 80.0,
	}))

	require.Eventually(t, func() bool {
		_, ok := store.Get("node-b")
		return ok
	}, 500*time.Millisecond, 10*time.Millisecond)

	stats, _ := store.Get("node-b")
	assert.Equal(t, 55.0, stats.DiskUsedPct)
	assert.Equal(t, 80.0, stats.RequestsPerSec)
}

func TestGossipReceiver_IgnoresNonAdminMessages(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recv := NewGossipReceiver(tr, store)
	go recv.Run(ctx)

	// inject a non-admin message (StreamData)
	tr.recv <- &transport.ReceivedMessage{
		From:    "node-b:9000",
		Message: &transport.Message{Type: transport.StreamData, Payload: []byte("not-stats")},
	}

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, store.Len(), "non-admin message should not update store")
}

func TestGossipReceiver_MultipleNodes(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recv := NewGossipReceiver(tr, store)
	go recv.Run(ctx)

	tr.inject("node-b:9000", statsGossipMsg(NodeStats{NodeID: "node-b", DiskUsedPct: 40.0}))
	tr.inject("node-c:9000", statsGossipMsg(NodeStats{NodeID: "node-c", DiskUsedPct: 60.0}))

	require.Eventually(t, func() bool {
		return store.Len() == 2
	}, 500*time.Millisecond, 10*time.Millisecond)

	sb, _ := store.Get("node-b")
	sc, _ := store.Get("node-c")
	assert.Equal(t, 40.0, sb.DiskUsedPct)
	assert.Equal(t, 60.0, sc.DiskUsedPct)
}

func TestGossipReceiver_DropsNodeIdSpoofing(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recv := NewGossipReceiver(tr, store)
	go recv.Run(ctx)

	// Inject a message claiming to be "node-a" but arriving from "node-b:9000"
	spoofed := statsGossipMsg(NodeStats{NodeID: "node-a", DiskUsedPct: 99.0})
	tr.recv <- &transport.ReceivedMessage{From: "node-b:9000", Message: spoofed}

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, store.Len(), "spoofed NodeId should be dropped")
}

func TestGossipReceiver_AcceptsMatchingNodeId(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recv := NewGossipReceiver(tr, store)
	go recv.Run(ctx)

	// NodeId matches the host part of From address
	valid := statsGossipMsg(NodeStats{NodeID: "node-b", DiskUsedPct: 55.0})
	tr.recv <- &transport.ReceivedMessage{From: "node-b:9000", Message: valid}

	require.Eventually(t, func() bool {
		_, ok := store.Get("node-b")
		return ok
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func TestNodeIDMatchesFrom(t *testing.T) {
	tests := []struct {
		nodeID string
		from   string
		want   bool
	}{
		// IP nodeID — strict validation
		{"192.168.1.1", "192.168.1.1:9000", true},
		{"192.168.1.1", "192.168.1.2:9000", false},
		// Hostname nodeID + hostname from — validated by host comparison
		{"node-b", "node-b:9000", true},
		{"node-a", "node-b:9000", false},
		// Hostname nodeID + IP from — tightened: DNS lookup required.
		// A made-up hostname like "node-a" won't resolve, so the path that
		// used to blanket-accept now rejects — closing the spoofing gap.
		{"node-a", "192.168.1.1:9000", false},
		// Full match (nodeID includes port)
		{"node-b:9000", "node-b:9000", true},
		// Localhost-to-loopback is a real hostname+IP pair we can verify:
		// DNS resolves "localhost" → 127.0.0.1 (on any sane machine).
		{"localhost", "127.0.0.1:9000", true},
	}
	for _, tc := range tests {
		got := nodeIDMatchesFrom(tc.nodeID, tc.from)
		assert.Equalf(t, tc.want, got, "nodeIDMatchesFrom(%q, %q)", tc.nodeID, tc.from)
	}
}

// TestGossipReceiver_UnknownFieldTolerance verifies that gossip messages containing
// fields unknown to this node (e.g., added in a newer version during rolling upgrade)
// are parsed without error and that known fields are decoded correctly.
func TestGossipReceiver_UnknownFieldTolerance(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recv := NewGossipReceiver(tr, store)
	go recv.Run(ctx)

	// FlatBuffers natively ignores unknown fields; just send a valid message.
	tr.inject("node-b:9000", statsGossipMsg(NodeStats{NodeID: "node-b", DiskUsedPct: 42.0}))

	require.Eventually(t, func() bool {
		_, ok := store.Get("node-b")
		return ok
	}, 500*time.Millisecond, 10*time.Millisecond)

	stats, _ := store.Get("node-b")
	assert.Equal(t, 42.0, stats.DiskUsedPct, "known fields intact despite unknown field")
}

func TestGossipReceiver_StopsOnContextCancel(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	recv := NewGossipReceiver(tr, store)

	done := make(chan struct{})
	go func() {
		recv.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("GossipReceiver did not stop after context cancel")
	}
}
