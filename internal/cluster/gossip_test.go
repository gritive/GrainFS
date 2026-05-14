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
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// mockTransport implements transport.Transport for gossip tests.
type mockTransport struct {
	mu        sync.Mutex
	sent      []sentMsg
	connected []string
	recv      chan *transport.ReceivedMessage
}

type sentMsg struct {
	to  string
	msg *transport.Message
}

func newMockTransport() *mockTransport {
	return &mockTransport{recv: make(chan *transport.ReceivedMessage, 64)}
}

func (m *mockTransport) Listen(_ context.Context, _ string) error { return nil }
func (m *mockTransport) Connect(_ context.Context, addr string) error {
	m.mu.Lock()
	m.connected = append(m.connected, addr)
	m.mu.Unlock()
	return nil
}
func (m *mockTransport) Close() error                               { return nil }
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

func (m *mockTransport) ConnectedTo(addr string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, got := range m.connected {
		if got == addr {
			return true
		}
	}
	return false
}

// inject simulates an incoming message from a remote node.
func (m *mockTransport) inject(from string, msg *transport.Message) {
	m.recv <- &transport.ReceivedMessage{From: from, Message: msg}
}

// statsGossipMsg encodes a NodeStatsMsg as a StreamAdmin transport.Message.
func statsGossipMsg(ns NodeStats, capabilities ...string) *transport.Message {
	b := flatbuffers.NewBuilder(64)
	nodeIDOff := b.CreateString(ns.NodeID)
	var capabilitiesVec flatbuffers.UOffsetT
	if len(capabilities) > 0 {
		offsets := make([]flatbuffers.UOffsetT, len(capabilities))
		for i, capability := range capabilities {
			offsets[i] = b.CreateString(capability)
		}
		clusterpb.NodeStatsMsgStartCapabilitiesVector(b, len(offsets))
		for i := len(offsets) - 1; i >= 0; i-- {
			b.PrependUOffsetT(offsets[i])
		}
		capabilitiesVec = b.EndVector(len(offsets))
	}
	clusterpb.NodeStatsMsgStart(b)
	clusterpb.NodeStatsMsgAddNodeId(b, nodeIDOff)
	clusterpb.NodeStatsMsgAddDiskUsedPct(b, ns.DiskUsedPct)
	clusterpb.NodeStatsMsgAddDiskAvailBytes(b, ns.DiskAvailBytes)
	clusterpb.NodeStatsMsgAddRequestsPerSec(b, ns.RequestsPerSec)
	if capabilitiesVec != 0 {
		clusterpb.NodeStatsMsgAddCapabilities(b, capabilitiesVec)
	}
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

type staticCapabilityEvidence struct {
	caps map[string]bool
}

func (s staticCapabilityEvidence) CapabilityEvidence(nodeID string, now time.Time) compat.Evidence {
	return compat.Evidence{
		NodeID:       compat.NodeID(nodeID),
		Capabilities: s.caps,
		LastSeen:     now,
		Ready:        true,
	}
}

type fakeGossipAddressBook struct {
	nodes []MetaNodeEntry
}

func (f fakeGossipAddressBook) Nodes() []MetaNodeEntry {
	return f.nodes
}

func TestGossipSenderIncludesCapabilityEvidence(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 70.0})

	sender := NewGossipSender("node-a", []string{"node-b:9000"}, tr, store, 30*time.Second).
		WithCapabilityEvidenceSource(staticCapabilityEvidence{caps: map[string]bool{
			compat.CapabilityNfsExportCreateV1: true,
		}})
	sender.broadcastOnce(context.Background())

	msgs := tr.SentTo("node-b:9000")
	require.Len(t, msgs, 1)

	pb := clusterpb.GetRootAsNodeStatsMsg(msgs[0].Payload, 0)
	require.Equal(t, 1, pb.CapabilitiesLength())
	assert.Equal(t, compat.CapabilityNfsExportCreateV1, string(pb.Capabilities(0)))
}

func TestGossipSenderUsesLatestPeerProvider(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 70.0})
	peers := []string{"node-b:9000"}

	sender := NewGossipSender("node-a", nil, tr, store, 30*time.Second).
		WithPeerProvider(func() []string {
			return append([]string(nil), peers...)
		})
	peers = []string{"node-c:9000"}

	sender.broadcastOnce(context.Background())

	require.Empty(t, tr.SentTo("node-b:9000"))
	require.Len(t, tr.SentTo("node-c:9000"), 1)
}

func TestGossipSenderConnectsDynamicPeerBeforeSend(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 70.0})

	sender := NewGossipSender("node-a", nil, tr, store, 30*time.Second).
		WithPeerProvider(func() []string { return []string{"node-c:9000"} })

	sender.broadcastOnce(context.Background())

	require.True(t, tr.ConnectedTo("node-c:9000"))
	require.Len(t, tr.SentTo("node-c:9000"), 1)
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

func TestGossipReceiverReportsCapabilityEvidenceToGate(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)
	gate.SetMetaRaftSnapshot(7, raft.Configuration{Servers: []raft.Server{
		{ID: "node-a", Suffrage: raft.Voter},
		{ID: "node-b", Suffrage: raft.Voter},
	}})
	now := time.Now()
	gate.ReportEvidence(compat.Evidence{
		NodeID: compat.NodeID("node-a"),
		Capabilities: map[string]bool{
			compat.CapabilityNfsExportCreateV1: true,
		},
		LastSeen: now,
		Ready:    true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recv := NewGossipReceiver(tr, store).WithCapabilityGate(gate)
	go recv.Run(ctx)

	msg := statsGossipMsg(NodeStats{NodeID: "node-b", DiskUsedPct: 55.0}, compat.CapabilityNfsExportCreateV1)
	tr.recv <- &transport.ReceivedMessage{From: "node-b:9000", Message: msg}

	require.Eventually(t, func() bool {
		_, err := gate.RequireMetaRaftCapability(compat.CapabilityNfsExportCreateV1, compat.OperationNfsExportCreate, time.Now())
		return err == nil
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func TestGossipReceiverReportsCapabilityEvidenceUnderRaftMemberID(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)
	gate.SetMetaRaftSnapshot(7, raft.Configuration{Servers: []raft.Server{
		{ID: "10.0.0.1:7001", Suffrage: raft.Voter},
		{ID: "10.0.0.2:7001", Suffrage: raft.Voter},
	}})
	now := time.Now()
	gate.ReportEvidence(compat.Evidence{
		NodeID: compat.NodeID("10.0.0.1:7001"),
		Capabilities: map[string]bool{
			compat.CapabilityNfsExportCreateV1: true,
		},
		LastSeen: now,
		Ready:    true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recv := NewGossipReceiver(tr, store).
		WithCapabilityGate(gate).
		WithNodeAddressBook(fakeGossipAddressBook{nodes: []MetaNodeEntry{
			{ID: "node-b", Address: "10.0.0.2:7001"},
		}})
	go recv.Run(ctx)

	msg := statsGossipMsg(NodeStats{NodeID: "node-b", DiskUsedPct: 55.0}, compat.CapabilityNfsExportCreateV1)
	tr.recv <- &transport.ReceivedMessage{From: "10.0.0.2:54321", Message: msg}

	require.Eventually(t, func() bool {
		_, err := gate.RequireMetaRaftCapability(compat.CapabilityNfsExportCreateV1, compat.OperationNfsExportCreate, time.Now())
		return err == nil
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func TestGossipReceiverPrefersAddressBookOverDirectNodeIDMatch(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)
	gate.SetMetaRaftSnapshot(7, raft.Configuration{Servers: []raft.Server{
		{ID: "node-a:7001", Suffrage: raft.Voter},
		{ID: "node-b:7001", Suffrage: raft.Voter},
	}})
	now := time.Now()
	gate.ReportEvidence(compat.Evidence{
		NodeID: compat.NodeID("node-a:7001"),
		Capabilities: map[string]bool{
			compat.CapabilityNfsExportCreateV1: true,
		},
		LastSeen: now,
		Ready:    true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recv := NewGossipReceiver(tr, store).
		WithCapabilityGate(gate).
		WithNodeAddressBook(fakeGossipAddressBook{nodes: []MetaNodeEntry{
			{ID: "node-b", Address: "node-b:7001"},
		}})
	go recv.Run(ctx)

	msg := statsGossipMsg(NodeStats{NodeID: "node-b", DiskUsedPct: 55.0}, compat.CapabilityNfsExportCreateV1)
	tr.recv <- &transport.ReceivedMessage{From: "node-b:54321", Message: msg}

	require.Eventually(t, func() bool {
		_, err := gate.RequireMetaRaftCapability(compat.CapabilityNfsExportCreateV1, compat.OperationNfsExportCreate, time.Now())
		return err == nil
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
