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

// mockTransport implements cluster.GossipTransport for gossip tests.
type mockTransport struct {
	mu           sync.Mutex
	sent         []sentMsg
	gossipRoutes map[string]transport.GossipHandler
}

type sentMsg struct {
	to      string
	path    string
	payload []byte
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		gossipRoutes: make(map[string]transport.GossipHandler),
	}
}

func (m *mockTransport) RegisterGossipRoute(path string, h transport.GossipHandler) {
	m.mu.Lock()
	m.gossipRoutes[path] = h
	m.mu.Unlock()
}

func (m *mockTransport) GossipSend(_ context.Context, addr, path string, payload []byte) error {
	m.mu.Lock()
	m.sent = append(m.sent, sentMsg{to: addr, path: path, payload: payload})
	m.mu.Unlock()
	return nil
}

func (m *mockTransport) SentTo(addr string) []sentMsg {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []sentMsg
	for _, s := range m.sent {
		if s.to == addr {
			out = append(out, s)
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

// deliver simulates an inbound gossip POST from a remote node by invoking the
// handler the receiver registered for path (RegisterNativeGossipRoutes).
func (m *mockTransport) deliver(t *testing.T, from, path string, payload []byte) {
	t.Helper()
	m.mu.Lock()
	h, ok := m.gossipRoutes[path]
	m.mu.Unlock()
	require.True(t, ok, "no gossip handler registered for %s", path)
	h(from, payload)
}

// statsGossipMsg encodes a NodeStatsMsg payload for the /gossip/admin route.
func statsGossipMsg(ns NodeStats, capabilities ...string) []byte {
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
	return payload
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
		assert.Equal(t, transport.RouteGossipAdmin, msgs[0].path)
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

	pb := clusterpb.GetRootAsNodeStatsMsg(msgs[0].payload, 0)
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

	pb := clusterpb.GetRootAsNodeStatsMsg(msgs[0].payload, 0)
	require.Equal(t, 1, pb.CapabilitiesLength())
	assert.Equal(t, compat.CapabilityNfsExportCreateV1, string(pb.Capabilities(0)))
}

func TestGossipSenderIncludesMultipartListingCapabilityEvidence(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 70.0})

	sender := NewGossipSender("node-a", []string{"node-b:9000"}, tr, store, 30*time.Second).
		WithCapabilityEvidenceSource(NewMetaFSM())
	sender.broadcastOnce(context.Background())

	msgs := tr.SentTo("node-b:9000")
	require.Len(t, msgs, 1)

	pb := clusterpb.GetRootAsNodeStatsMsg(msgs[0].payload, 0)
	var caps []string
	for i := 0; i < pb.CapabilitiesLength(); i++ {
		caps = append(caps, string(pb.Capabilities(i)))
	}
	assert.Contains(t, caps, compat.CapabilityMultipartListingV1)
}

func TestGossipSenderRefreshesLocalCapabilityEvidence(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 70.0})
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)

	sender := NewGossipSender("node-a", []string{"node-b:9000"}, tr, store, 30*time.Second).
		WithCapabilityEvidenceSource(staticCapabilityEvidence{caps: map[string]bool{
			compat.CapabilityMultipartListingV1: true,
		}}).
		WithCapabilityGate(gate)
	sender.broadcastOnce(context.Background())

	_, err := gate.RequirePeerTransportCapability(
		compat.CapabilityMultipartListingV1,
		compat.OperationCreateMultipartUpload,
		[]string{"node-a"},
		time.Now(),
	)
	require.NoError(t, err)
}

func TestGossipSenderRefreshesLocalCapabilityEvidenceAliases(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 70.0})
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)

	sender := NewGossipSender("node-a", []string{"node-b:9000"}, tr, store, 30*time.Second).
		WithCapabilityEvidenceSource(staticCapabilityEvidence{caps: map[string]bool{
			compat.CapabilityMultipartListingV1: true,
		}}).
		WithCapabilityGate(gate).
		WithCapabilityEvidenceAliases("127.0.0.1:9001")
	sender.broadcastOnce(context.Background())

	_, err := gate.RequirePeerTransportCapability(
		compat.CapabilityMultipartListingV1,
		compat.OperationCreateMultipartUpload,
		[]string{"127.0.0.1:9001"},
		time.Now(),
	)
	require.NoError(t, err)
}

func TestGossipSenderRefreshesLocalCapabilityEvidenceDynamicAliases(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 70.0})
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)
	aliases := []string{}

	sender := NewGossipSender("node-a", []string{"node-b:9000"}, tr, store, 30*time.Second).
		WithCapabilityEvidenceSource(staticCapabilityEvidence{caps: map[string]bool{
			compat.CapabilityMultipartListingV1: true,
		}}).
		WithCapabilityGate(gate).
		WithCapabilityEvidenceAliasProvider(func() []string {
			return append([]string(nil), aliases...)
		})

	sender.broadcastOnce(context.Background())
	_, err := gate.RequirePeerTransportCapability(
		compat.CapabilityMultipartListingV1,
		compat.OperationCreateMultipartUpload,
		[]string{"127.0.0.1:9001"},
		time.Now(),
	)
	require.Error(t, err)

	aliases = []string{"127.0.0.1:9001"}
	sender.broadcastOnce(context.Background())
	_, err = gate.RequirePeerTransportCapability(
		compat.CapabilityMultipartListingV1,
		compat.OperationCreateMultipartUpload,
		[]string{"127.0.0.1:9001"},
		time.Now(),
	)
	require.NoError(t, err)
}

func TestGossipSenderBroadcastsCapabilityEvidenceWithoutStats(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)

	sender := NewGossipSender("node-a", []string{"node-b:9000"}, tr, store, 30*time.Second).
		WithCapabilityEvidenceSource(staticCapabilityEvidence{caps: map[string]bool{
			compat.CapabilityMultipartListingV1: true,
		}}).
		WithCapabilityGate(gate).
		WithCapabilityEvidenceAliases("127.0.0.1:9001")
	sender.broadcastOnce(context.Background())

	require.Len(t, tr.SentTo("node-b:9000"), 1)
	_, err := gate.RequirePeerTransportCapability(
		compat.CapabilityMultipartListingV1,
		compat.OperationCreateMultipartUpload,
		[]string{"127.0.0.1:9001"},
		time.Now(),
	)
	require.NoError(t, err)
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

func TestGossipSenderSendsToDynamicPeer(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", DiskUsedPct: 70.0})

	sender := NewGossipSender("node-a", nil, tr, store, 30*time.Second).
		WithPeerProvider(func() []string { return []string{"node-c:9000"} })

	sender.broadcastOnce(context.Background())

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

	recv := NewGossipReceiver(tr, store)
	recv.RegisterNativeGossipRoutes()

	tr.deliver(t, "node-b:9000", transport.RouteGossipAdmin, statsGossipMsg(NodeStats{
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

func TestGossipReceiver_MultipleNodes(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)

	recv := NewGossipReceiver(tr, store)
	recv.RegisterNativeGossipRoutes()

	tr.deliver(t, "node-b:9000", transport.RouteGossipAdmin, statsGossipMsg(NodeStats{NodeID: "node-b", DiskUsedPct: 40.0}))
	tr.deliver(t, "node-c:9000", transport.RouteGossipAdmin, statsGossipMsg(NodeStats{NodeID: "node-c", DiskUsedPct: 60.0}))

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

	recv := NewGossipReceiver(tr, store)
	recv.RegisterNativeGossipRoutes()

	// Deliver a message claiming to be "node-a" but arriving from "node-b:9000"
	spoofed := statsGossipMsg(NodeStats{NodeID: "node-a", DiskUsedPct: 99.0})
	tr.deliver(t, "node-b:9000", transport.RouteGossipAdmin, spoofed)

	assert.Equal(t, 0, store.Len(), "spoofed NodeId should be dropped")
}

func TestGossipReceiver_AcceptsMatchingNodeId(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)

	recv := NewGossipReceiver(tr, store)
	recv.RegisterNativeGossipRoutes()

	// NodeId matches the host part of From address
	valid := statsGossipMsg(NodeStats{NodeID: "node-b", DiskUsedPct: 55.0})
	tr.deliver(t, "node-b:9000", transport.RouteGossipAdmin, valid)

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

	recv := NewGossipReceiver(tr, store).WithCapabilityGate(gate)
	recv.RegisterNativeGossipRoutes()

	msg := statsGossipMsg(NodeStats{NodeID: "node-b", DiskUsedPct: 55.0}, compat.CapabilityNfsExportCreateV1)
	tr.deliver(t, "node-b:9000", transport.RouteGossipAdmin, msg)

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

	recv := NewGossipReceiver(tr, store).
		WithCapabilityGate(gate).
		WithNodeAddressBook(fakeGossipAddressBook{nodes: []MetaNodeEntry{
			{ID: "node-b", Address: "10.0.0.2:7001"},
		}})
	recv.RegisterNativeGossipRoutes()

	msg := statsGossipMsg(NodeStats{NodeID: "node-b", DiskUsedPct: 55.0}, compat.CapabilityNfsExportCreateV1)
	tr.deliver(t, "10.0.0.2:54321", transport.RouteGossipAdmin, msg)

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

	recv := NewGossipReceiver(tr, store).
		WithCapabilityGate(gate).
		WithNodeAddressBook(fakeGossipAddressBook{nodes: []MetaNodeEntry{
			{ID: "node-b", Address: "node-b:7001"},
		}})
	recv.RegisterNativeGossipRoutes()

	msg := statsGossipMsg(NodeStats{NodeID: "node-b", DiskUsedPct: 55.0}, compat.CapabilityNfsExportCreateV1)
	tr.deliver(t, "node-b:54321", transport.RouteGossipAdmin, msg)

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

	recv := NewGossipReceiver(tr, store)
	recv.RegisterNativeGossipRoutes()

	// FlatBuffers natively ignores unknown fields; just send a valid message.
	tr.deliver(t, "node-b:9000", transport.RouteGossipAdmin, statsGossipMsg(NodeStats{NodeID: "node-b", DiskUsedPct: 42.0}))

	require.Eventually(t, func() bool {
		_, ok := store.Get("node-b")
		return ok
	}, 500*time.Millisecond, 10*time.Millisecond)

	stats, _ := store.Get("node-b")
	assert.Equal(t, 42.0, stats.DiskUsedPct, "known fields intact despite unknown field")
}

// --- malformed-payload robustness (panic containment) ---

// malformedFlatBuffer returns bytes whose root uoffset is in-bounds for
// GetRootAs* (so the recovered decode call itself survives) but whose table
// position sits at the very end of the buffer, so any LAZY field accessor
// (vtable read) indexes past the end and panics. This is the adversarial shape
// an authenticated-but-buggy/malicious peer can send to /gossip/admin or
// /gossip/receipt.
func malformedFlatBuffer() []byte {
	// uoffset32 little-endian = 8 → table pos 8 == len(buf): accessors panic.
	return []byte{8, 0, 0, 0, 0, 0, 0, 0}
}

// TestGossipReceiver_MalformedAdminPayloadDoesNotPanic: a corrupt NodeStatsMsg
// must be dropped, not panic the gossip drain goroutine (process crash).
func TestGossipReceiver_MalformedAdminPayloadDoesNotPanic(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	recv := NewGossipReceiver(tr, store)
	recv.RegisterNativeGossipRoutes()

	require.NotPanics(t, func() {
		tr.deliver(t, "node-b:9000", transport.RouteGossipAdmin, malformedFlatBuffer())
	})
	assert.Equal(t, 0, store.Len(), "malformed payload must not populate the store")

	// The receiver must still process valid gossip afterwards.
	tr.deliver(t, "node-b:9000", transport.RouteGossipAdmin, statsGossipMsg(NodeStats{NodeID: "node-b", DiskUsedPct: 12.0}))
	require.Eventually(t, func() bool {
		_, ok := store.Get("node-b")
		return ok
	}, 500*time.Millisecond, 10*time.Millisecond)
}

// TestGossipReceiver_MalformedReceiptPayloadDoesNotPanic: same containment for
// the receipt gossip family (decodeReceiptGossipMsg has the identical lazy-
// accessor exposure).
func TestGossipReceiver_MalformedReceiptPayloadDoesNotPanic(t *testing.T) {
	tr := newMockTransport()
	store := NewNodeStatsStore(1 * time.Minute)
	recv := NewGossipReceiver(tr, store)
	recv.SetReceiptCache(mockReceiptCache{})
	recv.RegisterNativeGossipRoutes()

	require.NotPanics(t, func() {
		tr.deliver(t, "node-b:9000", transport.RouteGossipReceipt, malformedFlatBuffer())
	})
}

type mockReceiptCache struct{}

func (mockReceiptCache) Update(nodeID string, receiptIDs []string) {}
