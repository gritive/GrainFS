package cluster

// Gossip × CapabilityGate integration tests. These lived in gossip_test.go
// before the gossip package was extracted (Phase 9); they exercise the REAL
// CapabilityGate through the gossip sender/receiver wiring, so they stay in
// package cluster (the gossip package only sees the EvidenceReporter /
// AddressResolver interfaces). The pure gossip tests moved to internal/gossip.

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
	"github.com/gritive/GrainFS/internal/gossip"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// gateGossipTransport is a local copy of the gossip package's mockTransport
// (test helpers don't cross package boundaries).
type gateGossipTransport struct {
	mu           sync.Mutex
	sent         []gateSentMsg
	gossipRoutes map[string]transport.GossipHandler
}

type gateSentMsg struct {
	to      string
	path    string
	payload []byte
}

func newGateGossipTransport() *gateGossipTransport {
	return &gateGossipTransport{gossipRoutes: make(map[string]transport.GossipHandler)}
}

func (m *gateGossipTransport) RegisterGossipRoute(path string, h transport.GossipHandler) {
	m.mu.Lock()
	m.gossipRoutes[path] = h
	m.mu.Unlock()
}

func (m *gateGossipTransport) GossipSend(_ context.Context, addr, path string, payload []byte) error {
	m.mu.Lock()
	m.sent = append(m.sent, gateSentMsg{to: addr, path: path, payload: payload})
	m.mu.Unlock()
	return nil
}

func (m *gateGossipTransport) sentTo(addr string) []gateSentMsg {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []gateSentMsg
	for _, s := range m.sent {
		if s.to == addr {
			out = append(out, s)
		}
	}
	return out
}

func (m *gateGossipTransport) deliver(t *testing.T, from, path string, payload []byte) {
	t.Helper()
	m.mu.Lock()
	h, ok := m.gossipRoutes[path]
	m.mu.Unlock()
	require.True(t, ok, "no gossip handler registered for %s", path)
	h(from, payload)
}

// gateStatsGossipMsg encodes a NodeStatsMsg payload for the /gossip/admin route.
func gateStatsGossipMsg(ns gossip.NodeStats, capabilities ...string) []byte {
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

type gateStaticEvidence struct{ caps map[string]bool }

func (s gateStaticEvidence) CapabilityEvidence(nodeID string, now time.Time) compat.Evidence {
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

func (f fakeGossipAddressBook) Nodes() []MetaNodeEntry { return f.nodes }

func TestGossipSenderIncludesMultipartListingCapabilityEvidence(t *testing.T) {
	tr := newGateGossipTransport()
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	store.Set(gossip.NodeStats{NodeID: "node-a", DiskUsedPct: 70.0})

	sender := gossip.NewGossipSender("node-a", []string{"node-b:9000"}, tr, store, 30*time.Second).
		WithCapabilityEvidenceSource(NewMetaFSM())
	sender.BroadcastOnce(context.Background())

	msgs := tr.sentTo("node-b:9000")
	require.Len(t, msgs, 1)

	pb := clusterpb.GetRootAsNodeStatsMsg(msgs[0].payload, 0)
	var caps []string
	for i := 0; i < pb.CapabilitiesLength(); i++ {
		caps = append(caps, string(pb.Capabilities(i)))
	}
	assert.Contains(t, caps, compat.CapabilityMultipartListingV1)
}

func TestGossipSenderRefreshesLocalCapabilityEvidence(t *testing.T) {
	tr := newGateGossipTransport()
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	store.Set(gossip.NodeStats{NodeID: "node-a", DiskUsedPct: 70.0})
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)

	sender := gossip.NewGossipSender("node-a", []string{"node-b:9000"}, tr, store, 30*time.Second).
		WithCapabilityEvidenceSource(gateStaticEvidence{caps: map[string]bool{
			compat.CapabilityMultipartListingV1: true,
		}}).
		WithCapabilityGate(gate)
	sender.BroadcastOnce(context.Background())

	_, err := gate.RequirePeerTransportCapability(
		compat.CapabilityMultipartListingV1,
		compat.OperationCreateMultipartUpload,
		[]string{"node-a"},
		time.Now(),
	)
	require.NoError(t, err)
}

func TestGossipSenderRefreshesLocalCapabilityEvidenceAliases(t *testing.T) {
	tr := newGateGossipTransport()
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	store.Set(gossip.NodeStats{NodeID: "node-a", DiskUsedPct: 70.0})
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)

	sender := gossip.NewGossipSender("node-a", []string{"node-b:9000"}, tr, store, 30*time.Second).
		WithCapabilityEvidenceSource(gateStaticEvidence{caps: map[string]bool{
			compat.CapabilityMultipartListingV1: true,
		}}).
		WithCapabilityGate(gate).
		WithCapabilityEvidenceAliases("127.0.0.1:9001")
	sender.BroadcastOnce(context.Background())

	_, err := gate.RequirePeerTransportCapability(
		compat.CapabilityMultipartListingV1,
		compat.OperationCreateMultipartUpload,
		[]string{"127.0.0.1:9001"},
		time.Now(),
	)
	require.NoError(t, err)
}

func TestGossipSenderRefreshesLocalCapabilityEvidenceDynamicAliases(t *testing.T) {
	tr := newGateGossipTransport()
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	store.Set(gossip.NodeStats{NodeID: "node-a", DiskUsedPct: 70.0})
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)
	aliases := []string{}

	sender := gossip.NewGossipSender("node-a", []string{"node-b:9000"}, tr, store, 30*time.Second).
		WithCapabilityEvidenceSource(gateStaticEvidence{caps: map[string]bool{
			compat.CapabilityMultipartListingV1: true,
		}}).
		WithCapabilityGate(gate).
		WithCapabilityEvidenceAliasProvider(func() []string {
			return append([]string(nil), aliases...)
		})

	sender.BroadcastOnce(context.Background())
	_, err := gate.RequirePeerTransportCapability(
		compat.CapabilityMultipartListingV1,
		compat.OperationCreateMultipartUpload,
		[]string{"127.0.0.1:9001"},
		time.Now(),
	)
	require.Error(t, err)

	aliases = []string{"127.0.0.1:9001"}
	sender.BroadcastOnce(context.Background())
	_, err = gate.RequirePeerTransportCapability(
		compat.CapabilityMultipartListingV1,
		compat.OperationCreateMultipartUpload,
		[]string{"127.0.0.1:9001"},
		time.Now(),
	)
	require.NoError(t, err)
}

func TestGossipSenderBroadcastsCapabilityEvidenceWithoutStats(t *testing.T) {
	tr := newGateGossipTransport()
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)

	sender := gossip.NewGossipSender("node-a", []string{"node-b:9000"}, tr, store, 30*time.Second).
		WithCapabilityEvidenceSource(gateStaticEvidence{caps: map[string]bool{
			compat.CapabilityMultipartListingV1: true,
		}}).
		WithCapabilityGate(gate).
		WithCapabilityEvidenceAliases("127.0.0.1:9001")
	sender.BroadcastOnce(context.Background())

	require.Len(t, tr.sentTo("node-b:9000"), 1)
	_, err := gate.RequirePeerTransportCapability(
		compat.CapabilityMultipartListingV1,
		compat.OperationCreateMultipartUpload,
		[]string{"127.0.0.1:9001"},
		time.Now(),
	)
	require.NoError(t, err)
}

func TestGossipReceiverReportsCapabilityEvidenceToGate(t *testing.T) {
	tr := newGateGossipTransport()
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)
	gate.SetMetaRaftSnapshot(7, raft.Configuration{Servers: []raft.Server{
		{ID: "node-a", Suffrage: raft.Voter},
		{ID: "node-b", Suffrage: raft.Voter},
	}})
	now := time.Now()
	gate.ReportEvidence(compat.Evidence{
		NodeID: compat.NodeID("node-a"),
		Capabilities: map[string]bool{
			compat.CapabilityMigrationCutoverV1: true,
		},
		LastSeen: now,
		Ready:    true,
	})

	recv := gossip.NewGossipReceiver(tr, store).WithCapabilityGate(gate)
	recv.RegisterNativeGossipRoutes()

	msg := gateStatsGossipMsg(gossip.NodeStats{NodeID: "node-b", DiskUsedPct: 55.0}, compat.CapabilityMigrationCutoverV1)
	tr.deliver(t, "node-b:9000", transport.RouteGossipAdmin, msg)

	require.Eventually(t, func() bool {
		_, err := gate.RequireMetaRaftCapability(compat.CapabilityMigrationCutoverV1, compat.OperationMigrationCutover, time.Now())
		return err == nil
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func TestGossipReceiverReportsCapabilityEvidenceUnderRaftMemberID(t *testing.T) {
	tr := newGateGossipTransport()
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)
	gate.SetMetaRaftSnapshot(7, raft.Configuration{Servers: []raft.Server{
		{ID: "10.0.0.1:7001", Suffrage: raft.Voter},
		{ID: "10.0.0.2:7001", Suffrage: raft.Voter},
	}})
	now := time.Now()
	gate.ReportEvidence(compat.Evidence{
		NodeID: compat.NodeID("10.0.0.1:7001"),
		Capabilities: map[string]bool{
			compat.CapabilityMigrationCutoverV1: true,
		},
		LastSeen: now,
		Ready:    true,
	})

	recv := gossip.NewGossipReceiver(tr, store).
		WithCapabilityGate(gate).
		WithAddressResolver(NodeAddressBookResolver(fakeGossipAddressBook{nodes: []MetaNodeEntry{
			{ID: "node-b", Address: "10.0.0.2:7001"},
		}}))
	recv.RegisterNativeGossipRoutes()

	msg := gateStatsGossipMsg(gossip.NodeStats{NodeID: "node-b", DiskUsedPct: 55.0}, compat.CapabilityMigrationCutoverV1)
	tr.deliver(t, "10.0.0.2:54321", transport.RouteGossipAdmin, msg)

	require.Eventually(t, func() bool {
		_, err := gate.RequireMetaRaftCapability(compat.CapabilityMigrationCutoverV1, compat.OperationMigrationCutover, time.Now())
		return err == nil
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func TestGossipReceiverPrefersAddressBookOverDirectNodeIDMatch(t *testing.T) {
	tr := newGateGossipTransport()
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)
	gate.SetMetaRaftSnapshot(7, raft.Configuration{Servers: []raft.Server{
		{ID: "node-a:7001", Suffrage: raft.Voter},
		{ID: "node-b:7001", Suffrage: raft.Voter},
	}})
	now := time.Now()
	gate.ReportEvidence(compat.Evidence{
		NodeID: compat.NodeID("node-a:7001"),
		Capabilities: map[string]bool{
			compat.CapabilityMigrationCutoverV1: true,
		},
		LastSeen: now,
		Ready:    true,
	})

	recv := gossip.NewGossipReceiver(tr, store).
		WithCapabilityGate(gate).
		WithAddressResolver(NodeAddressBookResolver(fakeGossipAddressBook{nodes: []MetaNodeEntry{
			{ID: "node-b", Address: "node-b:7001"},
		}}))
	recv.RegisterNativeGossipRoutes()

	msg := gateStatsGossipMsg(gossip.NodeStats{NodeID: "node-b", DiskUsedPct: 55.0}, compat.CapabilityMigrationCutoverV1)
	tr.deliver(t, "node-b:54321", transport.RouteGossipAdmin, msg)

	require.Eventually(t, func() bool {
		_, err := gate.RequireMetaRaftCapability(compat.CapabilityMigrationCutoverV1, compat.OperationMigrationCutover, time.Now())
		return err == nil
	}, 500*time.Millisecond, 10*time.Millisecond)
}
