package cluster

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync/atomic"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/transport"
)

// ReceiptRoutingCache is the subset of receipt.RoutingCache the gossip
// receiver needs. Declared here to avoid an import cycle (receipt package
// does not import cluster). Satisfied by *receipt.RoutingCache.
type ReceiptRoutingCache interface {
	Update(nodeID string, ids []string)
}

type CapabilityEvidenceSource interface {
	CapabilityEvidence(nodeID string, now time.Time) compat.Evidence
}

// GossipSender broadcasts local node stats to all peers at a configurable interval via StreamAdmin.
type GossipSender struct {
	nodeID          string
	peers           []string
	peerProvider    func() []string
	tr              transport.Transport
	store           *NodeStatsStore
	interval        time.Duration
	logger          zerolog.Logger
	evidenceSource  CapabilityEvidenceSource
	capabilityGate  *CapabilityGate
	evidenceAliases []string
	aliasProvider   func() []string
}

// NewGossipSender creates a sender that broadcasts nodeID's stats at the given interval.
func NewGossipSender(nodeID string, peers []string, tr transport.Transport, store *NodeStatsStore, interval time.Duration) *GossipSender {
	return &GossipSender{
		nodeID:   nodeID,
		peers:    peers,
		tr:       tr,
		store:    store,
		interval: interval,
		logger:   log.With().Str("component", "gossip").Logger(),
	}
}

func (s *GossipSender) WithCapabilityEvidenceSource(source CapabilityEvidenceSource) *GossipSender {
	s.evidenceSource = source
	return s
}

func (s *GossipSender) WithCapabilityGate(gate *CapabilityGate) *GossipSender {
	s.capabilityGate = gate
	return s
}

func (s *GossipSender) WithCapabilityEvidenceAliases(aliases ...string) *GossipSender {
	s.evidenceAliases = append(s.evidenceAliases[:0], aliases...)
	return s
}

func (s *GossipSender) WithCapabilityEvidenceAliasProvider(provider func() []string) *GossipSender {
	s.aliasProvider = provider
	return s
}

func (s *GossipSender) WithPeerProvider(provider func() []string) *GossipSender {
	s.peerProvider = provider
	return s
}

func (s *GossipSender) snapshotPeers() []string {
	if s.peerProvider != nil {
		return s.peerProvider()
	}
	return append([]string(nil), s.peers...)
}

// Run starts the gossip broadcast loop. Blocks until ctx is cancelled.
func (s *GossipSender) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	s.broadcastOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.broadcastOnce(ctx)
		}
	}
}

// broadcastOnce sends the current node stats to all peers.
func (s *GossipSender) broadcastOnce(ctx context.Context) {
	stats, ok := s.store.Get(s.nodeID)
	if !ok {
		if s.evidenceSource == nil {
			return // local stats not yet populated; skip to avoid broadcasting DiskUsedPct=0
		}
		stats = NodeStats{NodeID: s.nodeID}
	}

	var joinedAtUnix int64
	if !stats.JoinedAt.IsZero() {
		joinedAtUnix = stats.JoinedAt.Unix()
	}
	b := flatbuffers.NewBuilder(64)
	nodeIDOff := b.CreateString(s.nodeID)
	var capabilitiesVec flatbuffers.UOffsetT
	if s.evidenceSource != nil {
		ev := s.evidenceSource.CapabilityEvidence(s.nodeID, time.Now())
		if s.capabilityGate != nil {
			s.capabilityGate.ReportEvidence(ev)
			aliases := append([]string(nil), s.evidenceAliases...)
			if s.aliasProvider != nil {
				aliases = append(aliases, s.aliasProvider()...)
			}
			seenAliases := map[string]struct{}{s.nodeID: {}}
			for _, alias := range aliases {
				if alias == "" || alias == s.nodeID {
					continue
				}
				if _, seen := seenAliases[alias]; seen {
					continue
				}
				seenAliases[alias] = struct{}{}
				aliasEv := ev
				aliasEv.NodeID = compat.NodeID(alias)
				s.capabilityGate.ReportEvidence(aliasEv)
			}
		}
		if ev.Ready && len(ev.Capabilities) > 0 {
			capabilities := make([]string, 0, len(ev.Capabilities))
			for capability, ready := range ev.Capabilities {
				if ready {
					capabilities = append(capabilities, capability)
				}
			}
			sort.Strings(capabilities)
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
	}
	clusterpb.NodeStatsMsgStart(b)
	clusterpb.NodeStatsMsgAddNodeId(b, nodeIDOff)
	clusterpb.NodeStatsMsgAddDiskUsedPct(b, stats.DiskUsedPct)
	clusterpb.NodeStatsMsgAddDiskAvailBytes(b, stats.DiskAvailBytes)
	clusterpb.NodeStatsMsgAddRequestsPerSec(b, stats.RequestsPerSec)
	clusterpb.NodeStatsMsgAddJoinedAt(b, joinedAtUnix)
	if capabilitiesVec != 0 {
		clusterpb.NodeStatsMsgAddCapabilities(b, capabilitiesVec)
	}
	root := clusterpb.NodeStatsMsgEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	payload := make([]byte, len(raw))
	copy(payload, raw)
	msg := &transport.Message{Type: transport.StreamAdmin, Payload: payload}
	for _, peer := range s.snapshotPeers() {
		if err := s.tr.Connect(ctx, peer); err != nil {
			s.logger.Warn().Str("peer", peer).Err(err).Msg("gossip: connect failed")
			metrics.BalancerGossipErrorsTotal.Inc()
			continue
		}
		if err := s.tr.Send(ctx, peer, msg); err != nil {
			s.logger.Warn().Str("peer", peer).Err(err).Msg("gossip: send failed")
			metrics.BalancerGossipErrorsTotal.Inc()
		} else {
			metrics.BalancerGossipTotal.Inc()
		}
	}
}

// GossipReceiver listens on the transport's Receive channel and dispatches
// by StreamType:
//   - StreamAdmin   → NodeStatsMsg → NodeStatsStore
//   - StreamReceipt → ReceiptGossipMsg → ReceiptRoutingCache (if configured)
//
// Multi-stream dispatch lives in one receiver because transport.Receive()
// is a single channel; two competing goroutine consumers would race for
// each message and deliver to at most one.
type GossipReceiver struct {
	tr     transport.Transport
	store  *NodeStatsStore
	logger zerolog.Logger

	// receiptCache is set via SetReceiptCache. Stored as atomic.Pointer so
	// Run can read it without a lock and callers can wire it post-construction.
	receiptCache   atomic.Pointer[ReceiptRoutingCache]
	capabilityGate atomic.Pointer[CapabilityGate]
	addrBook       atomic.Pointer[NodeAddressBook]
}

// NewGossipReceiver creates a receiver backed by the given transport.
func NewGossipReceiver(tr transport.Transport, store *NodeStatsStore) *GossipReceiver {
	return &GossipReceiver{
		tr:     tr,
		store:  store,
		logger: log.With().Str("component", "gossip").Logger(),
	}
}

func (r *GossipReceiver) WithCapabilityGate(gate *CapabilityGate) *GossipReceiver {
	r.SetCapabilityGate(gate)
	return r
}

func (r *GossipReceiver) SetCapabilityGate(gate *CapabilityGate) {
	r.capabilityGate.Store(gate)
}

func (r *GossipReceiver) WithNodeAddressBook(book NodeAddressBook) *GossipReceiver {
	r.SetNodeAddressBook(book)
	return r
}

func (r *GossipReceiver) SetNodeAddressBook(book NodeAddressBook) {
	if book == nil {
		r.addrBook.Store(nil)
		return
	}
	r.addrBook.Store(&book)
}

// SetReceiptCache wires the receipt routing cache. When set, StreamReceipt
// messages update the cache; when nil, they are dropped. Separated from the
// constructor so /ship can land gossip-receiver and receipt-gossip piecemeal.
func (r *GossipReceiver) SetReceiptCache(c ReceiptRoutingCache) {
	r.receiptCache.Store(&c)
}

// Run processes incoming messages until ctx is cancelled.
func (r *GossipReceiver) Run(ctx context.Context) {
	ch := r.tr.Receive()
	for {
		select {
		case <-ctx.Done():
			return
		case rm, ok := <-ch:
			if !ok {
				return
			}
			if len(rm.Message.Payload) == 0 {
				continue
			}
			switch rm.Message.Type {
			case transport.StreamAdmin:
				r.handleNodeStats(rm)
			case transport.StreamReceipt:
				r.handleReceiptGossip(rm)
			default:
				// Other stream types are not gossip concerns.
			}
		}
	}
}

func (r *GossipReceiver) handleNodeStats(rm *transport.ReceivedMessage) {
	pb, err := decodeNodeStatsMsg(rm.Message.Payload)
	if err != nil {
		r.logger.Warn().Err(err).Msg("gossip: invalid payload")
		return
	}
	nodeID := string(pb.NodeId())
	if nodeID == "" {
		return
	}
	evidenceNodeID, matches := r.resolveGossipNodeID(nodeID, rm.From)
	// Verify the claimed NodeId matches the actual sender address to prevent spoofing.
	if !matches {
		r.logger.Warn().Str("claimed", nodeID).Str("from", rm.From).Msg("gossip: NodeId mismatch, dropping")
		return
	}
	var joinedAt time.Time
	if pb.JoinedAt() != 0 {
		joinedAt = time.Unix(pb.JoinedAt(), 0)
	}
	r.store.Set(NodeStats{
		NodeID:         nodeID,
		DiskUsedPct:    pb.DiskUsedPct(),
		DiskAvailBytes: pb.DiskAvailBytes(),
		RequestsPerSec: pb.RequestsPerSec(),
		JoinedAt:       joinedAt,
	})
	if gate := r.capabilityGate.Load(); gate != nil && pb.CapabilitiesLength() > 0 {
		capabilities := make(map[string]bool, pb.CapabilitiesLength())
		for i := 0; i < pb.CapabilitiesLength(); i++ {
			capability := string(pb.Capabilities(i))
			if capability != "" {
				capabilities[capability] = true
			}
		}
		gate.ReportEvidence(compat.Evidence{
			NodeID:       evidenceNodeID,
			Capabilities: capabilities,
			LastSeen:     time.Now(),
			Ready:        true,
		})
	}
}

func (r *GossipReceiver) handleReceiptGossip(rm *transport.ReceivedMessage) {
	cachePtr := r.receiptCache.Load()
	if cachePtr == nil {
		return // receipt gossip disabled on this node
	}
	pb, err := decodeReceiptGossipMsg(rm.Message.Payload)
	if err != nil {
		r.logger.Warn().Err(err).Msg("receipt-gossip: invalid payload")
		return
	}
	nodeID := string(pb.NodeId())
	if nodeID == "" {
		return
	}
	if !nodeIDMatchesFrom(nodeID, rm.From) {
		r.logger.Warn().Str("claimed", nodeID).Str("from", rm.From).Msg("receipt-gossip: NodeId mismatch, dropping")
		return
	}
	n := pb.ReceiptIdsLength()
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = string(pb.ReceiptIds(i))
	}
	(*cachePtr).Update(nodeID, ids)
}

func decodeNodeStatsMsg(data []byte) (msg *clusterpb.NodeStatsMsg, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode node stats: invalid flatbuffer: %v", r)
		}
	}()
	return clusterpb.GetRootAsNodeStatsMsg(data, 0), nil
}

func (r *GossipReceiver) resolveGossipNodeID(nodeID, from string) (compat.NodeID, bool) {
	bookPtr := r.addrBook.Load()
	if bookPtr != nil {
		if addr, ok := ResolveNodeAddress(*bookPtr, nodeID); ok {
			return compat.NodeID(addr), nodeIDMatchesFrom(addr, from)
		}
	}
	if nodeIDMatchesFrom(nodeID, from) {
		return compat.NodeID(nodeID), true
	}
	return compat.NodeID(nodeID), false
}

// nodeIDMatchesFrom returns true if nodeID corresponds to the connection address from.
// Handles both "host:port" and bare "host" node ID formats for nodeID and from.
//
// When from is a numeric IP but nodeID is a hostname, strict comparison requires
// DNS. Strict verification resolves the hostname and checks any resolved IP
// against fromHost; on resolution failure the message is rejected. Previously
// this path accepted unconditionally, which let an authenticated cluster-key
// holder impersonate any peer hostname and poison RoutingCache / NodeStats.
func nodeIDMatchesFrom(nodeID, from string) bool {
	if nodeID == from {
		return true
	}
	fromHost, _, err := net.SplitHostPort(from)
	if err != nil {
		fromHost = from
	}
	nodeHost := nodeID
	if h, _, e := net.SplitHostPort(nodeID); e == nil {
		nodeHost = h
	}
	if nodeHost == fromHost {
		return true
	}
	// Mixed format (from is an IP, nodeID is a hostname): resolve and compare.
	// Bounded DNS lookup keeps the gossip hot-path cheap; LookupHost respects
	// the system resolver's timeout so a broken DNS never stalls the receiver.
	if net.ParseIP(fromHost) != nil && net.ParseIP(nodeHost) == nil {
		addrs, err := net.LookupHost(nodeHost)
		if err != nil {
			return false
		}
		for _, a := range addrs {
			if a == fromHost {
				return true
			}
		}
		return false
	}
	return false
}
