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

// GossipTransport is the transport surface the gossip senders/receiver use:
// the native gossip routes (Phase 8 N7-3).
type GossipTransport interface {
	RegisterGossipRoute(path string, h transport.GossipHandler)
	GossipSend(ctx context.Context, addr, path string, payload []byte) error
}

// GossipSender broadcasts local node stats to all peers at a configurable
// interval via the native /gossip/admin route.
type GossipSender struct {
	nodeID          string
	peers           []string
	peerProvider    func() []string
	tr              GossipTransport
	store           *NodeStatsStore
	interval        time.Duration
	logger          zerolog.Logger
	evidenceSource  CapabilityEvidenceSource
	capabilityGate  *CapabilityGate
	evidenceAliases []string
	aliasProvider   func() []string
}

// NewGossipSender creates a sender that broadcasts nodeID's stats at the given interval.
func NewGossipSender(nodeID string, peers []string, tr GossipTransport, store *NodeStatsStore, interval time.Duration) *GossipSender {
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
	// Native /gossip/admin route (Phase 8 N7-3); errors are logged and skipped
	// exactly as the tunnel Connect/Send errors were.
	for _, peer := range s.snapshotPeers() {
		if err := s.tr.GossipSend(ctx, peer, transport.RouteGossipAdmin, payload); err != nil {
			s.logger.Warn().Str("peer", peer).Err(err).Msg("gossip: send failed")
			metrics.BalancerGossipErrorsTotal.Inc()
		} else {
			metrics.BalancerGossipTotal.Inc()
		}
	}
}

// GossipReceiver consumes both gossip families:
//   - /gossip/admin   → NodeStatsMsg → NodeStatsStore
//   - /gossip/receipt → ReceiptGossipMsg → ReceiptRoutingCache (if configured)
//
// Wired via RegisterNativeGossipRoutes (Phase 8 N7-3): each family's callback
// runs on the route's transport-owned drain goroutine.
type GossipReceiver struct {
	tr     GossipTransport
	store  *NodeStatsStore
	logger zerolog.Logger

	// receiptCache is set via SetReceiptCache. Stored as atomic.Pointer so
	// Run can read it without a lock and callers can wire it post-construction.
	receiptCache   atomic.Pointer[ReceiptRoutingCache]
	capabilityGate atomic.Pointer[CapabilityGate]
	addrBook       atomic.Pointer[NodeAddressBook]
}

// NewGossipReceiver creates a receiver backed by the given transport.
func NewGossipReceiver(tr GossipTransport, store *NodeStatsStore) *GossipReceiver {
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

// RegisterNativeGossipRoutes installs the receiver's two per-family handlers
// on the native gossip routes (Phase 8 N7-3). The callbacks run on the
// transport's per-route drain goroutine; empty payloads are skipped.
func (r *GossipReceiver) RegisterNativeGossipRoutes() {
	r.tr.RegisterGossipRoute(transport.RouteGossipAdmin, func(from string, payload []byte) {
		if len(payload) == 0 {
			return
		}
		r.handleNodeStats(from, payload)
	})
	r.tr.RegisterGossipRoute(transport.RouteGossipReceipt, func(from string, payload []byte) {
		if len(payload) == 0 {
			return
		}
		r.handleReceiptGossip(from, payload)
	})
}

func (r *GossipReceiver) handleNodeStats(from string, payload []byte) {
	msg, err := decodeNodeStatsMsg(payload)
	if err != nil {
		r.logger.Warn().Err(err).Msg("gossip: invalid payload")
		return
	}
	if msg.nodeID == "" {
		return
	}
	evidenceNodeID, matches := r.resolveGossipNodeID(msg.nodeID, from)
	// Verify the claimed NodeId matches the actual sender address to prevent spoofing.
	if !matches {
		r.logger.Warn().Str("claimed", msg.nodeID).Str("from", from).Msg("gossip: NodeId mismatch, dropping")
		return
	}
	var joinedAt time.Time
	if msg.joinedAt != 0 {
		joinedAt = time.Unix(msg.joinedAt, 0)
	}
	r.store.Set(NodeStats{
		NodeID:         msg.nodeID,
		DiskUsedPct:    msg.diskUsedPct,
		DiskAvailBytes: msg.diskAvailBytes,
		RequestsPerSec: msg.requestsPerSec,
		JoinedAt:       joinedAt,
	})
	// Gossip capabilities are diagnostic-only — capability gates use direct
	// RPC via CapabilityGate.Allow (Task 1b). ReportEvidence here feeds
	// EvidenceSnapshot (admin /v1/cluster/capabilities) and the bench warmup
	// probe, not production gate enforcement.
	if gate := r.capabilityGate.Load(); gate != nil && len(msg.capabilities) > 0 {
		capabilities := make(map[string]bool, len(msg.capabilities))
		for _, capability := range msg.capabilities {
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

func (r *GossipReceiver) handleReceiptGossip(from string, payload []byte) {
	cachePtr := r.receiptCache.Load()
	if cachePtr == nil {
		return // receipt gossip disabled on this node
	}
	nodeID, ids, err := decodeReceiptGossipMsg(payload)
	if err != nil {
		r.logger.Warn().Err(err).Msg("receipt-gossip: invalid payload")
		return
	}
	if nodeID == "" {
		return
	}
	if !nodeIDMatchesFrom(nodeID, from) {
		r.logger.Warn().Str("claimed", nodeID).Str("from", from).Msg("receipt-gossip: NodeId mismatch, dropping")
		return
	}
	(*cachePtr).Update(nodeID, ids)
}

// nodeStatsGossip is decodeNodeStatsMsg's plain-value result. FlatBuffers
// accessors are LAZY — they index the raw buffer at call time — so every field
// must be read INSIDE the decode's recover scope. Returning the FB table object
// and accessing it in the handler would move the panic surface (corrupt vtable
// from an authenticated peer) outside the recover and crash the gossip drain
// goroutine.
type nodeStatsGossip struct {
	nodeID         string
	diskUsedPct    float64
	diskAvailBytes uint64
	requestsPerSec float64
	joinedAt       int64
	capabilities   []string
}

func decodeNodeStatsMsg(data []byte) (msg nodeStatsGossip, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode node stats: invalid flatbuffer: %v", r)
		}
	}()
	pb := clusterpb.GetRootAsNodeStatsMsg(data, 0)
	msg.nodeID = string(pb.NodeId())
	msg.diskUsedPct = pb.DiskUsedPct()
	msg.diskAvailBytes = pb.DiskAvailBytes()
	msg.requestsPerSec = pb.RequestsPerSec()
	msg.joinedAt = pb.JoinedAt()
	if n := pb.CapabilitiesLength(); n > 0 {
		msg.capabilities = make([]string, 0, n)
		for i := 0; i < n; i++ {
			msg.capabilities = append(msg.capabilities, string(pb.Capabilities(i)))
		}
	}
	return msg, nil
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
