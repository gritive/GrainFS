package cluster

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/transport"
)

// ReceiptRoutingCache is the subset of receipt.RoutingCache the gossip
// receiver needs. Declared here to avoid an import cycle (receipt package
// does not import cluster). Satisfied by *receipt.RoutingCache.
type ReceiptRoutingCache interface {
	Update(nodeID string, ids []string)
}

// GossipSender broadcasts local node stats to all peers at a configurable interval via StreamAdmin.
type GossipSender struct {
	nodeID   string
	peers    []string
	tr       transport.Transport
	store    *NodeStatsStore
	interval time.Duration
	logger   zerolog.Logger
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
		return // local stats not yet populated; skip to avoid broadcasting DiskUsedPct=0
	}

	var joinedAtUnix int64
	if !stats.JoinedAt.IsZero() {
		joinedAtUnix = stats.JoinedAt.Unix()
	}
	b := flatbuffers.NewBuilder(64)
	nodeIDOff := b.CreateString(s.nodeID)
	clusterpb.NodeStatsMsgStart(b)
	clusterpb.NodeStatsMsgAddNodeId(b, nodeIDOff)
	clusterpb.NodeStatsMsgAddDiskUsedPct(b, stats.DiskUsedPct)
	clusterpb.NodeStatsMsgAddDiskAvailBytes(b, stats.DiskAvailBytes)
	clusterpb.NodeStatsMsgAddRequestsPerSec(b, stats.RequestsPerSec)
	clusterpb.NodeStatsMsgAddJoinedAt(b, joinedAtUnix)
	root := clusterpb.NodeStatsMsgEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	payload := make([]byte, len(raw))
	copy(payload, raw)
	msg := &transport.Message{Type: transport.StreamAdmin, Payload: payload}
	for _, peer := range s.peers {
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
	receiptCache atomic.Pointer[ReceiptRoutingCache]
}

// NewGossipReceiver creates a receiver backed by the given transport.
func NewGossipReceiver(tr transport.Transport, store *NodeStatsStore) *GossipReceiver {
	return &GossipReceiver{
		tr:     tr,
		store:  store,
		logger: log.With().Str("component", "gossip").Logger(),
	}
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
	// Verify the claimed NodeId matches the actual sender address to prevent spoofing.
	if !nodeIDMatchesFrom(nodeID, rm.From) {
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
