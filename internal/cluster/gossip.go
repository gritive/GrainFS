package cluster

import (
	"context"
	"log/slog"
	"net"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/transport"
)

// GossipSender broadcasts local node stats to all peers at a configurable interval via StreamAdmin.
type GossipSender struct {
	nodeID   string
	peers    []string
	tr       transport.Transport
	store    *NodeStatsStore
	interval time.Duration
	logger   *slog.Logger
}

// NewGossipSender creates a sender that broadcasts nodeID's stats at the given interval.
func NewGossipSender(nodeID string, peers []string, tr transport.Transport, store *NodeStatsStore, interval time.Duration) *GossipSender {
	return &GossipSender{
		nodeID:   nodeID,
		peers:    peers,
		tr:       tr,
		store:    store,
		interval: interval,
		logger:   slog.Default(),
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
	stats, _ := s.store.Get(s.nodeID)

	pb := &clusterpb.NodeStatsMsg{
		NodeId:         s.nodeID,
		DiskUsedPct:    stats.DiskUsedPct,
		DiskAvailBytes: stats.DiskAvailBytes,
		RequestsPerSec: stats.RequestsPerSec,
	}
	payload, err := proto.Marshal(pb)
	if err != nil {
		s.logger.Error("gossip: marshal failed", "err", err)
		return
	}
	msg := &transport.Message{Type: transport.StreamAdmin, Payload: payload}
	for _, peer := range s.peers {
		if err := s.tr.Send(ctx, peer, msg); err != nil {
			s.logger.Warn("gossip: send failed", "peer", peer, "err", err)
		}
	}
}

// GossipReceiver listens on the transport's Receive channel and updates NodeStatsStore
// for every incoming StreamAdmin message that carries a valid NodeStatsMsg.
type GossipReceiver struct {
	tr     transport.Transport
	store  *NodeStatsStore
	logger *slog.Logger
}

// NewGossipReceiver creates a receiver backed by the given transport.
func NewGossipReceiver(tr transport.Transport, store *NodeStatsStore) *GossipReceiver {
	return &GossipReceiver{
		tr:     tr,
		store:  store,
		logger: slog.Default(),
	}
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
			if rm.Message.Type != transport.StreamAdmin {
				continue
			}
			var pb clusterpb.NodeStatsMsg
			if err := proto.Unmarshal(rm.Message.Payload, &pb); err != nil {
				r.logger.Warn("gossip: unmarshal failed", "from", rm.From, "err", err)
				continue
			}
			if pb.NodeId == "" {
				continue
			}
			// Verify the claimed NodeId matches the actual sender address to prevent spoofing.
			if !nodeIDMatchesFrom(pb.NodeId, rm.From) {
				r.logger.Warn("gossip: NodeId mismatch, dropping", "claimed", pb.NodeId, "from", rm.From)
				continue
			}
			r.store.Set(NodeStats{
				NodeID:         pb.NodeId,
				DiskUsedPct:    pb.DiskUsedPct,
				DiskAvailBytes: pb.DiskAvailBytes,
				RequestsPerSec: pb.RequestsPerSec,
			})
		}
	}
}

// nodeIDMatchesFrom returns true if nodeID corresponds to the connection address from.
// Handles both "host:port" and bare "host" node ID formats.
func nodeIDMatchesFrom(nodeID, from string) bool {
	if nodeID == from {
		return true
	}
	host, _, err := net.SplitHostPort(from)
	if err != nil {
		return false
	}
	return nodeID == host
}
