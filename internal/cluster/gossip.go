package cluster

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/transport"
)

const gossipInterval = 30 * time.Second

// GossipSender broadcasts local node stats to all peers every 30s via StreamAdmin.
type GossipSender struct {
	nodeID string
	peers  []string
	tr     transport.Transport
	store  *NodeStatsStore
	logger *slog.Logger
}

// NewGossipSender creates a sender that broadcasts nodeID's stats.
func NewGossipSender(nodeID string, peers []string, tr transport.Transport, store *NodeStatsStore) *GossipSender {
	return &GossipSender{
		nodeID: nodeID,
		peers:  peers,
		tr:     tr,
		store:  store,
		logger: slog.Default(),
	}
}

// Run starts the gossip broadcast loop. Blocks until ctx is cancelled.
func (s *GossipSender) Run(ctx context.Context) {
	ticker := time.NewTicker(gossipInterval)
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
			r.store.Set(NodeStats{
				NodeID:         pb.NodeId,
				DiskUsedPct:    pb.DiskUsedPct,
				DiskAvailBytes: pb.DiskAvailBytes,
				RequestsPerSec: pb.RequestsPerSec,
			})
		}
	}
}
