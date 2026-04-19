package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/metrics"
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
		logger:   slog.Default().With("component", "gossip"),
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
			s.logger.Warn("gossip: send failed", "peer", peer, "err", err)
			metrics.BalancerGossipErrorsTotal.Inc()
		} else {
			metrics.BalancerGossipTotal.Inc()
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
		logger: slog.Default().With("component", "gossip"),
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
			if len(rm.Message.Payload) == 0 {
				continue
			}
			pb, err := decodeNodeStatsMsg(rm.Message.Payload)
			if err != nil {
				r.logger.Warn("gossip: invalid payload", "err", err)
				continue
			}
			nodeID := string(pb.NodeId())
			if nodeID == "" {
				continue
			}
			// Verify the claimed NodeId matches the actual sender address to prevent spoofing.
			if !nodeIDMatchesFrom(nodeID, rm.From) {
				r.logger.Warn("gossip: NodeId mismatch, dropping", "claimed", nodeID, "from", rm.From)
				continue
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
	}
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
// When from is a numeric IP but nodeID is a hostname, the comparison cannot be
// done without DNS — the message is accepted and transport-layer auth (QUIC) is
// the trust boundary in that deployment.
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
	// If from is a numeric IP but nodeID is a hostname, DNS resolution is needed
	// for strict validation. Accept the message; QUIC transport auth handles identity.
	return net.ParseIP(fromHost) != nil && net.ParseIP(nodeHost) == nil
}
