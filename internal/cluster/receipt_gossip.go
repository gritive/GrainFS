package cluster

import (
	"context"
	"fmt"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/transport"
)

// ReceiptProvider is implemented by the receipt store. The sender calls
// RecentReceiptIDs every tick to build the rolling window payload.
//
// Implementations should return the most recent `max` receipt IDs, allocating
// a fresh slice (the sender does not copy).
type ReceiptProvider interface {
	RecentReceiptIDs(max int) []string
}

// ReceiptGossipSender broadcasts the local node's rolling window of receipt
// IDs to every peer at a configurable interval via transport.StreamReceipt.
// Other nodes route `/api/receipts/:id` queries using these windows; IDs
// older than the window fall through to a cluster broadcast fallback.
type ReceiptGossipSender struct {
	nodeID   string
	peers    []string
	tr       transport.Transport
	provider ReceiptProvider
	interval time.Duration
	maxIDs   int
	logger   zerolog.Logger
}

// NewReceiptGossipSender creates a sender. interval is how often to broadcast;
// maxIDs caps the rolling window (Phase 16 target: 50).
func NewReceiptGossipSender(
	nodeID string,
	peers []string,
	tr transport.Transport,
	provider ReceiptProvider,
	interval time.Duration,
	maxIDs int,
) *ReceiptGossipSender {
	return &ReceiptGossipSender{
		nodeID:   nodeID,
		peers:    peers,
		tr:       tr,
		provider: provider,
		interval: interval,
		maxIDs:   maxIDs,
		logger:   log.With().Str("component", "receipt-gossip").Logger(),
	}
}

// Run starts the broadcast loop. Blocks until ctx is cancelled.
func (s *ReceiptGossipSender) Run(ctx context.Context) {
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

func (s *ReceiptGossipSender) broadcastOnce(ctx context.Context) {
	ids := s.provider.RecentReceiptIDs(s.maxIDs)
	if len(ids) == 0 {
		// Nothing to advertise yet; skip this tick to avoid broadcasting
		// an empty window that would evict peers' prior knowledge.
		return
	}

	payload := encodeReceiptGossip(s.nodeID, ids)
	msg := &transport.Message{Type: transport.StreamReceipt, Payload: payload}
	for _, peer := range s.peers {
		if err := s.tr.Send(ctx, peer, msg); err != nil {
			s.logger.Warn().Str("peer", peer).Err(err).Msg("receipt-gossip: send failed")
		}
	}
}

// encodeReceiptGossip serializes a ReceiptGossipMsg. Factored out so tests
// can build identical payloads for injection via mock transport.
func encodeReceiptGossip(nodeID string, ids []string) []byte {
	b := flatbuffers.NewBuilder(256)
	idOffsets := make([]flatbuffers.UOffsetT, len(ids))
	for i, id := range ids {
		idOffsets[i] = b.CreateString(id)
	}
	clusterpb.ReceiptGossipMsgStartReceiptIdsVector(b, len(ids))
	for i := len(idOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(idOffsets[i])
	}
	idsVec := b.EndVector(len(ids))

	nodeIDOff := b.CreateString(nodeID)
	clusterpb.ReceiptGossipMsgStart(b)
	clusterpb.ReceiptGossipMsgAddNodeId(b, nodeIDOff)
	clusterpb.ReceiptGossipMsgAddReceiptIds(b, idsVec)
	root := clusterpb.ReceiptGossipMsgEnd(b)
	b.Finish(root)

	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

func decodeReceiptGossipMsg(data []byte) (msg *clusterpb.ReceiptGossipMsg, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode receipt gossip: invalid flatbuffer: %v", r)
		}
	}()
	return clusterpb.GetRootAsReceiptGossipMsg(data, 0), nil
}
