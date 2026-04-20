package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/transport"
)

// encodeReceiptGossipMsg is a test helper wrapping the production encoder.
// Kept as a separate name so test expectations read explicitly ("test payload").
func encodeReceiptGossipMsg(t *testing.T, nodeID string, ids []string) []byte {
	t.Helper()
	return encodeReceiptGossip(nodeID, ids)
}

// fakeReceiptProvider returns a static list. Tests mutate it to simulate
// rolling window changes between ticks.
type fakeReceiptProvider struct {
	ids []string
}

func (f *fakeReceiptProvider) RecentReceiptIDs(max int) []string {
	if len(f.ids) <= max {
		return append([]string(nil), f.ids...)
	}
	out := append([]string(nil), f.ids[len(f.ids)-max:]...)
	return out
}

func TestReceiptGossipSender_BroadcastsToAllPeers(t *testing.T) {
	tr := newMockTransport()
	provider := &fakeReceiptProvider{ids: []string{"rcpt-1", "rcpt-2", "rcpt-3"}}
	peers := []string{"peer-a:9000", "peer-b:9000"}

	sender := NewReceiptGossipSender("node-self:9000", peers, tr, provider, 10*time.Millisecond, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	sender.Run(ctx)

	// Each peer received at least one StreamReceipt message.
	for _, p := range peers {
		sent := tr.SentTo(p)
		require.NotEmpty(t, sent, "peer %s should receive gossip", p)
		assert.Equal(t, transport.StreamReceipt, sent[0].Type, "messages must use StreamReceipt")
	}
}

func TestReceiptGossipSender_PayloadDecodesToExpectedIDs(t *testing.T) {
	tr := newMockTransport()
	provider := &fakeReceiptProvider{ids: []string{"alpha", "beta", "gamma"}}
	sender := NewReceiptGossipSender("node-x", []string{"peer:1"}, tr, provider, 10*time.Millisecond, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	sender.Run(ctx)

	sent := tr.SentTo("peer:1")
	require.NotEmpty(t, sent)

	msg := clusterpb.GetRootAsReceiptGossipMsg(sent[0].Payload, 0)
	assert.Equal(t, "node-x", string(msg.NodeId()))
	n := msg.ReceiptIdsLength()
	got := make([]string, n)
	for i := 0; i < n; i++ {
		got[i] = string(msg.ReceiptIds(i))
	}
	assert.Equal(t, []string{"alpha", "beta", "gamma"}, got)
}

func TestReceiptGossipSender_RespectsMaxIDs(t *testing.T) {
	tr := newMockTransport()
	// 100 IDs; max window is 50.
	ids := make([]string, 100)
	for i := range ids {
		ids[i] = string(rune('a' + (i % 26)))
	}
	provider := &fakeReceiptProvider{ids: ids}
	sender := NewReceiptGossipSender("node", []string{"peer:1"}, tr, provider, 10*time.Millisecond, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	sender.Run(ctx)

	sent := tr.SentTo("peer:1")
	require.NotEmpty(t, sent)
	msg := clusterpb.GetRootAsReceiptGossipMsg(sent[0].Payload, 0)
	assert.Equal(t, 50, msg.ReceiptIdsLength(), "sender must cap at maxIDs")
}

func TestReceiptGossipSender_SkipsWhenProviderEmpty(t *testing.T) {
	tr := newMockTransport()
	provider := &fakeReceiptProvider{ids: nil}
	sender := NewReceiptGossipSender("node", []string{"peer:1"}, tr, provider, 10*time.Millisecond, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	sender.Run(ctx)

	assert.Empty(t, tr.SentTo("peer:1"), "no broadcast when provider has zero receipts")
}

// --- Receiver tests (on the extended GossipReceiver) ---

func TestGossipReceiver_DispatchesReceiptGossipToCache(t *testing.T) {
	tr := newMockTransport()
	statsStore := NewNodeStatsStore(time.Hour)
	cache := receipt.NewRoutingCache()

	r := NewGossipReceiver(tr, statsStore)
	r.SetReceiptCache(cache)

	// Craft a ReceiptGossipMsg from "peer-a" and inject via mock.
	payload := encodeReceiptGossipMsg(t, "peer-a:9000", []string{"rcpt-1", "rcpt-2"})
	tr.recv <- &transport.ReceivedMessage{
		From:    "peer-a:9000",
		Message: &transport.Message{Type: transport.StreamReceipt, Payload: payload},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go r.Run(ctx)

	// Poll until cache has the IDs.
	require.Eventually(t, func() bool {
		node, ok := cache.Lookup("rcpt-1")
		return ok && node == "peer-a:9000"
	}, 500*time.Millisecond, 5*time.Millisecond)

	cancel()
}

func TestGossipReceiver_DropsReceiptGossipWithMismatchedNodeID(t *testing.T) {
	tr := newMockTransport()
	statsStore := NewNodeStatsStore(time.Hour)
	cache := receipt.NewRoutingCache()

	r := NewGossipReceiver(tr, statsStore)
	r.SetReceiptCache(cache)

	// NodeId in payload says "peer-a" but connection is from "peer-evil".
	payload := encodeReceiptGossipMsg(t, "peer-a:9000", []string{"rcpt-1"})
	tr.recv <- &transport.ReceivedMessage{
		From:    "peer-evil:9000",
		Message: &transport.Message{Type: transport.StreamReceipt, Payload: payload},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go r.Run(ctx)

	// Give the receiver a chance to process. Then assert cache is still empty.
	time.Sleep(50 * time.Millisecond)
	_, ok := cache.Lookup("rcpt-1")
	assert.False(t, ok, "spoofed gossip must be dropped")

	cancel()
}

func TestGossipReceiver_RollingWindowReplacesPriorIDs(t *testing.T) {
	tr := newMockTransport()
	statsStore := NewNodeStatsStore(time.Hour)
	cache := receipt.NewRoutingCache()

	r := NewGossipReceiver(tr, statsStore)
	r.SetReceiptCache(cache)

	ctx, cancel := context.WithCancel(context.Background())
	go r.Run(ctx)
	t.Cleanup(cancel)

	// First tick: old IDs
	tr.recv <- &transport.ReceivedMessage{
		From:    "peer-a:9000",
		Message: &transport.Message{Type: transport.StreamReceipt, Payload: encodeReceiptGossipMsg(t, "peer-a:9000", []string{"old-1"})},
	}
	require.Eventually(t, func() bool {
		_, ok := cache.Lookup("old-1")
		return ok
	}, 500*time.Millisecond, 5*time.Millisecond)

	// Second tick: replacement IDs
	tr.recv <- &transport.ReceivedMessage{
		From:    "peer-a:9000",
		Message: &transport.Message{Type: transport.StreamReceipt, Payload: encodeReceiptGossipMsg(t, "peer-a:9000", []string{"new-1"})},
	}
	require.Eventually(t, func() bool {
		_, okOld := cache.Lookup("old-1")
		_, okNew := cache.Lookup("new-1")
		return !okOld && okNew
	}, 500*time.Millisecond, 5*time.Millisecond, "rolling window should evict old IDs on next gossip")
}
