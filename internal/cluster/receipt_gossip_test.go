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

	// Each peer received at least one /gossip/receipt message.
	for _, p := range peers {
		sent := tr.SentTo(p)
		require.NotEmpty(t, sent, "peer %s should receive gossip", p)
		assert.Equal(t, transport.RouteGossipReceipt, sent[0].path, "messages must use the receipt gossip route")
	}
}

func TestReceiptGossipSender_UsesLatestPeerProvider(t *testing.T) {
	tr := newMockTransport()
	provider := &fakeReceiptProvider{ids: []string{"rcpt-1"}}
	peers := []string{"peer-c:9000"}
	sender := NewReceiptGossipSenderWithPeerProvider("node-self:9000", func() []string {
		return append([]string(nil), peers...)
	}, tr, provider, 10*time.Millisecond, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	sender.Run(ctx)

	require.NotEmpty(t, tr.SentTo("peer-c:9000"))
}

func TestReceiptGossipSender_SendsToDynamicPeer(t *testing.T) {
	tr := newMockTransport()
	provider := &fakeReceiptProvider{ids: []string{"rcpt-1"}}
	sender := NewReceiptGossipSenderWithPeerProvider("node-self:9000", func() []string {
		return []string{"peer-c:9000"}
	}, tr, provider, 10*time.Millisecond, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	sender.Run(ctx)

	require.NotEmpty(t, tr.SentTo("peer-c:9000"))
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

	msg := clusterpb.GetRootAsReceiptGossipMsg(sent[0].payload, 0)
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
	msg := clusterpb.GetRootAsReceiptGossipMsg(sent[0].payload, 0)
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

	r.RegisterNativeGossipRoutes()

	// Craft a ReceiptGossipMsg from "peer-a" and deliver via the native route.
	payload := encodeReceiptGossipMsg(t, "peer-a:9000", []string{"rcpt-1", "rcpt-2"})
	tr.deliver(t, "peer-a:9000", transport.RouteGossipReceipt, payload)

	node, ok := cache.Lookup("rcpt-1")
	require.True(t, ok)
	require.Equal(t, "peer-a:9000", node)
}

func TestGossipReceiver_DropsReceiptGossipWithMismatchedNodeID(t *testing.T) {
	tr := newMockTransport()
	statsStore := NewNodeStatsStore(time.Hour)
	cache := receipt.NewRoutingCache()

	r := NewGossipReceiver(tr, statsStore)
	r.SetReceiptCache(cache)

	r.RegisterNativeGossipRoutes()

	// NodeId in payload says "peer-a" but connection is from "peer-evil".
	payload := encodeReceiptGossipMsg(t, "peer-a:9000", []string{"rcpt-1"})
	tr.deliver(t, "peer-evil:9000", transport.RouteGossipReceipt, payload)

	_, ok := cache.Lookup("rcpt-1")
	assert.False(t, ok, "spoofed gossip must be dropped")
}

func TestGossipReceiver_RollingWindowReplacesPriorIDs(t *testing.T) {
	tr := newMockTransport()
	statsStore := NewNodeStatsStore(time.Hour)
	cache := receipt.NewRoutingCache()

	r := NewGossipReceiver(tr, statsStore)
	r.SetReceiptCache(cache)

	r.RegisterNativeGossipRoutes()

	// First tick: old IDs
	tr.deliver(t, "peer-a:9000", transport.RouteGossipReceipt, encodeReceiptGossipMsg(t, "peer-a:9000", []string{"old-1"}))
	_, ok := cache.Lookup("old-1")
	require.True(t, ok)

	// Second tick: replacement IDs
	tr.deliver(t, "peer-a:9000", transport.RouteGossipReceipt, encodeReceiptGossipMsg(t, "peer-a:9000", []string{"new-1"}))
	_, okOld := cache.Lookup("old-1")
	_, okNew := cache.Lookup("new-1")
	require.False(t, okOld, "rolling window should evict old IDs on next gossip")
	require.True(t, okNew)
}
