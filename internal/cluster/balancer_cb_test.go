package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestTickOnce_CBUpdatesFromStoreGetAll: tickOnce must call syncCB after store.GetAll().
// Verifies that a CB opens when gossip indicates disk full.
func TestTickOnce_CBUpdatesFromStoreGetAll(t *testing.T) {
	store := NewNodeStatsStore(time.Minute)
	node := &mockRaftNode{nodeID: "self", peerIDs: []string{"n1"}, state: 2}
	cfg := testBalancerConfig()
	cfg.CBThreshold = 0.90

	p := NewBalancerProposer("self", store, node, cfg)
	p.startedAt = p.startedAt.Add(-cfg.WarmupTimeout - time.Second) // skip warmup

	// Feed self + n1 (n1 disk at 95% — above threshold)
	store.Set(NodeStats{NodeID: "self", DiskUsedPct: 10})
	store.Set(NodeStats{NodeID: "n1", DiskUsedPct: 95})

	ctx := t.Context()
	p.tickOnce(ctx)

	// n1's CB must be open
	cb := p.getCB("n1")
	assert.NotNil(t, cb, "CB must exist for n1 after tickOnce")
	assert.False(t, cb.allow(), "n1 CB must be open (DiskUsedPct=95 >= threshold=90)")
}

// TestTickOnce_AllDstsCBOpen_NoProposal: when all peers have open CBs, no migration is proposed.
func TestTickOnce_AllDstsCBOpen_NoProposal(t *testing.T) {
	store := NewNodeStatsStore(time.Minute)
	node := &mockRaftNode{nodeID: "self", peerIDs: []string{"n1", "n2"}, state: 2}
	cfg := testBalancerConfig()
	cfg.CBThreshold = 0.90
	cfg.ImbalanceTriggerPct = 5 // easily triggered

	p := NewBalancerProposer("self", store, node, cfg)
	p.startedAt = p.startedAt.Add(-cfg.WarmupTimeout - time.Second)
	p.SetObjectPicker(&mockPicker{bucket: "b", key: "k"})

	// Self has high disk, peers all full
	store.Set(NodeStats{NodeID: "self", DiskUsedPct: 80})
	store.Set(NodeStats{NodeID: "n1", DiskUsedPct: 95})
	store.Set(NodeStats{NodeID: "n2", DiskUsedPct: 97})

	ctx := t.Context()
	p.tickOnce(ctx)

	// No migration proposal should have been made
	assert.Equal(t, 0, node.ProposedLen(), "no proposal when all dst CBs are open")
}

// TestTickOnce_CBOpenSkipsDst: CB-open dst is excluded from migration target.
func TestTickOnce_CBOpenSkipsDst(t *testing.T) {
	store := NewNodeStatsStore(time.Minute)
	node := &mockRaftNode{nodeID: "self", peerIDs: []string{"n1", "n2"}, state: 2}
	cfg := testBalancerConfig()
	cfg.CBThreshold = 0.90
	cfg.ImbalanceTriggerPct = 5

	p := NewBalancerProposer("self", store, node, cfg)
	p.startedAt = p.startedAt.Add(-cfg.WarmupTimeout - time.Second)
	p.SetObjectPicker(&mockPicker{bucket: "b", key: "k"})

	// Self heavy, n1 full (CB open), n2 light (CB closed) — expect migration to n2
	store.Set(NodeStats{NodeID: "self", DiskUsedPct: 80})
	store.Set(NodeStats{NodeID: "n1", DiskUsedPct: 95})
	store.Set(NodeStats{NodeID: "n2", DiskUsedPct: 10})

	ctx := t.Context()
	p.tickOnce(ctx)

	assert.Equal(t, 1, node.ProposedLen(), "must propose one migration to n2")
}

// mockPicker always returns the same object.
type mockPicker struct {
	bucket, key string
}

func (m *mockPicker) PickObjectOnSrcNode(_ string, skip map[string]bool) (string, string, string, bool) {
	id := m.bucket + "/" + m.key + "/"
	if skip[id] {
		return "", "", "", false
	}
	return m.bucket, m.key, "", true
}
