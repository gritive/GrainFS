// index_group_multinode_test.go — Slice 4b-1 Task 6 acceptance: N>1 object-index
// groups boot-wired over a REAL transport, exercising the production wiring this
// slice added (IndexGroupManager.InstantiateAndStart with GroupMux → per-group
// raft.GroupRaftMux.ForGroup/Register), then a PUT/DELETE round-trip through the
// ObjectIndexShardSet façade with read-back.
//
// WHY a real groupRaftMux (not the in-proc igCluster shortcut): igCluster wires
// n.SetTransport(c.lookup(peer).Handle…) — raw routing that BYPASSES the mux
// entirely. The GroupMux ForGroup/Register wiring (the load-bearing code this
// slice added, and the exact class of the fixed Transport:nil bug) is only
// exercised when index-group raft RequestVote/AppendEntries actually ride
// raft.GroupRaftMux over a TCP transport — the same bar as the data-group proof
// in raftv2_group_mux_tcp_test.go. Each node drives the production
// manager.InstantiateAndStart(GroupMux: muxes[i]) path, NOT raw newRaftNode.
//
// SCOPE / honest residual: the follower→leader proposal-FORWARD hop uses an
// in-proc dialer onto the production receiver.Handle (the receiver/sender LOGIC is
// proven over the wire-format in index_group_forward_test.go; the CallPooled TCP
// hop is structurally identical to the data/meta forward dialers). The raft
// REPLICATION underneath rides the real mux. The serveruntime post-seed glue
// (WaitForIndexGroupCount + façade rewire in bootIndexGroupsPostSeed) is covered
// by structural identity with the data-group post-seed phase + the N=1
// byte-identical unit (serveruntime.TestBuildObjectIndexShards_N1MetaFSM); this
// harness does not drive Run(). Claims in CHANGELOG/commit are scoped to match.

package cluster

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// igMuxCluster wires `nodes` index-group nodes over real TCP + GroupRaftMux and
// seeds `groups` object-index raft groups with RF=nodes (every node a voter).
type igMuxCluster struct {
	t          *testing.T
	addrs      []string
	transports []*transport.TCPTransport
	mgrs       []*IndexGroupManager
	facades    []*ObjectIndexShardSet
	entries    []IndexGroupEntry
	n          int // index group count
}

// inboundMuxSessions sums InboundMuxSessionCount across all node transports.
// >0 proves index-group raft RPCs actually rode the TCP mux CARRIER (ForGroup →
// GroupRaftSender mux send), not the legacy tr.Call(StreamGroupRaft) fallback —
// the same discriminator raftv2_group_mux_tcp_test.go uses for data groups.
func (cl *igMuxCluster) inboundMuxSessions() int {
	total := 0
	for _, tr := range cl.transports {
		total += tr.InboundMuxSessionCount()
	}
	return total
}

// igTestGroupID mirrors serveruntime.indexGroupID: zero-padded so lexicographic
// sort == numeric shard order (width = len(str(n-1))). Kept local because the
// harness lives in the cluster package and cannot import serveruntime.
func igTestGroupID(i, n int) string {
	width := len(strconv.Itoa(n - 1))
	return fmt.Sprintf("index-%0*d", width, i)
}

// startIGMuxCluster boots `nodes` nodes, each running `groups` index groups over
// a real groupRaftMux carrier, and waits for every group to elect a leader.
func startIGMuxCluster(t *testing.T, nodes, groups int) *igMuxCluster {
	t.Helper()
	ctx := context.Background()
	require.GreaterOrEqual(t, nodes, 1)
	require.GreaterOrEqual(t, groups, 1)

	// 1. Real TCP transports + per-node GroupRaftMux, mux mode enabled (the
	// carrier the data-group proof asserts).
	transports := make([]*transport.TCPTransport, nodes)
	for i := range transports {
		transports[i] = transport.MustNewTCPTransport("test-index-group-mux-psk")
		require.NoError(t, transports[i].Listen(ctx, "127.0.0.1:0"))
	}
	addrs := make([]string, nodes)
	for i, tr := range transports {
		addrs[i] = tr.LocalAddr()
	}
	muxes := make([]*raft.GroupRaftMux, nodes)
	for i := range muxes {
		muxes[i] = raft.NewGroupRaftMux(transports[i])
		muxes[i].EnableMux(2, 5*time.Millisecond)
	}

	// 2. The N index-group registry entries (RF=nodes — every node a voter).
	entries := make([]IndexGroupEntry, groups)
	for i := range entries {
		entries[i] = IndexGroupEntry{ID: igTestGroupID(i, groups), PeerIDs: append([]string(nil), addrs...)}
	}

	// 3. Per-node manager + forward receiver. The receivers are looked up by addr
	// by the shared in-proc forward dialer (the wire hop the production CallPooled
	// dialer makes — see file header residual note).
	mgrs := make([]*IndexGroupManager, nodes)
	receivers := make([]*IndexGroupProposeForwardReceiver, nodes)
	receiverByAddr := make(map[string]*IndexGroupProposeForwardReceiver, nodes)
	for i := range mgrs {
		mgrs[i] = NewIndexGroupManager()
		receivers[i] = NewIndexGroupProposeForwardReceiver(mgrs[i])
		receiverByAddr[addrs[i]] = receivers[i]
	}

	// 4. Shared forward sender: dialer maps the resolved leader addr → that node's
	// receiver.Handle. The hint is the raft ID, which IS the addr here, so no
	// leader-hint resolver is needed.
	dialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, error) {
		recv := receiverByAddr[peer]
		if recv == nil {
			return nil, fmt.Errorf("no receiver for peer %q", peer)
		}
		reply := recv.Handle(&transport.Message{Type: transport.StreamIndexGroupProposeForward, Payload: payload})
		return reply.Payload, nil
	}
	send := NewIndexGroupProposeForwardSender(dialer).Send

	// 5. Instantiate every group on every node via the PRODUCTION manager path
	// with GroupMux set — this is what wires ForGroup/Register per group.
	kek := newTestKEKStore(t, bytes.Repeat([]byte{0xC4}, encrypt.KEKSize))
	for i := range mgrs {
		cfg := IndexGroupLifecycleConfig{
			NodeID:           addrs[i],
			DataDir:          t.TempDir(),
			KEKStore:         kek,
			GroupMux:         muxes[i],
			ElectionTimeout:  200 * time.Millisecond,
			HeartbeatTimeout: 50 * time.Millisecond,
		}
		require.NoError(t, mgrs[i].InstantiateAndStart(ctx, cfg, entries, send))
	}

	t.Cleanup(func() {
		for _, m := range mgrs {
			m.Close()
		}
		for _, tr := range transports {
			_ = tr.Close()
		}
	})

	facades := make([]*ObjectIndexShardSet, nodes)
	for i := range mgrs {
		f, err := NewObjectIndexShardSet(mgrs[i].Shards())
		require.NoError(t, err)
		facades[i] = f
	}

	cl := &igMuxCluster{t: t, addrs: addrs, transports: transports, mgrs: mgrs, facades: facades, entries: entries, n: groups}
	cl.waitAllGroupsElect()
	return cl
}

// waitAllGroupsElect blocks until every index group has a leader on some node.
func (cl *igMuxCluster) waitAllGroupsElect() {
	cl.t.Helper()
	for _, e := range cl.entries {
		groupID := e.ID
		require.Eventuallyf(cl.t, func() bool { return cl.leaderNodeOf(groupID) >= 0 },
			10*time.Second, 25*time.Millisecond, "index group %s never elected a leader", groupID)
	}
}

// leaderNodeOf returns the node index that is the raft leader of groupID, or -1.
func (cl *igMuxCluster) leaderNodeOf(groupID string) int {
	for i, m := range cl.mgrs {
		if g, ok := m.Lookup(groupID); ok && g.node != nil && g.node.IsLeader() {
			return i
		}
	}
	return -1
}

// nonLeaderNodeOf returns a node index that is NOT the leader of groupID.
func (cl *igMuxCluster) nonLeaderNodeOf(groupID string) int {
	leader := cl.leaderNodeOf(groupID)
	for i := range cl.mgrs {
		if i != leader {
			return i
		}
	}
	return -1
}

// groupFSMHas reports whether node `node`'s local replica of groupID has the
// object — proving the row landed in the hash-selected group's OWN FSM.
func (cl *igMuxCluster) groupFSMHas(node int, groupID, bucket, key string) bool {
	g, ok := cl.mgrs[node].Lookup(groupID)
	if !ok {
		return false
	}
	_, found := g.ObjectIndexLatest(bucket, key)
	return found
}

// keysCoveringEveryShard returns one key per shard: keys[i] hashes to shard i
// under hashObjectPlacementKey % n (the façade's shardFor rule).
func keysCoveringEveryShard(t *testing.T, bucket string, n int) []string {
	t.Helper()
	keys := make([]string, n)
	filled := 0
	for i := 0; filled < n && i < 100000; i++ {
		k := fmt.Sprintf("obj-%d", i)
		shard := int(hashObjectPlacementKey(bucket, k) % uint64(n))
		if keys[shard] == "" {
			keys[shard] = k
			filled++
		}
	}
	require.Equal(t, n, filled, "could not find a key for every shard")
	return keys
}

// TestIndexGroupMultiNode_N4_AllShardsRoundTrip is the core acceptance: 4 nodes,
// 4 index groups over a real groupRaftMux carrier. Every shard gets a key written
// through the façade FROM A NON-LEADER replica (exercises proposal forward), and
// the row reads back through another node's façade AND lives in the hash-selected
// group's own FSM (proves correct-shard routing + real-mux replication + liveness
// across all N groups — not the vacuous one-key/not-meta check).
func TestIndexGroupMultiNode_N4_AllShardsRoundTrip(t *testing.T) {
	const nodes, groups = 4, 4
	cl := startIGMuxCluster(t, nodes, groups)
	ctx := context.Background()
	bucket := "bkt"
	keys := keysCoveringEveryShard(t, bucket, groups)

	for shard, key := range keys {
		groupID := igTestGroupID(shard, groups)
		writer := cl.nonLeaderNodeOf(groupID) // write from a follower → forward path
		require.GreaterOrEqual(t, writer, 0)

		entry := ObjectIndexEntry{Bucket: bucket, Key: key, VersionID: "v1", PlacementGroupID: groupID}
		require.NoErrorf(t, cl.facades[writer].ProposeObjectIndex(ctx, entry, false),
			"PUT key %q (shard %d) via non-leader node %d", key, shard, writer)

		// Read back through a DIFFERENT node's façade (any will do — RF=N, all replicate).
		reader := (writer + 1) % nodes
		require.Eventuallyf(t, func() bool {
			got, ok := cl.facades[reader].ObjectIndexLatest(bucket, key)
			return ok && got.Key == key
		}, 5*time.Second, 25*time.Millisecond, "key %q must replicate to node %d façade", key, reader)

		// Correct-shard liveness: the row lives in shard's OWN index-group FSM on the reader.
		require.Truef(t, cl.groupFSMHas(reader, groupID, bucket, key),
			"key %q (shard %d) must land in group %s's own FSM", key, shard, groupID)
	}

	// DELETE round-trip on one key.
	delKey := keys[0]
	require.NoError(t, cl.facades[0].ProposeDeleteObjectIndex(ctx, bucket, delKey, "v1"))
	require.Eventually(t, func() bool {
		_, ok := cl.facades[1%nodes].ObjectIndexLatest(bucket, delKey)
		return !ok
	}, 5*time.Second, 25*time.Millisecond, "DELETE of %q must replicate", delKey)

	// Carrier discriminator: the GroupRaftSender falls back to tr.Call on mux
	// failure, so election+replication ALONE could pass even if the ForGroup mux
	// wiring were broken. A non-zero inbound mux session count proves index-group
	// raft actually rode the TCP mux carrier (same bar as the data-group proof).
	require.Greater(t, cl.inboundMuxSessions(), 0,
		"index-group raft must ride the TCP mux carrier (inbound mux sessions), not the Call fallback")
}

// NOTE: there is intentionally NO leader-distribution assertion here. Per-group
// leader staggering is NOT guaranteed — ElectionPriorityKey is an inert raft.Config
// field (set by both data and index group lifecycle, never read by the raft node;
// election timeout RNG seeds on cfg.ID only, node.go:254). Distribution is
// probabilistic across independent group elections, so a deterministic ≥2-nodes
// assertion is flaky (it failed under -race with all leaders on node 0). Validating
// real per-node leader distribution is a 4b-2 empirical-measurement concern,
// recorded as a 4b-2 BLOCKER in TODOS — not something to fake as a unit test here.
