package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSoloNode creates a durable single-voter raft node under dir and returns
// it plus a closeStore func to call after node.Close() on cleanup.
func newSoloNode(t *testing.T, dir string) (RaftNode, func() error) {
	t.Helper()
	rcfg := raft.DefaultConfig("n1", nil)
	node, closeStore, err := newRaftNode(rcfg, dir)
	require.NoError(t, err)
	return node, closeStore
}

// startSoloIndexGroup builds a KEK-wired MetaFSM, creates an indexGroup
// backed by the given solo node (nil forward = leader-local), starts it,
// and waits for the node to elect itself leader.
func startSoloIndexGroup(t *testing.T, node RaftNode) *indexGroup {
	t.Helper()
	fsm := NewMetaFSM()
	wireTestKEK(t, fsm)
	ig := newIndexGroup(node, fsm, nil)
	require.NoError(t, ig.Start(context.Background()))
	require.Eventually(t, node.IsLeader, 5*time.Second, 20*time.Millisecond,
		"solo node should elect itself leader")
	return ig
}

// TestIndexGroup_SingleNode_PutGetDeleteRoundTrip verifies propose + read-your-write
// + delete on a real solo raft node. Happy path only: every propose waits for
// apply, so committed==applied throughout. The forward path + lagging cases
// (committed > applied) are exercised by Task 3's 3-node loopback test.
func TestIndexGroup_SingleNode_PutGetDeleteRoundTrip(t *testing.T) {
	dir := t.TempDir()
	node, closeStore := newSoloNode(t, dir)
	ig := startSoloIndexGroup(t, node)
	t.Cleanup(func() { ig.Close() })
	t.Cleanup(func() { _ = closeStore() })

	ctx := context.Background()

	// Put v1.
	err := ig.ProposeObjectIndex(ctx, ObjectIndexEntry{
		Bucket:           "b",
		Key:              "k",
		VersionID:        "v1",
		PlacementGroupID: "g0",
		Size:             10,
		ModTime:          100,
	}, false)
	require.NoError(t, err)

	// Read-your-write: v1 must be visible immediately.
	got, ok := ig.ObjectIndexLatest("b", "k")
	require.True(t, ok, "expected entry after put")
	assert.Equal(t, "v1", got.VersionID)
	assert.Equal(t, int64(10), got.Size)

	// Delete v1.
	err = ig.ProposeDeleteObjectIndex(ctx, "b", "k", "v1")
	require.NoError(t, err)

	// After delete, the entry must be gone.
	_, ok = ig.ObjectIndexLatest("b", "k")
	assert.False(t, ok, "entry should be gone after delete")
}

// TestIndexGroup_SingleNode_SnapshotRestoreOnRestart verifies that snapshot()
// persists state and a freshly-opened node + indexGroup restores from it. Happy
// path only: every propose waits for apply, so committed==applied at snapshot
// time. The forward path + lagging cases are exercised by Task 3's 3-node loopback.
func TestIndexGroup_SingleNode_SnapshotRestoreOnRestart(t *testing.T) {
	dir := t.TempDir()

	// ── Phase 1: build state and take snapshot ──────────────────────────────
	node, closeStore := newSoloNode(t, dir)
	ig := startSoloIndexGroup(t, node)

	ctx := context.Background()

	for _, v := range []string{"v1", "v2", "v3"} {
		err := ig.ProposeObjectIndex(ctx, ObjectIndexEntry{
			Bucket:           "b",
			Key:              "k",
			VersionID:        v,
			PlacementGroupID: "g0",
			Size:             1,
			ModTime:          1,
		}, false)
		require.NoError(t, err, "propose %s", v)
	}

	data, idx, err := ig.snapshot()
	require.NoError(t, err)
	assert.Greater(t, idx, uint64(0), "snapshot index must be > 0")
	assert.NotEmpty(t, data, "snapshot data must be non-empty")

	// ── Phase 2: restart ──────────────────────────────────────────────────
	ig.Close()
	require.NoError(t, closeStore())

	node2, closeStore2 := newSoloNode(t, dir)
	t.Cleanup(func() { _ = closeStore2() })

	fsm2 := NewMetaFSM()
	wireTestKEK(t, fsm2)
	ig2 := newIndexGroup(node2, fsm2, nil)
	require.NoError(t, ig2.Start(ctx))
	t.Cleanup(func() { ig2.Close() })

	// After Start(), the snapshot is restored. ObjectIndexVersion for v2 must exist.
	got, ok := ig2.ObjectIndexVersion("b", "k", "v2")
	require.True(t, ok, "v2 should be present after snapshot restore")
	assert.Equal(t, "v2", got.VersionID)
}

// TestIndexGroup_PeriodicSnapshotCompactsAndRestores verifies Task 4.6: when a
// snapshot interval is set, the apply loop fires snapshot() periodically (so the
// raft log is compacted under load), and a FRESH indexGroup on the same dir
// restores rows from that snapshot.
func TestIndexGroup_PeriodicSnapshotCompactsAndRestores(t *testing.T) {
	dir := t.TempDir()

	// ── Phase 1: solo group with a small snapshot interval ──────────────────
	node, closeStore := newSoloNode(t, dir)
	fsm := NewMetaFSM()
	wireTestKEK(t, fsm)
	ig := newIndexGroup(node, fsm, nil)
	ig.setSnapshotInterval(8)
	require.NoError(t, ig.Start(context.Background()))
	require.Eventually(t, node.IsLeader, 5*time.Second, 20*time.Millisecond,
		"solo node should elect itself leader")

	ctx := context.Background()
	// Apply well past the interval so the modulo trigger fires at least once.
	for i := 0; i < 20; i++ {
		require.NoError(t, ig.ProposeObjectIndex(ctx, ObjectIndexEntry{
			Bucket:           "b",
			Key:              fmt.Sprintf("k%02d", i),
			VersionID:        "v1",
			PlacementGroupID: "g0",
			Size:             1,
			ModTime:          int64(i + 1),
		}, false), "propose %d", i)
	}

	// The apply loop snapshots inside itself, so wait for compaction to fire.
	require.Eventually(t, func() bool {
		snap, err := node.LatestSnapshot()
		return err == nil && snap != nil && snap.Index > 0
	}, 5*time.Second, 20*time.Millisecond, "periodic snapshot must fire (LatestSnapshot.Index > 0)")

	// ── Phase 2: fresh group on the same dir restores from the snapshot ─────
	ig.Close()
	require.NoError(t, closeStore())

	node2, closeStore2 := newSoloNode(t, dir)
	t.Cleanup(func() { _ = closeStore2() })
	fsm2 := NewMetaFSM()
	wireTestKEK(t, fsm2)
	ig2 := newIndexGroup(node2, fsm2, nil)
	require.NoError(t, ig2.Start(ctx))
	t.Cleanup(func() { ig2.Close() })

	// Rows from before the snapshot boundary must be present after restore.
	got, ok := ig2.ObjectIndexLatest("b", "k00")
	require.True(t, ok, "k00 should be present after snapshot restore")
	assert.Equal(t, "v1", got.VersionID)
	got, ok = ig2.ObjectIndexLatest("b", "k07")
	require.True(t, ok, "k07 (at the first snapshot boundary) should be present after restore")
	assert.Equal(t, "v1", got.VersionID)
}

// ── Task 3: 3-node in-process loopback ───────────────────────────────────────

const (
	igFastElection = 50 * time.Millisecond
	igSlowElection = 300 * time.Millisecond
	igHeartbeat    = 30 * time.Millisecond
)

// igCluster wires index groups over an in-process raft transport. A node is
// reachable only once registered, so a follower can be wired late to force lag.
type igCluster struct {
	mu     sync.Mutex
	nodes  map[string]RaftNode
	groups map[string]*indexGroup

	// snapCount tracks the number of InstallSnapshot RPCs sent to each peer.
	// Protected by mu.
	snapCount map[string]int
}

func newIGCluster() *igCluster {
	return &igCluster{
		nodes:     map[string]RaftNode{},
		groups:    map[string]*indexGroup{},
		snapCount: map[string]int{},
	}
}

func (c *igCluster) lookup(id string) RaftNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func (c *igCluster) register(id string, n RaftNode) {
	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
}

// wireTransport routes node id's outbound RPCs to peers' inbound Handle* methods.
// Unregistered peers are unreachable (returns an error) — used to force lag.
func (c *igCluster) wireTransport(id string, n RaftNode) {
	n.SetTransport(
		func(peer string, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			d := c.lookup(peer)
			if d == nil {
				return nil, fmt.Errorf("unreachable %s", peer)
			}
			return d.HandleRequestVote(a), nil
		},
		func(peer string, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			d := c.lookup(peer)
			if d == nil {
				return nil, fmt.Errorf("unreachable %s", peer)
			}
			return d.HandleAppendEntries(a), nil
		},
	)
	n.SetInstallSnapshotTransport(func(peer string, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
		d := c.lookup(peer)
		if d == nil {
			return nil, fmt.Errorf("unreachable %s", peer)
		}
		// Count the outbound InstallSnapshot before forwarding.
		c.mu.Lock()
		c.snapCount[peer]++
		c.mu.Unlock()
		return d.HandleInstallSnapshot(a), nil
	})
}

func (c *igCluster) leaderID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, n := range c.nodes {
		if n.IsLeader() {
			return id
		}
	}
	return ""
}

func (c *igCluster) installSnapshotCount(peer string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapCount[peer]
}

// startNode builds a durable node with the given election timeout, wires transport
// (BEFORE Start), builds a KEK-wired MetaFSM + forwarding index group, and
// registers it. KEK is wired on every node so leader's sealed InstallSnapshot
// envelope can be decrypted by any follower.
func (c *igCluster) startNode(t *testing.T, id string, peers []string, election time.Duration, forwardEnabled bool) *indexGroup {
	t.Helper()
	rcfg := raft.DefaultConfig(id, peers)
	rcfg.ElectionTimeout = election
	rcfg.HeartbeatTimeout = igHeartbeat
	node, closeStore, err := newRaftNode(rcfg, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeStore() })
	c.register(id, node)
	c.wireTransport(id, node)

	fsm := NewMetaFSM()
	wireTestKEK(t, fsm)

	var fwd indexGroupForwardFunc
	if forwardEnabled {
		fwd = func(ctx context.Context, data []byte) (uint64, error) {
			lid := c.leaderID()
			lnode := c.lookup(lid)
			if lnode == nil {
				return 0, fmt.Errorf("no leader")
			}
			return lnode.ProposeWait(ctx, data)
		}
	}
	ig := newIndexGroup(node, fsm, fwd)
	require.NoError(t, ig.Start(context.Background()))
	t.Cleanup(ig.Close)
	c.mu.Lock()
	c.groups[id] = ig
	c.mu.Unlock()
	return ig
}

// startNodeWithForward mirrors startNode but installs a caller-supplied forward
// hook (built from the node's own *indexGroup so the hook can read LeaderID()),
// instead of the direct-ProposeWait shortcut. Used by index_group_forward_test
// to drive the production sender+receiver path.
func (c *igCluster) startNodeWithForward(t *testing.T, id string, peers []string, election time.Duration, hookFor func(self *indexGroup) indexGroupForwardFunc) *indexGroup {
	t.Helper()
	rcfg := raft.DefaultConfig(id, peers)
	rcfg.ElectionTimeout = election
	rcfg.HeartbeatTimeout = igHeartbeat
	node, closeStore, err := newRaftNode(rcfg, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeStore() })
	c.register(id, node)
	c.wireTransport(id, node)

	fsm := NewMetaFSM()
	wireTestKEK(t, fsm)

	ig := newIndexGroup(node, fsm, nil)
	ig.forward = hookFor(ig) // installed before Start so the hook is live immediately
	require.NoError(t, ig.Start(context.Background()))
	t.Cleanup(ig.Close)
	c.mu.Lock()
	c.groups[id] = ig
	c.mu.Unlock()
	return ig
}

// start3 brings up n1(fast→leader), n2, n3(slow) and waits for n1 leadership.
func start3(t *testing.T, forwardEnabled bool) *igCluster {
	t.Helper()
	c := newIGCluster()
	peers := func(self string) []string {
		var p []string
		for _, id := range []string{"n1", "n2", "n3"} {
			if id != self {
				p = append(p, id)
			}
		}
		return p
	}
	c.startNode(t, "n1", peers("n1"), igFastElection, forwardEnabled)
	c.startNode(t, "n2", peers("n2"), igSlowElection, forwardEnabled)
	c.startNode(t, "n3", peers("n3"), igSlowElection, forwardEnabled)
	require.Eventually(t, func() bool { return c.leaderID() == "n1" }, 10*time.Second, 50*time.Millisecond,
		"n1 should win election")
	return c
}

// TestIndexGroup_ThreeNode_FollowerForwardReplicates verifies that a follower's
// ProposeObjectIndex / ProposeDeleteObjectIndex fires the forward hook to the
// leader, commits, and replicates to all nodes.
func TestIndexGroup_ThreeNode_FollowerForwardReplicates(t *testing.T) {
	c := start3(t, true)
	n2 := c.groups["n2"] // deterministic follower (n1 won the fast election)
	require.False(t, c.lookup("n2").IsLeader(), "n2 must be a follower")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Phase 1: propose a Put from the follower — forward hook must route to the
	// leader, replicate, and become visible (with the put's value) on all nodes.
	require.NoError(t, n2.ProposeObjectIndex(ctx,
		ObjectIndexEntry{Bucket: "b", Key: "k", VersionID: "v1", PlacementGroupID: "g0", Size: 7, ModTime: 1}, false),
		"follower forward (put) must succeed")
	for id, g := range c.groups {
		g := g
		require.Eventually(t, func() bool {
			got, ok := g.ObjectIndexLatest("b", "k")
			return ok && got.VersionID == "v1"
		}, 5*time.Second, 20*time.Millisecond, "node %s should see the put replicated", id)
	}

	// Phase 2: propose a Delete from the follower — delete-forward must replicate
	// and the entry must become absent on all nodes.
	require.NoError(t, n2.ProposeDeleteObjectIndex(ctx, "b", "k", "v1"),
		"follower forward (delete) must succeed")
	for id, g := range c.groups {
		g := g
		require.Eventually(t, func() bool {
			_, ok := g.ObjectIndexLatest("b", "k")
			return !ok
		}, 5*time.Second, 20*time.Millisecond, "node %s should see the delete replicated", id)
	}
}

// TestIndexGroup_ThreeNode_ForwardDisabled_FollowerProposeFails is the
// RED-on-revert guard: when no forwarder is wired, a follower's local
// ProposeWait returns ErrNotLeader and the whole proposal must fail.
func TestIndexGroup_ThreeNode_ForwardDisabled_FollowerProposeFails(t *testing.T) {
	c := start3(t, false /*forwardEnabled=false*/)
	n2 := c.groups["n2"]
	require.False(t, c.lookup("n2").IsLeader(), "n2 must be a follower")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := n2.ProposeObjectIndex(ctx,
		ObjectIndexEntry{Bucket: "b", Key: "k", VersionID: "v1", PlacementGroupID: "g0", Size: 1, ModTime: 1}, false)
	require.ErrorIs(t, err, raft.ErrNotLeader, "follower propose without forwarding must fail with ErrNotLeader")
}

// TestIndexGroup_ThreeNode_LaggingFollowerInstallSnapshot verifies the
// apply-loop LogEntrySnapshot path over a real RPC. n1 (fast) + n3 (slow) form
// quorum while n2 is held unregistered. After several puts, g1.snapshot() →
// CreateSnapshot → CompactBefore(idx) advances the leader's log FirstIndex past
// the compaction boundary. When n2 (a late joiner) is brought online, the leader
// on first contact finds n2's required prevLogIndex below the compacted
// FirstIndex, so it dispatches InstallSnapshot (not AppendEntries) and n2's apply
// loop must restore state from it.
func TestIndexGroup_ThreeNode_LaggingFollowerInstallSnapshot(t *testing.T) {
	c := newIGCluster()

	// n1 (fast → deterministic leader) + n3 (slow) form quorum; n2 is absent.
	g1 := c.startNode(t, "n1", []string{"n2", "n3"}, igFastElection, true)
	c.startNode(t, "n3", []string{"n1", "n2"}, igSlowElection, true)
	require.Eventually(t, func() bool { return c.leaderID() == "n1" }, 10*time.Second, 50*time.Millisecond,
		"n1 should become leader")

	ctx := context.Background()
	// Propose enough entries so the snapshot covers them all.
	for i, v := range []string{"v1", "v2", "v3", "v4", "v5"} {
		require.NoError(t, g1.ProposeObjectIndex(ctx,
			ObjectIndexEntry{Bucket: "b", Key: "k", VersionID: v, PlacementGroupID: "g0", Size: 1, ModTime: int64(i + 1)}, false),
			"put %s on leader", v)
	}

	// Snapshot the leader: CreateSnapshot → CompactBefore(idx) advances the log's
	// FirstIndex past n2's nextIndex, so the leader must catch n2 up via
	// InstallSnapshot rather than AppendEntries.
	_, _, err := g1.snapshot()
	require.NoError(t, err, "leader snapshot must succeed")

	// Now bring n2 online (fresh store, no prior log — must catch up via InstallSnapshot).
	g2 := c.startNode(t, "n2", []string{"n1", "n3"}, igSlowElection, true)

	// Wait for the leader to send at least one InstallSnapshot to n2.
	require.Eventually(t, func() bool {
		return c.installSnapshotCount("n2") > 0
	}, 10*time.Second, 50*time.Millisecond, "leader must send InstallSnapshot to n2")

	// n2's apply loop must restore the snapshot and reach v5.
	require.Eventually(t, func() bool {
		got, ok := g2.ObjectIndexLatest("b", "k")
		return ok && got.VersionID == "v5"
	}, 10*time.Second, 50*time.Millisecond, "n2 did not catch up to v5 via InstallSnapshot")
}
