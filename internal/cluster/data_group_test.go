package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataGroup_Fields(t *testing.T) {
	g := NewDataGroup("group-0", []string{"node-0", "node-1"})
	assert.Equal(t, "group-0", g.ID())
	assert.Equal(t, []string{"node-0", "node-1"}, g.PeerIDs())
}

func TestDataGroupManager_Empty_ReturnsNil(t *testing.T) {
	mgr := NewDataGroupManager()
	assert.Nil(t, mgr.Get("group-0"))
	assert.Empty(t, mgr.All())
}

func TestDataGroupManager_Add_Get(t *testing.T) {
	mgr := NewDataGroupManager()
	g := NewDataGroup("group-0", []string{"node-0"})
	mgr.Add(g)

	found := mgr.Get("group-0")
	require.NotNil(t, found)
	assert.Equal(t, "group-0", found.ID())
}

func TestDataGroupManager_Add_ReplacesExisting(t *testing.T) {
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-0", []string{"node-0"}))
	mgr.Add(NewDataGroup("group-0", []string{"node-0", "node-1"}))

	all := mgr.All()
	require.Len(t, all, 1)
	assert.Equal(t, []string{"node-0", "node-1"}, all[0].PeerIDs())
}

func TestDataGroupManager_ConcurrentAdd(t *testing.T) {
	mgr := NewDataGroupManager()
	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.Add(NewDataGroup("group-a", []string{"node-0"}))
	}()
	mgr.Add(NewDataGroup("group-b", []string{"node-1"}))
	<-done
}

// TestDataGroupManager_GroupForBucket verifies the bucket→group lookup used by
// ClusterCoordinator's bucket-scoped routing path. Three cases:
//   - happy path: assigned bucket returns its group
//   - unassigned bucket returns (nil, false) (no default set in this test)
//   - nil router returns (nil, false) (defensive — coordinator may be wired
//     before router exists during startup race)
func TestDataGroupManager_GroupForBucket(t *testing.T) {
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-1", []string{"a", "b", "c"}))

	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")

	dg, ok := mgr.GroupForBucket("photos", router)
	require.True(t, ok)
	require.Equal(t, "group-1", dg.ID())

	_, ok = mgr.GroupForBucket("not-assigned", router)
	require.False(t, ok)

	_, ok = mgr.GroupForBucket("photos", nil)
	require.False(t, ok)
}

func TestDataGroupManager_LeaderIDs(t *testing.T) {
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-placeholder", []string{"node-0"}))
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"node-0", "node-1"}, &GroupBackend{
		DistributedBackend: &DistributedBackend{node: &dataGroupLeaderNode{leaderID: "node-1"}},
	}))

	require.Equal(t, map[string]string{"group-1": "node-1"}, mgr.LeaderIDs())
}

func TestDataGroupManager_RaftHealthSnapshot_UnwiredGroupVisible(t *testing.T) {
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-placeholder", []string{"node-0", "node-1"}))

	got := mgr.RaftHealthSnapshot()
	require.Len(t, got, 1)
	require.Equal(t, "group-placeholder", got[0].GroupID)
	require.Equal(t, []string{"node-0", "node-1"}, got[0].PeerIDs)
	require.Contains(t, got[0].Issues, "unwired")
}

func TestDataGroupManager_RaftHealthSnapshot_HealthyGroup(t *testing.T) {
	mgr := NewDataGroupManager()
	node := &dataGroupHealthNode{
		id:           "node-0",
		state:        raft.Follower,
		term:         8,
		leaderID:     "node-1",
		commitIndex:  1204,
		lastLogIndex: 1204,
		match:        map[string]uint64{"node-0": 1204, "node-1": 1204},
	}
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"node-0", "node-1"}, &GroupBackend{
		DistributedBackend: &DistributedBackend{node: node},
	}))

	got := mgr.RaftHealthSnapshot()
	require.Len(t, got, 1)
	require.Equal(t, "group-1", got[0].GroupID)
	require.Equal(t, "Follower", got[0].LocalState)
	require.Equal(t, "node-1", got[0].LeaderID)
	require.Equal(t, uint64(8), got[0].Term)
	require.Equal(t, uint64(1204), got[0].CommitIndex)
	require.Equal(t, uint64(1204), got[0].LastLogIndex)
	require.Equal(t, uint64(0), got[0].MaxPeerLag)
	require.Empty(t, got[0].Issues)
}

func TestDataGroupManager_RaftHealthSnapshot_LeaderlessAndPeerLag(t *testing.T) {
	mgr := NewDataGroupManager()
	node := &dataGroupHealthNode{
		id:           "node-0",
		state:        raft.Follower,
		term:         9,
		commitIndex:  50,
		lastLogIndex: 51,
		match:        map[string]uint64{"node-0": 50, "node-1": 44},
	}
	mgr.Add(NewDataGroupWithBackend("group-2", []string{"node-0", "node-1"}, &GroupBackend{
		DistributedBackend: &DistributedBackend{node: node},
	}))

	got := mgr.RaftHealthSnapshot()
	require.Len(t, got, 1)
	require.Equal(t, "group-2", got[0].GroupID)
	require.Contains(t, got[0].Issues, "leaderless")
	require.Contains(t, got[0].Issues, "peer_lag")
	require.Equal(t, uint64(6), got[0].MaxPeerLag)
	require.Equal(t, map[string]uint64{"node-0": 50, "node-1": 44}, got[0].PeerMatchIndex)
}

// TestRaftHealthSnapshot_RaftVoters_ReadsRealConfigNotMirror verifies that
// RaftHealthSnapshot populates RaftVoters from the node's REAL raft
// Configuration (Suffrage==Voter), not from the constructed peerIDs mirror.
func TestRaftHealthSnapshot_RaftVoters_ReadsRealConfigNotMirror(t *testing.T) {
	mgr := NewDataGroupManager()
	node := &dataGroupHealthNode{
		id:          "node-0",
		state:       raft.Leader,
		term:        3,
		leaderID:    "node-0",
		commitIndex: 100,
		config: raft.Configuration{
			Servers: []raft.Server{
				{ID: "10.0.0.0:9000", Suffrage: raft.Voter},
				{ID: "10.0.0.1:9001", Suffrage: raft.Voter},
				{ID: "10.0.0.9:9009", Suffrage: raft.Voter}, // extra voter NOT in the mirror
			},
		},
	}
	// Mirror peerIDs intentionally differs from the real config voter set.
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"node-0", "node-1"}, &GroupBackend{
		DistributedBackend: &DistributedBackend{node: node},
	}))

	got := mgr.RaftHealthSnapshot()
	require.Len(t, got, 1)
	require.Equal(t, []string{"node-0", "node-1"}, got[0].PeerIDs, "PeerIDs mirror untouched")
	require.Equal(t, []string{"10.0.0.0:9000", "10.0.0.1:9001", "10.0.0.9:9009"}, got[0].RaftVoters,
		"RaftVoters must reflect the real raft config voter set, not the peerIDs mirror")
}

type dataGroupLeaderNode struct {
	RaftNode
	leaderID string
}

func (n *dataGroupLeaderNode) LeaderID() string { return n.leaderID }

type dataGroupHealthNode struct {
	RaftNode
	id           string
	state        raft.NodeState
	term         uint64
	leaderID     string
	commitIndex  uint64
	lastLogIndex uint64
	match        map[string]uint64
	config       raft.Configuration
}

func (n *dataGroupHealthNode) ID() string                        { return n.id }
func (n *dataGroupHealthNode) Configuration() raft.Configuration { return n.config }
func (n *dataGroupHealthNode) State() raft.NodeState             { return n.state }
func (n *dataGroupHealthNode) Term() uint64                      { return n.term }
func (n *dataGroupHealthNode) LeaderID() string                  { return n.leaderID }
func (n *dataGroupHealthNode) CommittedIndex() uint64            { return n.commitIndex }
func (n *dataGroupHealthNode) LastLogIndex() uint64              { return n.lastLogIndex }
func (n *dataGroupHealthNode) PeerMatchIndex(peer string) (uint64, bool) {
	v, ok := n.match[peer]
	return v, ok
}

// TestDataGroupManager_SetMetaBucketStore_WiresAllOwnedBackends is the
// regression guard for the group-0 demotion wiring gap: the meta-bucket seam
// (bucket versioning/policy) must reach EVERY owned data-group backend, not just
// group-0. Without it, a data-plane read (e.g. PutObject's previous-object
// HeadObject) routed to a non-group-0 owned group hits a nil MetaBucketStore and
// fails 500 "MetaBucketStore not wired".
func TestDataGroupManager_SetMetaBucketStore_WiresAllOwnedBackends(t *testing.T) {
	mgr := NewDataGroupManager()
	gb0 := &GroupBackend{DistributedBackend: &DistributedBackend{}}
	mgr.Add(NewDataGroupWithBackend("group-0", nil, gb0))
	require.Nil(t, gb0.MetaBucketStore(), "precondition: backend starts unwired")

	mbs := newDirectFSMMetaBucketStore(nil)
	mgr.SetMetaBucketStore(mbs)
	require.NotNil(t, gb0.MetaBucketStore(), "an already-registered owned group backend must be wired")

	// A group added AFTER SetMetaBucketStore (dynamic AddGroup / evacuation /
	// placeholder→real swap) must also receive the seam.
	gb1 := &GroupBackend{DistributedBackend: &DistributedBackend{}}
	mgr.Add(NewDataGroupWithBackend("group-1", nil, gb1))
	require.NotNil(t, gb1.MetaBucketStore(), "a group added after SetMetaBucketStore must be wired")

	// A placeholder group (nil backend) must not panic.
	require.NotPanics(t, func() {
		mgr.Add(NewDataGroupWithBackend("group-2", nil, nil))
	})
}
