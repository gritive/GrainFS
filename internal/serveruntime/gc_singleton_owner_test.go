package serveruntime

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
)

type gcTestPinger struct {
	errs  map[string]error
	calls []string
}

func (p *gcTestPinger) Ping(_ context.Context, peer string) error {
	p.calls = append(p.calls, peer)
	if p.errs == nil {
		return nil
	}
	return p.errs[peer]
}

type gcSingletonShardGroups map[string]cluster.ShardGroupEntry

func (g gcSingletonShardGroups) ShardGroup(id string) (cluster.ShardGroupEntry, bool) {
	entry, ok := g[id]
	return entry, ok
}

func (g gcSingletonShardGroups) ShardGroups() []cluster.ShardGroupEntry {
	out := make([]cluster.ShardGroupEntry, 0, len(g))
	for _, entry := range g {
		out = append(out, entry)
	}
	return out
}

func TestGCSingletonOwnerForBucket_UsesCanonicalShardGroupOrder(t *testing.T) {
	routerMgr := cluster.NewDataGroupManager()
	router := cluster.NewRouter(routerMgr)
	// Simulate a node-local, self-first group-0 DataGroup entry. The singleton
	// predicate must ignore this local ordering and use the canonical meta entry.
	routerMgr.Add(cluster.NewDataGroup("group-0", []string{"node-b", "node-a"}))
	router.AssignBucket("bkt", "group-0")

	meta := gcSingletonShardGroups{
		"group-0": {ID: "group-0", PeerIDs: []string{"node-a", "node-b"}},
	}

	require.True(t, gcSingletonOwnerForBucket(routerMgr, router, meta, "node-a", "10.0.0.1:7000", "bkt"))
	require.False(t, gcSingletonOwnerForBucket(routerMgr, router, meta, "node-b", "10.0.0.2:7000", "bkt"))
}

func TestGCSingletonOwnerForBucket_AllowsLegacyFirstPeerAddress(t *testing.T) {
	routerMgr := cluster.NewDataGroupManager()
	router := cluster.NewRouter(routerMgr)
	routerMgr.Add(cluster.NewDataGroup("group-0", []string{"node-b", "node-a"}))
	router.AssignBucket("bkt", "group-0")

	meta := gcSingletonShardGroups{
		"group-0": {ID: "group-0", PeerIDs: []string{"10.0.0.1:7000", "node-b"}},
	}

	require.True(t, gcSingletonOwnerForBucket(routerMgr, router, meta, "node-a", "10.0.0.1:7000", "bkt"))
	require.False(t, gcSingletonOwnerForBucket(routerMgr, router, meta, "node-b", "10.0.0.2:7000", "bkt"))
}

func TestGCFreshnessReachable_PingsCanonicalRemotePeers(t *testing.T) {
	mgr := cluster.NewDataGroupManager()
	mgr.Add(cluster.NewDataGroupWithBackend("group-0", []string{"node-a", "node-b"}, &cluster.GroupBackend{DistributedBackend: &cluster.DistributedBackend{}}))
	meta := gcSingletonShardGroups{
		"group-0": {ID: "group-0", PeerIDs: []string{"node-a", "node-b"}},
	}
	pinger := &gcTestPinger{}

	require.True(t, gcFreshnessReachable(context.Background(), mgr, meta, pinger, "node-a", "10.0.0.1:7000"))
	require.Equal(t, []string{"node-b"}, pinger.calls)
}

func TestGCFreshnessReachable_FailsClosedOnUnreachablePeer(t *testing.T) {
	mgr := cluster.NewDataGroupManager()
	mgr.Add(cluster.NewDataGroupWithBackend("group-0", []string{"node-a", "node-b"}, &cluster.GroupBackend{DistributedBackend: &cluster.DistributedBackend{}}))
	meta := gcSingletonShardGroups{
		"group-0": {ID: "group-0", PeerIDs: []string{"node-a", "node-b"}},
	}
	pinger := &gcTestPinger{errs: map[string]error{"node-b": fmt.Errorf("down")}}

	require.False(t, gcFreshnessReachable(context.Background(), mgr, meta, pinger, "node-a", "10.0.0.1:7000"))
	require.False(t, gcFreshnessReachable(context.Background(), mgr, meta, nil, "node-a", "10.0.0.1:7000"))
}

func TestGCFreshnessReachable_SingleNodeDuplicateSelfNeedsNoPinger(t *testing.T) {
	mgr := cluster.NewDataGroupManager()
	mgr.Add(cluster.NewDataGroupWithBackend("group-0", []string{"node-a", "node-a", "node-a"}, &cluster.GroupBackend{DistributedBackend: &cluster.DistributedBackend{}}))
	meta := gcSingletonShardGroups{
		"group-0": {ID: "group-0", PeerIDs: []string{"node-a", "node-a", "node-a"}},
	}

	require.True(t, gcFreshnessReachable(context.Background(), mgr, meta, nil, "node-a", "10.0.0.1:7000"))
}
