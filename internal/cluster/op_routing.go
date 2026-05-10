package cluster

import (
	"github.com/gritive/GrainFS/internal/storage"
)

// RouteTarget is the resolved placement-group destination for a storage op.
// Peer addresses are dial-ready (resolved through the address book at route
// time per Q5).
type RouteTarget struct {
	GroupID         string
	Peers           []string
	SelfIsLeader    bool
	SelfIsVoter     bool
	SelfIsOnlyVoter bool
}

// CanReadLocal reports whether self can answer a read without coordinating
// with the rest of the group. Used by LocalExecution.ResolveRead.
func (t RouteTarget) CanReadLocal() bool {
	return t.SelfIsLeader || t.SelfIsOnlyVoter
}

type objectIndexLookup interface {
	ObjectIndexLatest(bucket, key string) (ObjectIndexEntry, bool)
	ObjectIndexVersion(bucket, key, versionID string) (ObjectIndexEntry, bool)
}

type dataGroupLeaderProbe interface {
	GroupLeaderIsSelf(groupID string) bool
}

type OpRouter struct {
	router      *Router
	groups      ShardGroupSource
	index       objectIndexLookup
	addr        NodeAddressBook
	leaderProbe dataGroupLeaderProbe
	ec          ECConfig
	selfID      string
	selfAliases []string
}

func NewOpRouter(
	router *Router,
	groups ShardGroupSource,
	index objectIndexLookup,
	addr NodeAddressBook,
	leaderProbe dataGroupLeaderProbe,
	ec ECConfig,
	selfID string,
	selfAliases []string,
) *OpRouter {
	return &OpRouter{
		router:      router,
		groups:      groups,
		index:       index,
		addr:        addr,
		leaderProbe: leaderProbe,
		ec:          ec,
		selfID:      selfID,
		selfAliases: selfAliases,
	}
}

func (r *OpRouter) RouteBucket(bucket string) (RouteTarget, error) {
	if r.router == nil {
		return RouteTarget{}, ErrCoordinatorNoRouter
	}
	dg, err := r.router.RouteKey(bucket, "")
	if err != nil || dg == nil {
		return RouteTarget{}, storage.ErrNoSuchBucket
	}
	return r.routeGroup(dg.ID())
}

func (r *OpRouter) routeGroup(groupID string) (RouteTarget, error) {
	if err := ValidatePlacementGroupID(groupID); err != nil {
		return RouteTarget{}, err
	}
	if r.groups == nil {
		return RouteTarget{}, ErrUnknownGroup
	}
	entry, ok := r.groups.ShardGroup(groupID)
	if !ok || len(entry.PeerIDs) == 0 {
		return RouteTarget{}, ErrUnknownGroup
	}
	t := RouteTarget{GroupID: entry.ID}
	_, t.SelfIsVoter = NewShardGroupPeerSet(entry).MatchLocal(r.selfID, r.selfAliases...)
	t.SelfIsOnlyVoter = t.SelfIsVoter && len(entry.PeerIDs) == 1
	if t.SelfIsVoter && r.leaderProbe != nil && r.leaderProbe.GroupLeaderIsSelf(entry.ID) {
		t.SelfIsLeader = true
	}
	if t.SelfIsLeader {
		return t, nil
	}
	peers := NewShardGroupPeerSet(entry).ForwardOrder(r.selfID, r.selfAliases...)
	if r.addr != nil {
		resolved, err := ResolveNodeAddresses(r.addr, peers)
		if err != nil {
			return RouteTarget{}, err
		}
		peers = resolved
	}
	t.Peers = peers
	return t, nil
}
