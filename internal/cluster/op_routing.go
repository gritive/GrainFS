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

// OpRouter resolves S3-level operations to placement-group targets via
// bucket, object-index, or EC-placement lookups. Ctx-free; performs no I/O.
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

// RouteObjectRead resolves an object read to its placement-group target via
// the meta-FSM object index. Empty versionID means the latest version.
// Internal buckets (storage.IsInternalBucket) bypass the object index per
// ADR 0004's pinned-bucket invariant.
//
// F1: nil objectIndexLookup is a configuration error, not a fallback.
func (r *OpRouter) RouteObjectRead(bucket, key, versionID string) (RouteTarget, ObjectIndexEntry, error) {
	if storage.IsInternalBucket(bucket) {
		target, err := r.RouteBucket(bucket)
		entry := ObjectIndexEntry{Bucket: bucket, Key: key, VersionID: versionID, PlacementGroupID: target.GroupID}
		return target, entry, err
	}
	if r.index == nil {
		return RouteTarget{}, ObjectIndexEntry{}, ErrObjectIndexRequired
	}
	var (
		entry ObjectIndexEntry
		ok    bool
	)
	if versionID == "" {
		entry, ok = r.index.ObjectIndexLatest(bucket, key)
	} else {
		entry, ok = r.index.ObjectIndexVersion(bucket, key, versionID)
	}
	if !ok {
		return RouteTarget{}, ObjectIndexEntry{}, storage.ErrObjectNotFound
	}
	target, err := r.routeGroup(entry.PlacementGroupID)
	return target, entry, err
}

// RouteObjectWrite picks the placement group for a new object write via
// SelectObjectPlacementGroup. Returns the chosen ShardGroupEntry alongside
// the RouteTarget so callers can commit the object-index entry after the
// storage write succeeds (Q3).
//
// Internal buckets bypass placement selection (ADR 0004); EC profile too
// large for the topology falls back to the bucket-routed group.
func (r *OpRouter) RouteObjectWrite(bucket, key string) (RouteTarget, ShardGroupEntry, error) {
	if r.groups == nil {
		return RouteTarget{}, ShardGroupEntry{}, ErrCoordinatorNoRouter
	}
	if storage.IsInternalBucket(bucket) {
		target, err := r.RouteBucket(bucket)
		return target, ShardGroupEntry{ID: target.GroupID}, err
	}
	group, err := SelectObjectPlacementGroup(bucket, key, r.groups.ShardGroups(), r.ec)
	if err != nil {
		target, routeErr := r.RouteBucket(bucket)
		if routeErr != nil {
			return RouteTarget{}, ShardGroupEntry{}, err
		}
		groupSnapshot, ok := r.groups.ShardGroup(target.GroupID)
		if !ok {
			return RouteTarget{}, ShardGroupEntry{}, err
		}
		return target, groupSnapshot, nil
	}
	target, err := r.routeGroup(group.ID)
	return target, group, err
}
