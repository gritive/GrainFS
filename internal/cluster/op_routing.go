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

type dataGroupLeaderProbe interface {
	GroupLeaderIsSelf(groupID string) bool
}

// OpRouter resolves S3-level operations to placement-group targets via
// bucket, object-index, or EC-placement lookups. Ctx-free; performs no I/O.
type OpRouter struct {
	router      *Router
	groups      ShardGroupSource
	addr        NodeAddressBook
	leaderProbe dataGroupLeaderProbe
	ec          ECConfig
	selfID      string
	selfAliases []string
	// placement owns the topology generation list for object→group metadata
	// placement. At a single generation its currentGroupIDs() is byte-identical
	// to the legacy frozen sorted candidate IDs (nil on bootstrap).
	placement *GenerationPlacement
}

func NewOpRouter(
	router *Router,
	groups ShardGroupSource,
	addr NodeAddressBook,
	leaderProbe dataGroupLeaderProbe,
	ec ECConfig,
	selfID string,
	selfAliases []string,
) *OpRouter {
	var placementGroupIDs []string
	if groups != nil {
		if candidates, err := candidateGroupsFor(groups.ShardGroups(), ec); err == nil {
			ids := make([]string, len(candidates))
			for i, c := range candidates {
				ids[i] = c.ID
			}
			placementGroupIDs = ids
		}
	}
	return &OpRouter{
		router:      router,
		groups:      groups,
		addr:        addr,
		leaderProbe: leaderProbe,
		ec:          ec,
		selfID:      selfID,
		selfAliases: selfAliases,
		placement:   newGenerationPlacement(placementGroupIDs),
	}
}

// applyGenerations replaces the placement with one built from the FSM topology
// generation list when non-empty. An empty list (the default, single-generation
// case) leaves the live-candidate-set placement built at construction untouched
// — byte-identical to legacy routing. Called by the coordinator's rebuild after
// NewOpRouter so the generation source stays out of the constructor signature.
func (r *OpRouter) applyGenerations(gens []placementGeneration) {
	if len(gens) > 0 {
		r.placement = newGenerationPlacementFromList(gens)
	}
}

// currentPlacementGroupIDs returns the latest generation's pinned candidate
// group-ID set — the set objects are currently routed under (S7-7 base for
// topology growth). Boot-frozen until a generation is recorded; the live shard
// groups cannot reconstruct it once new groups have joined.
func (r *OpRouter) currentPlacementGroupIDs() []string {
	return r.placement.currentGroupIDs()
}

// generationCount reports the number of recorded topology generations (S7-6).
// A value > 1 means an operator has added a generation; the coordinator uses
// this to arm the cross-generation LWW read merge on the backends. The default
// single-generation cluster returns 1 (or 0 before the candidate set is frozen).
func (r *OpRouter) generationCount() int {
	return r.placement.generationCount()
}

// RouteObjectReadGenerations resolves an object read to one placement-group
// target per topology generation, newest-first (S7-4 generation probe). At a
// single generation it returns exactly one target equal to RouteObjectRead's,
// so the read path is byte-identical. Internal buckets and the empty-candidate
// bootstrap case mirror RouteObjectRead. Targets whose group cannot be resolved
// are skipped; an all-unresolved result returns the first such error.
func (r *OpRouter) RouteObjectReadGenerations(bucket, key, versionID string) ([]RouteTarget, error) {
	if storage.IsInternalBucket(bucket) {
		target, err := r.RouteBucket(bucket)
		if err != nil {
			return nil, err
		}
		return []RouteTarget{target}, nil
	}
	genGroupIDs := r.placement.readGenerationGroupIDs()
	if len(genGroupIDs) == 0 {
		return nil, ErrObjectIndexRequired
	}
	targets := make([]RouteTarget, 0, len(genGroupIDs))
	var firstErr error
	for _, ids := range genGroupIDs {
		if len(ids) == 0 {
			continue
		}
		groupID := groupIDForObject(bucket, key, ids)
		target, err := r.routeGroup(groupID)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		if firstErr != nil {
			return nil, firstErr
		}
		return nil, ErrObjectIndexRequired
	}
	return targets, nil
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
	peers := NewShardGroupPeerSet(entry)
	_, t.SelfIsVoter = peers.MatchLocal(r.selfID, r.selfAliases...)
	t.SelfIsOnlyVoter = t.SelfIsVoter && peers.AllMatchLocal(r.selfID, r.selfAliases...)
	if t.SelfIsVoter && r.leaderProbe != nil && r.leaderProbe.GroupLeaderIsSelf(entry.ID) {
		t.SelfIsLeader = true
		return t, nil
	}
	peersForward := peers.ForwardOrder(r.selfID, r.selfAliases...)
	if r.addr != nil {
		resolved, err := ResolveNodeAddresses(r.addr, peersForward)
		if err != nil {
			return RouteTarget{}, err
		}
		peersForward = resolved
	}
	t.Peers = peersForward
	return t, nil
}

// RouteObjectRead resolves an object read to its placement-group target via
// deterministic hash placement. Empty versionID means the latest version.
// Internal buckets bypass placement selection per ADR 0004.
// Returns ErrObjectIndexRequired when the frozen candidate list is empty (bootstrap).
func (r *OpRouter) RouteObjectRead(bucket, key, versionID string) (RouteTarget, ObjectIndexEntry, error) {
	if storage.IsInternalBucket(bucket) {
		target, err := r.RouteBucket(bucket)
		entry := ObjectIndexEntry{Bucket: bucket, Key: key, VersionID: versionID, PlacementGroupID: target.GroupID}
		return target, entry, err
	}
	placementIDs := r.placement.currentGroupIDs()
	if len(placementIDs) == 0 {
		return RouteTarget{}, ObjectIndexEntry{}, ErrObjectIndexRequired
	}
	groupID := groupIDForObject(bucket, key, placementIDs)
	target, err := r.routeGroup(groupID)
	entry := ObjectIndexEntry{Bucket: bucket, Key: key, VersionID: versionID, PlacementGroupID: groupID}
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
		if err != nil {
			return RouteTarget{}, ShardGroupEntry{}, err
		}
		group, ok := r.groups.ShardGroup(target.GroupID)
		if !ok {
			return RouteTarget{}, ShardGroupEntry{}, ErrNoGroup
		}
		return target, group, nil
	}
	var (
		group ShardGroupEntry
		err   error
	)
	if placementIDs := r.placement.currentGroupIDs(); len(placementIDs) > 0 {
		groupID := groupIDForObject(bucket, key, placementIDs)
		if g, ok := r.groups.ShardGroup(groupID); ok {
			group = g
		}
	}
	if group.ID == "" {
		group, err = SelectObjectPlacementGroup(bucket, key, r.groups.ShardGroups(), r.ec)
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
	}
	target, err := r.routeGroup(group.ID)
	if err != nil {
		return RouteTarget{}, ShardGroupEntry{}, err
	}
	// When self is the leader, routeGroup skips peer resolution (short-circuit
	// for RouteBucket). For object writes we still need forward candidates so
	// the write can be forwarded to the rest of the group if leadership changes
	// between routing and execution (route-to-execute race).
	if target.SelfIsLeader && len(target.Peers) == 0 {
		peers := NewShardGroupPeerSet(group).ForwardOrder(r.selfID, r.selfAliases...)
		if r.addr != nil {
			if resolved, resolveErr := ResolveNodeAddresses(r.addr, peers); resolveErr == nil {
				peers = resolved
			}
		}
		target.Peers = peers
	}
	return target, group, nil
}
