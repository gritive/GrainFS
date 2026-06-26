package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/rs/zerolog/log"
)

// PlacementExpansionPlan describes a proposed topology-generation growth (S7-7):
// Base is the placement group set objects are currently routed under (the
// OpRouter's boot-frozen / latest-generation set); Expanded is the candidate set
// derived from the live shard groups (which includes any groups formed by node
// joins since boot). Added is Expanded minus Base; Removed is Base minus
// Expanded. Removed is normally empty, but can be non-empty when a newly-joined
// group is WIDER than the Base groups: candidateGroupsFor keeps only the
// widest-peer-count groups, so narrower Base groups drop out of the active
// placement set. Their existing objects stay readable via the prior generation
// (gen-0 probe) but stop receiving new writes — the operator must be shown this,
// not just the additions. NoOp is true when Expanded equals Base.
type PlacementExpansionPlan struct {
	Base     []string
	Expanded []string
	Added    []string
	Removed  []string
	NoOp     bool
}

// PlanPlacementExpansion computes the topology-generation growth that would
// activate the currently-formed-but-unused shard groups for object placement
// (S7-7). It does NOT mutate anything — the caller (serveruntime) proposes the
// generation via MetaRaft.AddTopologyGeneration(plan.Base, plan.Expanded). Base
// comes from the OpRouter (boot-frozen original / latest generation), which is
// the only authoritative source of the set existing objects were placed under;
// the live shard groups alone cannot reconstruct it once new groups have joined.
func (c *ClusterCoordinator) PlanPlacementExpansion() (PlacementExpansionPlan, error) {
	base := append([]string(nil), c.runtimeState().opRouter.currentPlacementGroupIDs()...)
	if len(base) == 0 {
		return PlacementExpansionPlan{}, fmt.Errorf("placement expansion: no current placement groups (bootstrap or no EC-active groups)")
	}
	if c.meta == nil {
		return PlacementExpansionPlan{}, fmt.Errorf("placement expansion: no shard-group source")
	}
	candidates, err := candidateGroupsFor(c.meta.ShardGroups(), c.ecConfig)
	if err != nil {
		return PlacementExpansionPlan{}, fmt.Errorf("placement expansion: candidate groups: %w", err)
	}
	// Durability invariant: never let the operator record a non-redundant generation
	// in a multi-node cluster. ensureGenZero's self-heal assumes the latest generation
	// regresses to non-redundant only at gen-0/boot (all producers append redundant-or-
	// wider sets); an operator expansion to a 1+0 set would break that and could strand
	// the self-heal in a non-advancing re-propose. Refuse it here at the proposer.
	if err := redundantPlacementGate(candidates, metaNodeCount(c.meta)); err != nil {
		return PlacementExpansionPlan{}, fmt.Errorf("placement expansion: %w", err)
	}
	expanded := make([]string, len(candidates))
	for i, cand := range candidates {
		expanded[i] = cand.ID
	}
	if stringSlicesEqual(base, expanded) {
		return PlacementExpansionPlan{Base: base, Expanded: expanded, NoOp: true}, nil
	}
	baseSet := make(map[string]struct{}, len(base))
	for _, id := range base {
		baseSet[id] = struct{}{}
	}
	expandedSet := make(map[string]struct{}, len(expanded))
	for _, id := range expanded {
		expandedSet[id] = struct{}{}
	}
	var added []string
	for _, id := range expanded {
		if _, ok := baseSet[id]; !ok {
			added = append(added, id)
		}
	}
	var removed []string
	for _, id := range base {
		if _, ok := expandedSet[id]; !ok {
			removed = append(removed, id)
		}
	}
	return PlacementExpansionPlan{Base: base, Expanded: expanded, Added: added, Removed: removed}, nil
}

// stringSlicesEqual reports whether two already-sorted string slices are
// element-wise equal. Both base (currentGroupIDs) and expanded (candidateGroupsFor)
// are sorted candidate-ID lists, so this is a sound set-equality test.
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// routeReadOrBucket resolves an object read to its placement-group target via
// deterministic hash placement (S4-4c: index-free). When the frozen placement
// candidate list is empty (bootstrap) or the key resolves to no object, it
// falls back to bucket routing so internal/sole-voter reads still resolve. The
// authoritative metadata now lives in quorum meta; the GroupBackend read (local
// via ResolveRead, or forwarded to the leader) resolves it — there is no object
// index to consult for routing.
func (c *ClusterCoordinator) routeReadOrBucket(bucket, key, versionID string) (RouteTarget, error) {
	target, _, err := c.runtimeState().opRouter.RouteObjectRead(bucket, key, versionID)
	if errors.Is(err, storage.ErrObjectNotFound) || errors.Is(err, ErrObjectIndexRequired) {
		if fallback, _, fallbackErr := c.routeWriteOrBucket(bucket, key); fallbackErr == nil {
			return fallback, nil
		}
	}
	return target, err
}

// placementGenerationSource is the optional MetaFSM capability that exposes the
// topology-generation registry. OpRouter consumes it via the coordinator
// (rebuild) rather than widening ShardGroupSource.
type placementGenerationSource interface {
	PlacementGenerations() []placementGeneration
}

// routeReadGenerations resolves an object read to one target per topology
// generation, newest-first (S7-4 probe order). At a single generation it returns
// the same single target routeReadOrBucket would, preserving the bootstrap
// bucket-route fallback. The returned slice is non-empty on success.
func (c *ClusterCoordinator) routeReadGenerations(bucket, key, versionID string) ([]RouteTarget, error) {
	targets, err := c.runtimeState().opRouter.RouteObjectReadGenerations(bucket, key, versionID)
	if errors.Is(err, storage.ErrObjectNotFound) || errors.Is(err, ErrObjectIndexRequired) {
		if fallback, _, fallbackErr := c.routeWriteOrBucket(bucket, key); fallbackErr == nil {
			return []RouteTarget{fallback}, nil
		}
	}
	return targets, err
}

// probeRead runs do against each topology-generation target newest-first,
// advancing to the next (older) generation ONLY on a definitive
// ErrObjectNotFound. Any other error (group unavailable, transport failure) is
// returned immediately (fail-closed) so a transiently-down older-generation
// group never masquerades as a 404 for an object that exists. At a single
// generation this is exactly one attempt — byte-identical to legacy routing.
func (c *ClusterCoordinator) probeRead(bucket, key, versionID string, do func(target RouteTarget) error) error {
	targets, err := c.routeReadGenerations(bucket, key, versionID)
	if err != nil {
		return err
	}
	var lastErr error
	for _, target := range targets {
		err := do(target)
		if err == nil {
			return nil
		}
		if !errors.Is(err, storage.ErrObjectNotFound) {
			return err
		}
		lastErr = err
	}
	return lastErr
}

// routeWriteOrBucket falls back to RouteBucket for single-node wiring (no EC).
func (c *ClusterCoordinator) routeWriteOrBucket(bucket, key string) (RouteTarget, ShardGroupEntry, error) {
	state := c.runtimeState()
	if state.ecConfig.NumShards() == 0 {
		target, err := state.opRouter.RouteBucket(bucket)
		return target, ShardGroupEntry{ID: target.GroupID}, err
	}
	return state.opRouter.RouteObjectWrite(bucket, key)
}

// routeOwnerWriteOrBucket falls back to RouteBucket for single-node wiring
// but still stamps a deterministic owner so append/multipart RMWs do not
// depend on data-group Raft leadership.
func (c *ClusterCoordinator) routeOwnerWriteOrBucket(bucket, key string) (RouteTarget, ShardGroupEntry, error) {
	state := c.runtimeState()
	if state.ecConfig.NumShards() == 0 {
		target, err := state.opRouter.RouteBucket(bucket)
		if err != nil {
			return RouteTarget{}, ShardGroupEntry{}, err
		}
		group := ShardGroupEntry{ID: target.GroupID}
		if c.meta != nil {
			if g, ok := c.meta.ShardGroup(target.GroupID); ok {
				group = g
			}
		}
		if len(group.PeerIDs) == 0 {
			group.PeerIDs = []string{c.selfID}
		}
		return state.opRouter.ownerWriteTarget(target, group)
	}
	return state.opRouter.RouteObjectOwnerWrite(bucket, key)
}

func (c *ClusterCoordinator) ownerWriteTargetFor(target RouteTarget) (RouteTarget, ShardGroupEntry, error) {
	if c.meta == nil {
		return RouteTarget{}, ShardGroupEntry{}, ErrUnknownGroup
	}
	group, ok := c.meta.ShardGroup(target.GroupID)
	if !ok {
		return RouteTarget{}, ShardGroupEntry{}, ErrUnknownGroup
	}
	return c.runtimeState().opRouter.ownerWriteTarget(target, group)
}

func (c *ClusterCoordinator) routeAppendOrBucket(bucket, key string, expectedOffset int64) (RouteTarget, ShardGroupEntry, error) {
	_ = expectedOffset
	return c.routeOwnerWriteOrBucket(bucket, key)
}

// liveCandidateGroupIDs returns the candidate placement group-ID set derived
// from the LIVE shard-group registry (sorted, as candidateGroupsFor returns it).
// Unlike the OpRouter's boot-frozen placementGroupIDs, this reflects every group
// that has joined since boot — the correct gen-0 ground truth at formation, when
// zero objects have been written.
func (c *ClusterCoordinator) liveCandidateGroupIDs() ([]string, error) {
	if c.meta == nil {
		return nil, fmt.Errorf("gen-0: no shard-group source")
	}
	candidates, err := candidateGroupsFor(c.meta.ShardGroups(), c.ecConfig)
	if err != nil {
		return nil, err
	}
	// Durability gate: defer gen-0 capture while the candidate set is non-redundant
	// (1+0 single-peer groups) in a multi-node cluster. Capturing gen-0 here would
	// pin every object to a group a single node loss destroys. ensureGenZero treats
	// the error as "not ready yet" and retries on the next write. See redundantPlacementGate.
	if err := redundantPlacementGate(candidates, metaNodeCount(c.meta)); err != nil {
		return nil, err
	}
	ids := make([]string, len(candidates))
	for i, cand := range candidates {
		ids[i] = cand.ID
	}
	return ids, nil
}

// ensureGenZero lazily establishes a consistent, REDUNDANT placement generation on
// object writes. In a per-join cluster each node boot-freezes a partial, divergent
// shard-group snapshot, so the same key hash-routes to different groups on different
// nodes (append fragments → InvalidWriteOffset). Recording a generation once into the
// meta-FSM (replicated) makes every node's rebuild() apply the SAME candidate set, so
// routing converges.
//
// Durability self-heal: liveCandidateGroupIDs is gated to redundant-only groups, so
// while the multi-node cluster is still forming (only 1+0 single-peer groups exist)
// gen-0 capture is DEFERRED — no object is pinned to a group a single node loss would
// destroy. If a generation was nonetheless recorded over a non-redundant set (the
// narrow formation-race window where only one node is registered), this advances it:
// once redundant groups form, it appends a new generation over them so subsequent
// writes route redundantly, while objects under the old generation stay readable via
// the newest-first read probe. When the latest generation is already redundant,
// further growth is the operator's job (expand-placement), so this no-ops.
//
// Concurrent writers all propose the same set; the FSM apply dedups against the whole
// registry, so they converge without a new lock. Best-effort: a propose failure is
// logged and retried by the next write rather than failing the user's request.
func (c *ClusterCoordinator) ensureGenZero(ctx context.Context) {
	if c.recordGenZero == nil {
		return
	}
	src, ok := c.meta.(placementGenerationSource)
	if !ok {
		return
	}
	gens := src.PlacementGenerations()
	if len(gens) > 0 && c.placementGenerationRedundant(gens[len(gens)-1]) {
		return // latest generation already redundant — further growth is the operator's
	}
	// Invariant that bounds the self-heal: the only producers of placement
	// generations are this self-heal (appends redundant sets only) and the operator
	// expand-placement (appends WIDER, so still redundant, sets). So once any
	// redundant generation is appended the latest stays redundant — the latest is
	// non-redundant only at gen-0/boot before the first redundant append. Therefore
	// the redundant set computed below is always NEW (not a registry duplicate), so
	// the FSM dedup never silently no-ops it into a non-advancing loop.
	ids, err := c.liveCandidateGroupIDs()
	if err != nil || len(ids) == 0 {
		return // no redundant candidate groups yet — defer until the cluster forms them
	}
	if err := c.recordGenZero(ctx, ids); err != nil {
		log.Warn().Err(err).Strs("groups", ids).
			Msg("redundant placement generation record failed; retrying on next write")
		return
	}
	log.Debug().Strs("groups", ids).Int("prior_generations", len(gens)).
		Msg("recorded redundant placement generation")
}

// placementGenerationRedundant reports whether a placement generation's groups can
// survive a single-node loss. candidateGroupsFor produces a uniform-width set, so the
// first live-resolvable group represents the generation. An unresolvable generation
// (groups gone) is treated as non-redundant so the self-heal re-derives from live.
func (c *ClusterCoordinator) placementGenerationRedundant(gen placementGeneration) bool {
	if c.meta == nil {
		return false
	}
	for _, id := range gen.groupIDs {
		if g, ok := c.meta.ShardGroup(id); ok {
			return DesiredECConfigForGroup(g).Redundant()
		}
	}
	return false
}
