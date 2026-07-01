package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
)

// ErrPlacementNotRedundant is returned by the placement-redundancy gate when an
// object cannot be placed durably: the widest candidate group cannot survive a
// single-node loss (parity 0 / <2 peers) while the cluster has >=2 member nodes,
// so the redundant EC groups have not formed yet. Callers map it to a transient
// ServiceUnavailable rather than routing the object into a non-redundant group.
var ErrPlacementNotRedundant = errors.New("placement: no redundancy-capable group (cluster still forming)")

// redundantPlacementGate reports whether the candidate set may receive a durable
// object placement. candidates must be candidateGroupsFor output (all widest, same
// peer count), so candidates[0] represents the whole set's redundancy. It returns
// ErrPlacementNotRedundant when the widest group is non-redundant (1+0, a single
// peer) AND the cluster has >=2 member nodes — a multi-node cluster mid-formation
// whose wide EC groups have not appeared yet; the caller defers rather than pinning
// the object to a group a single node loss would destroy. A genuine single-node
// cluster (metaNodeCount < 2) keeps the 1+0 candidate, which is the best available.
func redundantPlacementGate(candidates []ShardGroupEntry, metaNodeCount int) error {
	if len(candidates) == 0 || metaNodeCount < 2 {
		return nil
	}
	if !DesiredECConfigForGroup(candidates[0]).Redundant() {
		return ErrPlacementNotRedundant
	}
	return nil
}

// ValidatePlacementGroupID rejects new object-index rows that cannot route to a
// concrete data group.
func ValidatePlacementGroupID(groupID string) error {
	if groupID == "" {
		return fmt.Errorf("empty placement_group_id")
	}
	return nil
}

// groupIDForObject returns the deterministic placement group ID for (bucket, key).
// sortedGroupIDs must be the alphabetically-sorted candidate ID list produced by
// candidateGroupsFor. Callers that build the list independently (e.g. a frozen
// snapshot in OpRouter) must use the same sort order to guarantee equivalence.
func groupIDForObject(bucket, key string, sortedGroupIDs []string) string {
	idx := hashObjectPlacementKey(bucket, key) % uint64(len(sortedGroupIDs))
	return sortedGroupIDs[idx]
}

// SelectObjectPlacementGroup chooses the normal data group for a new object.
func SelectObjectPlacementGroup(bucket, key string, groups []ShardGroupEntry, cfg ECConfig) (ShardGroupEntry, error) {
	candidates, err := candidateGroupsFor(groups, cfg)
	if err != nil {
		return ShardGroupEntry{}, fmt.Errorf("no EC-capable object placement group")
	}
	ids := make([]string, len(candidates))
	for i, c := range candidates {
		ids[i] = c.ID
	}
	selectedID := groupIDForObject(bucket, key, ids)
	for _, c := range candidates {
		if c.ID == selectedID {
			return c, nil
		}
	}
	return candidates[0], nil // unreachable: groupIDForObject always picks from ids
}

// SelectSegmentPlacementGroup picks the placement group for segment segmentIdx
// of (bucket, key) with stable identity supplied by blobID (UUIDv7). Reuses
// the candidate filter from SelectObjectPlacementGroup: EC-capable +
// widest-set + alphabetical sort for determinism.
//
// No versionID parameter: blobID is globally unique and supplies per-version
// uniqueness via the hash; pulling versionID through here would couple
// segment placement to a write-time field readers do not have.
func SelectSegmentPlacementGroup(
	bucket, key string,
	segmentIdx int,
	blobID string,
	groups []ShardGroupEntry,
	cfg ECConfig,
) (ShardGroupEntry, error) {
	candidates, err := candidateGroupsFor(groups, cfg)
	if err != nil {
		return ShardGroupEntry{}, fmt.Errorf("no EC-capable segment placement group")
	}
	idx := hashSegmentPlacementKey(bucket, key, segmentIdx, blobID) % uint64(len(candidates))
	return candidates[idx], nil
}

// candidateGroupsFor returns the EC-capable placement-group candidate set,
// shared by object-level and segment-level placement. Filters out empty IDs
// and EC-incapable groups, keeps only widest-topology groups, and sorts
// alphabetically for deterministic hash indexing.
func candidateGroupsFor(groups []ShardGroupEntry, cfg ECConfig) ([]ShardGroupEntry, error) {
	candidates := make([]ShardGroupEntry, 0, len(groups))
	maxPeerCount := 0
	for _, group := range groups {
		if group.ID == "" {
			continue
		}
		desired := DesiredECConfigForGroup(group)
		if !desired.IsActive(len(group.PeerIDs)) {
			continue
		}
		candidate := ShardGroupEntry{
			ID:      group.ID,
			PeerIDs: cloneStringSlice(group.PeerIDs),
		}
		candidates = append(candidates, candidate)
		if len(candidate.PeerIDs) > maxPeerCount {
			maxPeerCount = len(candidate.PeerIDs)
		}
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no candidate placement group")
	}
	if maxPeerCount > 0 {
		widest := candidates[:0]
		for _, candidate := range candidates {
			if len(candidate.PeerIDs) == maxPeerCount {
				widest = append(widest, candidate)
			}
		}
		candidates = widest
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ID < candidates[j].ID
	})
	return candidates, nil
}

func hashObjectPlacementKey(bucket, key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(bucket))
	_, _ = h.Write([]byte("/"))
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

func hashSegmentPlacementKey(bucket, key string, segmentIdx int, blobID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(bucket))
	_, _ = h.Write([]byte("/"))
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte("#"))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(segmentIdx))
	_, _ = h.Write(buf[:])
	_, _ = h.Write([]byte("#"))
	_, _ = h.Write([]byte(blobID))
	return h.Sum64()
}

type placementGroupContextKey struct{}
type placementGroupEntryContextKey struct{}

func ContextWithPlacementGroup(ctx context.Context, groupID string) context.Context {
	return context.WithValue(ctx, placementGroupContextKey{}, groupID)
}

func ContextWithPlacementGroupEntry(ctx context.Context, group ShardGroupEntry) context.Context {
	cloned := ShardGroupEntry{
		ID:      group.ID,
		PeerIDs: cloneStringSlice(group.PeerIDs),
	}
	ctx = context.WithValue(ctx, placementGroupEntryContextKey{}, cloned)
	return context.WithValue(ctx, placementGroupContextKey{}, cloned.ID)
}

func PlacementGroupFromContext(ctx context.Context) (string, bool) {
	if entry, ok := PlacementGroupEntryFromContext(ctx); ok {
		return entry.ID, true
	}
	groupID, ok := ctx.Value(placementGroupContextKey{}).(string)
	return groupID, ok && groupID != ""
}

func PlacementGroupEntryFromContext(ctx context.Context) (ShardGroupEntry, bool) {
	entry, ok := ctx.Value(placementGroupEntryContextKey{}).(ShardGroupEntry)
	if !ok || entry.ID == "" {
		return ShardGroupEntry{}, false
	}
	entry.PeerIDs = cloneStringSlice(entry.PeerIDs)
	return entry, true
}
