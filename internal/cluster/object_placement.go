package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
)

// ValidatePlacementGroupID rejects new object-index rows that cannot route to a
// concrete data group.
func ValidatePlacementGroupID(groupID string) error {
	if groupID == "" {
		return fmt.Errorf("empty placement_group_id")
	}
	return nil
}

// SelectObjectPlacementGroup chooses the normal data group for a new object.
// group-0 stays reserved for legacy/system paths when normal data groups are
// present, but remains the fallback for bootstrap clusters that only have the
// legacy group.
func SelectObjectPlacementGroup(bucket, key string, groups []ShardGroupEntry, cfg ECConfig) (ShardGroupEntry, error) {
	candidates, err := candidateGroupsFor(groups, cfg)
	if err != nil {
		return ShardGroupEntry{}, fmt.Errorf("no EC-capable object placement group")
	}
	idx := hashObjectPlacementKey(bucket, key) % uint64(len(candidates))
	return candidates[idx], nil
}

// SelectSegmentPlacementGroup picks the placement group for segment segmentIdx
// of (bucket, key) with stable identity supplied by blobID (UUIDv7). Reuses
// the candidate filter from SelectObjectPlacementGroup: EC-capable + group-0
// excluded when non-legacy candidates exist + widest-set + alphabetical sort
// for determinism.
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
// and EC-incapable groups, excludes group-0 when any non-legacy candidate is
// available (falls back to group-0 otherwise), keeps only widest-topology
// groups, and sorts alphabetically for deterministic hash indexing.
func candidateGroupsFor(groups []ShardGroupEntry, cfg ECConfig) ([]ShardGroupEntry, error) {
	candidates := make([]ShardGroupEntry, 0, len(groups))
	legacyCandidates := make([]ShardGroupEntry, 0, 1)
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
		if group.ID == "group-0" {
			legacyCandidates = append(legacyCandidates, candidate)
			continue
		}
		candidates = append(candidates, candidate)
		if len(candidate.PeerIDs) > maxPeerCount {
			maxPeerCount = len(candidate.PeerIDs)
		}
	}
	if len(candidates) == 0 && len(legacyCandidates) > 0 {
		candidates = legacyCandidates
		maxPeerCount = len(candidates[0].PeerIDs)
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
