package cluster

import (
	"context"
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
		return ShardGroupEntry{}, fmt.Errorf("no EC-capable object placement group")
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
	idx := hashObjectPlacementKey(bucket, key) % uint64(len(candidates))
	return candidates[idx], nil
}

func hashObjectPlacementKey(bucket, key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(bucket))
	_, _ = h.Write([]byte("/"))
	_, _ = h.Write([]byte(key))
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
