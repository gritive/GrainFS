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
// group-0 stays reserved for legacy/system paths and is excluded from normal
// hot-bucket object placement.
func SelectObjectPlacementGroup(bucket, key string, groups []ShardGroupEntry, cfg ECConfig) (ShardGroupEntry, error) {
	candidates := make([]ShardGroupEntry, 0, len(groups))
	for _, group := range groups {
		if group.ID == "" || group.ID == "group-0" {
			continue
		}
		if !cfg.IsActive(len(group.PeerIDs)) {
			continue
		}
		candidates = append(candidates, ShardGroupEntry{
			ID:      group.ID,
			PeerIDs: cloneStringSlice(group.PeerIDs),
		})
	}
	if len(candidates) == 0 {
		return ShardGroupEntry{}, fmt.Errorf("no EC-capable object placement group")
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
