package cluster

import (
	"context"
	"fmt"
	"sort"
)

// ObjectIndexShard bundles the read, write, and list faces of one object-index shard.
// At N=1 the single shard wraps the cluster's only meta-raft object index
// (Reader = *MetaFSM, Writer = *ForwardingObjectIndexProposer, Lister = *MetaFSM).
type ObjectIndexShard struct {
	Reader objectIndexLookup
	Writer objectIndexProposer
	Lister objectIndexListSource
}

// ObjectIndexShardSet routes object-index point-reads and writes to one of N
// shards by hash(bucket,key). It implements objectIndexLookup (read) and
// objectIndexProposer (write) so it drops in wherever those interfaces are
// consumed today. At N=1 the selector always returns 0 and every call is a
// pass-through to shard 0 — behavior-neutral.
//
// Sharding key uses hashObjectPlacementKey (the placement hash family) but an
// INDEPENDENT modulus (len(shards)): the index shard count is decoupled from
// the data-group/candidate count so data-group topology changes never reshuffle
// index shards.
//
//	key "b/obj" --hashObjectPlacementKey--> uint64 --% len(shards)--> shard idx
type ObjectIndexShardSet struct {
	shards []ObjectIndexShard
}

var (
	_ objectIndexLookup     = (*ObjectIndexShardSet)(nil)
	_ objectIndexProposer   = (*ObjectIndexShardSet)(nil)
	_ objectIndexListSource = (*ObjectIndexShardSet)(nil)
)

// NewObjectIndexShardSet builds a shard set. At least one shard is required.
func NewObjectIndexShardSet(shards []ObjectIndexShard) (*ObjectIndexShardSet, error) {
	if len(shards) == 0 {
		return nil, fmt.Errorf("object index shardset: at least one shard required")
	}
	return &ObjectIndexShardSet{shards: shards}, nil
}

func (s *ObjectIndexShardSet) shardFor(bucket, key string) int {
	// N=1 fast path: the only shard is 0. Skips hashObjectPlacementKey (a fresh
	// fnv hasher + []byte conversions) so the N=1 façade adds zero per-op alloc
	// over the pre-façade direct lookup — keeping "behavior-neutral pass-through"
	// literally true on the read/write hot path.
	if len(s.shards) == 1 {
		return 0
	}
	return int(hashObjectPlacementKey(bucket, key) % uint64(len(s.shards)))
}

func (s *ObjectIndexShardSet) ObjectIndexLatest(bucket, key string) (ObjectIndexEntry, bool) {
	return s.shards[s.shardFor(bucket, key)].Reader.ObjectIndexLatest(bucket, key)
}

func (s *ObjectIndexShardSet) ObjectIndexVersion(bucket, key, versionID string) (ObjectIndexEntry, bool) {
	return s.shards[s.shardFor(bucket, key)].Reader.ObjectIndexVersion(bucket, key, versionID)
}

func (s *ObjectIndexShardSet) ProposeObjectIndex(ctx context.Context, entry ObjectIndexEntry, preserveLatest bool) error {
	return s.shards[s.shardFor(entry.Bucket, entry.Key)].Writer.ProposeObjectIndex(ctx, entry, preserveLatest)
}

func (s *ObjectIndexShardSet) ProposeDeleteObjectIndex(ctx context.Context, bucket, key, versionID string) error {
	return s.shards[s.shardFor(bucket, key)].Writer.ProposeDeleteObjectIndex(ctx, bucket, key, versionID)
}

func (s *ObjectIndexShardSet) ObjectIndexLatestEntriesPage(bucket, prefix, marker string, maxKeys int) ([]ObjectIndexEntry, bool) {
	if len(s.shards) == 1 {
		return s.shards[0].Lister.ObjectIndexLatestEntriesPage(bucket, prefix, marker, maxKeys)
	}
	merged := make([]ObjectIndexEntry, 0)
	anyTrunc := false
	for i := range s.shards {
		part, trunc := s.shards[i].Lister.ObjectIndexLatestEntriesPage(bucket, prefix, marker, maxKeys)
		merged = append(merged, part...)
		anyTrunc = anyTrunc || trunc
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Key < merged[j].Key })
	truncated := anyTrunc
	if maxKeys > 0 && len(merged) > maxKeys {
		merged = merged[:maxKeys]
		truncated = true
	}
	return merged, truncated
}

func (s *ObjectIndexShardSet) ObjectIndexLatestEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry {
	entries, _ := s.ObjectIndexLatestEntriesPage(bucket, prefix, "", maxKeys)
	return entries
}

func (s *ObjectIndexShardSet) ObjectIndexVersionEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry {
	if len(s.shards) == 1 {
		return s.shards[0].Lister.ObjectIndexVersionEntries(bucket, prefix, maxKeys)
	}
	merged := make([]ObjectIndexEntry, 0)
	for i := range s.shards {
		merged = append(merged, s.shards[i].Lister.ObjectIndexVersionEntries(bucket, prefix, maxKeys)...)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Key != merged[j].Key {
			return merged[i].Key < merged[j].Key
		}
		return merged[i].VersionID > merged[j].VersionID
	})
	if maxKeys > 0 && len(merged) > maxKeys {
		merged = merged[:maxKeys]
	}
	return merged
}
