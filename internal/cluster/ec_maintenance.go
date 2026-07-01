package cluster

import (
	"context"
	"errors"
	"fmt"
)

// RepairShard rebuilds a single missing shard by reading the other shards from
// the cluster and writing the reconstructed shardIdx back to its placement
// node. Phase 18 Slice 6: the primitive that ShardPlacementMonitor.onMissing
// plugs into, and that an admin endpoint can trigger on demand.
//
// Preconditions: the object must already have a placement record (created by
// the EC write path). shardIdx must be in [0, k+m). At least k of the other
// shards must be readable or reconstruction fails.
//
// versionID identifies the physical shard files on disk: putObjectEC writes
// shards under `{key}/{versionID}/shard_{N}` via ShardService, and this
// routine must read/write the same layout. When versionID is empty, the
// latest pointer from the FSM is consulted.
//
// Write target: the repaired shard goes back to placement[shardIdx]. When
// that node is this node, we use WriteLocalShard; otherwise WriteShard.
func (b *DistributedBackend) RepairShard(ctx context.Context, bucket, key, versionID string, shardIdx int) error {
	if b.shardSvc == nil {
		return fmt.Errorf("shard service not configured")
	}
	// Resolve to the latest version when caller doesn't know it (monitor
	// callback path). Empty latest means pre-versioned legacy EC; fall back
	// to bare-key layout, preserving pre-Slice-3 behaviour.
	if versionID == "" {
		latest, lerr := b.fsm.LookupLatestVersion(bucket, key)
		if lerr != nil {
			return fmt.Errorf("resolve version for repair %s/%s: %w", bucket, key, lerr)
		}
		versionID = latest
	}
	resolved, lookupErr := b.ResolvePlacement(ctx, bucket, key, b.readPlacementMeta(bucket, key, versionID))
	if errors.Is(lookupErr, ErrNotEC) {
		return fmt.Errorf("no placement for %s/%s — object is not EC-managed", bucket, key)
	}
	if lookupErr != nil {
		return fmt.Errorf("lookup shard placement: %w", lookupErr)
	}
	return b.reconstructShardAtKey(ctx, bucket, resolved.ShardKey, resolved.Record, shardIdx)
}

// RepairShardAtShardKey reconstructs the local shardIdx shard for an arbitrary
// physical shard key (object-version, segment, or coalesced) given its already-resolved
// EC placement. Used by the placement monitor / repair paths for segment/coalesced shards.
func (b *DistributedBackend) RepairShardAtShardKey(ctx context.Context, bucket, shardKey string, rec PlacementRecord, shardIdx int) error {
	if b.shardSvc == nil {
		return fmt.Errorf("shard service not configured")
	}
	return b.reconstructShardAtKey(ctx, bucket, shardKey, rec, shardIdx)
}

// reconstructShardAtKey reads survivors from rec.Nodes at shardKey, EC-reconstructs
// the stripe, and writes the local shardIdx shard. Shared by RepairShard and
// RepairShardAtShardKey.
// Callers must ensure b.shardSvc != nil.
func (b *DistributedBackend) reconstructShardAtKey(ctx context.Context, bucket, shardKey string, rec PlacementRecord, shardIdx int) error {
	recCfg := rec.ECConfigOrFallback(b.currentECConfig())
	if shardIdx < 0 || shardIdx >= len(rec.Nodes) {
		return fmt.Errorf("shardIdx %d out of range [0,%d)", shardIdx, len(rec.Nodes))
	}
	if len(rec.Nodes) != recCfg.NumShards() {
		return fmt.Errorf("placement length %d != k+m %d", len(rec.Nodes), recCfg.NumShards())
	}

	selfID := b.currentSelfAddr()
	shards := make([][]byte, len(rec.Nodes))
	available := 0

	// Pull every OTHER shard. We intentionally skip shardIdx to avoid pulling
	// the corrupt/missing copy into the reconstruction.
	for i, node := range rec.Nodes {
		if i == shardIdx {
			continue
		}
		var data []byte
		var rerr error
		if node == selfID {
			data, rerr = b.shardSvc.ReadLocalShard(bucket, shardKey, i)
		} else {
			data, rerr = b.shardSvc.ReadShard(ctx, node, bucket, shardKey, i)
		}
		if rerr == nil && data != nil {
			shards[i] = data
			available++
		}
	}
	if available < recCfg.DataShards {
		return fmt.Errorf("repair: only %d/%d other shards readable, need %d",
			available, len(rec.Nodes)-1, recCfg.DataShards)
	}

	// Regenerate the missing shard's body in the SAME layout the surviving
	// siblings use. For a STRIPED object the siblings are stripe-interleaved, so a
	// contiguous ECSplit would produce a shard that does not match them — future
	// reads/reconstructs would break. stripeReconstructShardBody slices each
	// surviving sibling at the per-stripe fragment offsets the de-interleave
	// reader uses and Reed-Solomon Reconstructs the missing fragment per stripe,
	// so the regenerated shard is byte-identical to what the pipeline wrote.
	// ecReconstructBodies only strips the on-disk 8-byte headers (missing shard
	// stays nil); it does NOT itself reconstruct.
	var freshShardBody []byte
	if rec.StripeBytes > 0 {
		origSize, bodies, berr := ecReconstructBodies(recCfg, shards)
		if berr != nil {
			return fmt.Errorf("repair reconstruct bodies: %w", berr)
		}
		body, serr := stripeReconstructShardBody(recCfg, bodies, int(rec.StripeBytes), shardIdx, origSize)
		if serr != nil {
			return fmt.Errorf("repair stripe reconstruct shard: %w", serr)
		}
		// Match on-disk format: 8-byte header + interleaved body (same as ECSplit's
		// header-prefixed shard). Header records the full object size.
		hdr := ShardHeader(origSize)
		freshShardBody = append(hdr[:], body...)
	} else {
		data, rerr := ECReconstruct(recCfg, shards)
		if rerr != nil {
			return fmt.Errorf("repair reconstruct: %w", rerr)
		}
		freshShards, serr := ECSplit(recCfg, data)
		if serr != nil {
			return fmt.Errorf("repair re-split: %w", serr)
		}
		freshShardBody = freshShards[shardIdx]
	}

	// Write just the missing shard back to its placement node.
	target := rec.Nodes[shardIdx]
	var werr error
	if target == selfID {
		werr = b.shardSvc.WriteLocalShard(bucket, shardKey, shardIdx, freshShardBody)
	} else {
		werr = b.shardSvc.WriteShard(ctx, target, bucket, shardKey, shardIdx, freshShardBody)
	}
	if werr == nil && b.shardCache != nil {
		// Repaired shard bytes may differ from any cached copy of the
		// corrupted slot. Drop the cache entry so subsequent reads pull
		// fresh data — repaint > stale.
		b.shardCache.Invalidate(shardCacheKey(bucket, shardKey, shardIdx))
		b.shardCache.InvalidatePrefix(shardRangeCachePrefix(bucket, shardKey, shardIdx))
	}
	return werr
}
