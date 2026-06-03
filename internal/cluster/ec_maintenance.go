package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"golang.org/x/sync/errgroup"
)

// RepairShard rebuilds a single missing shard by reading the other shards from
// the cluster and writing the reconstructed shardIdx back to its placement
// node. Phase 18 Slice 6: the primitive that ShardPlacementMonitor.onMissing
// plugs into, and that an admin endpoint can trigger on demand.
//
// Preconditions: the object must already have a placement record (created by
// putObjectEC or ConvertObjectToEC). shardIdx must be in [0, k+m). At least k
// of the other shards must be readable or reconstruction fails.
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
// EC placement. Used by startup data WAL repair for segment/coalesced shards.
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

	// ECReconstruct rebuilds the whole object; we then re-split to get the
	// canonical byte layout of each shard (including the missing one).
	data, rerr := ECReconstruct(recCfg, shards)
	if rerr != nil {
		return fmt.Errorf("repair reconstruct: %w", rerr)
	}
	freshShards, serr := ECSplit(recCfg, data)
	if serr != nil {
		return fmt.Errorf("repair re-split: %w", serr)
	}

	// Write just the missing shard back to its placement node.
	target := rec.Nodes[shardIdx]
	var werr error
	if target == selfID {
		werr = b.shardSvc.WriteLocalShard(bucket, shardKey, shardIdx, freshShards[shardIdx])
	} else {
		werr = b.shardSvc.WriteShard(ctx, target, bucket, shardKey, shardIdx, freshShards[shardIdx])
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

// ConvertObjectToEC migrates an existing N×-replicated object to Phase 18
// EC placement. Used by the background re-placement manager (Slice 5).
// Idempotent: if the object already has a placement record, returns nil
// immediately. If the object meta changes mid-conversion (detected via ETag),
// rolls back and returns a retry-able error.
//
// Consistency: etag-check-before-commit. PUT races that land between read and
// commit overwrite both the N× copy and (post-commit) the EC shards, so the
// last writer wins per normal PUT semantics.
func (b *DistributedBackend) ConvertObjectToEC(ctx context.Context, bucket, key string) error {
	liveNodes := b.effectivePlacementNodes()
	effectiveCfg := EffectiveConfig(len(liveNodes), b.currentECConfig())
	if !effectiveCfg.IsActive(len(liveNodes)) || b.shardSvc == nil {
		return fmt.Errorf("ec not active: cluster_size=%d shard_svc=%v",
			len(liveNodes), b.shardSvc != nil)
	}
	// Snapshot meta before reading data so we can detect concurrent writes.
	metaBefore, placementMeta, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("head before convert: %w", err)
	}
	if _, rerr := b.ResolvePlacement(ctx, bucket, key, placementMeta); rerr == nil {
		return nil // already converted
	} else if !errors.Is(rerr, ErrNotEC) {
		return fmt.Errorf("resolve existing placement before convert: %w", rerr)
	}

	// Read the full object via the legacy N× path. GetObject will fall through
	// to local or peer full-replica fetch because placement is still absent.
	rc, _, err := b.GetObject(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("read for convert: %w", err)
	}
	sp, spoolErr := b.spoolPutObject(ctx, bucket, rc)
	closeErr := rc.Close()
	if spoolErr != nil {
		return fmt.Errorf("spool for convert: %w", spoolErr)
	}
	if closeErr != nil {
		sp.Cleanup()
		return fmt.Errorf("close convert source: %w", closeErr)
	}
	defer sp.Cleanup()
	if sp.ETag != metaBefore.ETag {
		return fmt.Errorf("convert aborted: spool ETag mismatch for %s/%s: got %s, want %s",
			bucket, key, sp.ETag, metaBefore.ETag)
	}

	// Re-check meta: did a PUT race us while we were spooling the source?
	metaAfter, err := b.HeadObject(ctx, bucket, key)
	if err != nil || metaAfter.ETag != metaBefore.ETag {
		return fmt.Errorf("convert aborted: meta changed mid-conversion (etag %q → %q)",
			metaBefore.ETag, func() string {
				if metaAfter != nil {
					return metaAfter.ETag
				}
				return ""
			}())
	}

	beforeCommit := func() error {
		metaAfter, err := b.HeadObject(ctx, bucket, key)
		if err != nil || metaAfter.ETag != metaBefore.ETag {
			return fmt.Errorf("convert aborted: meta changed mid-conversion (etag %q → %q)",
				metaBefore.ETag, func() string {
					if metaAfter != nil {
						return metaAfter.ETag
					}
					return ""
				}())
		}
		return nil
	}
	// applyPutObjectMeta writes Tags unconditionally; forward metaBefore.Tags
	// so a legacy N×→EC conversion doesn't clobber existing user tags.
	if _, err := b.putObjectECSpooledWithOptionalModTime(ctx, bucket, key, metaBefore.VersionID, sp, metaBefore.ContentType, metaBefore.UserMetadata, metaBefore.SSEAlgorithm, metaBefore.LastModified, true, metaBefore.ETag, beforeCommit, nil, metaBefore.Tags, ""); err != nil {
		return fmt.Errorf("convert write ec shards: %w", err)
	}
	_, convertedMeta, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("head after convert: %w", err)
	}
	resolved, err := b.ResolvePlacement(ctx, bucket, key, convertedMeta)
	if err != nil {
		return fmt.Errorf("resolve converted placement: %w", err)
	}

	// Cleanup legacy N× replicas on nodes NOT in the placement. The local full-
	// object file is always deleted (whether or not self is a placement node,
	// the full file is now redundant). Best-effort — failures just leave stale
	// N× copies that a future sweep can reclaim.
	_ = os.Remove(b.objectPath(bucket, key))
	if metaBefore.VersionID != "" {
		_ = os.Remove(b.objectPathV(bucket, key, metaBefore.VersionID))
	}
	selfID := b.currentSelfAddr()
	placementSet := make(map[string]bool, len(resolved.Record.Nodes))
	for _, n := range resolved.Record.Nodes {
		placementSet[n] = true
	}
	for _, peer := range b.liveNodes() {
		if peer == selfID {
			continue
		}
		if placementSet[peer] {
			continue // this peer legitimately holds a shard now
		}
		// Peer only had the old full-object N× copy at shardIdx=0; DeleteShards
		// wipes the whole <bucket>/<key>/ dir on that peer.
		_ = b.shardSvc.DeleteShards(ctx, peer, bucket, key)
	}
	return nil
}

// upgradeObjectEC re-encodes an EC object from oldRec's (k1,m1) to newCfg's (k2,m2).
// Called by ReshardManager when the cluster grows and the effective EC config changes.
// Sequence: reconstruct with old config → re-encode with new config → fan-out new shards
// → propose updated placement → delete old shards (best-effort).
func (b *DistributedBackend) upgradeObjectEC(ctx context.Context, bucket, key string, oldRec PlacementRecord, newCfg ECConfig) error {
	if b.shardSvc == nil {
		return fmt.Errorf("shard service unavailable")
	}

	obj, meta, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("upgrade head object: %w", err)
	}
	resolved, err := b.ResolvePlacement(ctx, bucket, key, meta)
	if err != nil {
		return fmt.Errorf("upgrade resolve placement: %w", err)
	}
	if len(resolved.Record.Nodes) > 0 {
		oldRec = resolved.Record
	}
	shardKey := resolved.ShardKey

	// Reconstruct original data from old shards.
	data, err := b.newECObjectReader().ReadObject(ctx, bucket, shardKey, oldRec)
	if err != nil {
		return fmt.Errorf("upgrade reconstruct: %w", err)
	}

	// Re-encode with new config.
	liveNodes := b.effectivePlacementNodes()
	newShards, err := ECSplit(newCfg, data)
	if err != nil {
		return fmt.Errorf("upgrade re-split: %w", err)
	}
	newPlacement := PlacementForNodes(newCfg, liveNodes, shardKey)
	selfID := b.currentSelfAddr()

	var (
		writtenMu sync.Mutex
		written   []string
	)
	cleanup := func() {
		for _, n := range written {
			if n == selfID {
				_ = b.shardSvc.DeleteLocalShards(bucket, shardKey)
			} else {
				_ = b.shardSvc.DeleteShards(ctx, n, bucket, shardKey)
			}
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	for i, node := range newPlacement {
		i, node := i, node
		g.Go(func() error {
			var werr error
			if node == selfID {
				// Serialise the local re-split write with the EC rewrap lane,
				// which acquires the same per-(bucket, shardKey) write lock:
				// shardKey == ecObjectShardKey(key, versionID), the exact key the
				// lane locks on. Without this, a concurrent rewrap could read a
				// shard between this write's tmp+rename and clobber the upgraded
				// content with re-encrypted stale bytes.
				unlock := b.acquireShardWriteLock(bucket, shardKey)
				werr = b.shardSvc.WriteLocalShard(bucket, shardKey, i, newShards[i])
				unlock()
			} else {
				writeCtx, writeCancel := context.WithTimeout(gctx, shardRPCTimeout)
				defer writeCancel()
				werr = b.shardSvc.WriteShard(writeCtx, node, bucket, shardKey, i, newShards[i])
				if b.currentPeerHealth() != nil {
					if werr != nil {
						b.currentPeerHealth().MarkUnhealthy(node)
					} else {
						b.currentPeerHealth().MarkHealthy(node)
					}
				}
			}
			if werr != nil {
				return fmt.Errorf("upgrade write shard %d to %s: %w", i, node, werr)
			}
			writtenMu.Lock()
			written = append(written, node)
			writtenMu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		cleanup()
		return err
	}

	// Commit updated EC placement via ObjectMeta. CmdPutShardPlacement is
	// retained only for legacy decode compatibility and no longer stores rows.
	if perr := b.propose(ctx, CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:           bucket,
		Key:              key,
		Size:             obj.Size,
		ContentType:      obj.ContentType,
		ETag:             obj.ETag,
		ModTime:          obj.LastModified,
		VersionID:        obj.VersionID,
		PlacementGroupID: meta.PlacementGroupID,
		ECData:           uint8(newCfg.DataShards),
		ECParity:         uint8(newCfg.ParityShards),
		NodeIDs:          newPlacement,
		UserMetadata:     cloneStringMap(obj.UserMetadata),
		// applyPutObjectMeta writes Tags unconditionally; forward the existing
		// tags so an EC config upgrade doesn't clobber them to nil.
		Tags: obj.Tags,
	}); perr != nil {
		cleanup()
		return fmt.Errorf("upgrade propose object meta: %w", perr)
	}

	// Best-effort deletion of old shards from nodes no longer in the new placement.
	newSet := make(map[string]struct{}, len(newPlacement))
	for _, n := range newPlacement {
		newSet[n] = struct{}{}
	}
	for _, n := range oldRec.Nodes {
		if _, inNew := newSet[n]; inNew {
			continue
		}
		if n == selfID {
			_ = b.shardSvc.DeleteLocalShards(bucket, shardKey)
		} else {
			_ = b.shardSvc.DeleteShards(ctx, n, bucket, shardKey)
		}
	}
	return nil
}
