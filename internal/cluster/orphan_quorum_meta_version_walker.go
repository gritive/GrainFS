package cluster

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// This file implements the per-version blob reconciliation sweep under blob-primary.
// The per-version quorum-meta blob (.quorum_meta_versions/{bucket}/{key}/{vid}) is
// the BLOB AUTHORITY for a versioned object; a hard delete REPLACES the data blob
// with a TOMBSTONE (IsHardDeleted) rather than physically purging it. This sweep is
// the eventual-consistency reconciler for those tombstones — the blob-primary
// replacement for raft's delete propagation. It does two things, both deleting only
// THIS node's local blob copy (no cluster fan-out of the deletion):
//
//  1. RECLAIM a stale DATA blob: a data blob (IsHardDeleted=false) whose
//     authoritative cross-cluster state for that exact version is a tombstone — i.e.
//     this node missed the hard delete and kept the old data blob. Reclaiming it
//     stops the derive-by-scan LIST/HEAD from resurfacing the dead version once the
//     tombstone-holding peers are reachable again.
//  2. GC a TOMBSTONE: a tombstone whose delete has FULLY PROPAGATED — every
//     placement node is reachable AND none still holds a live data blob for the
//     version. Only then is the tombstone safe to drop; until then a node that still
//     has the data blob could resurrect the version, so the tombstone must remain to
//     win the LWW. This is downtime-independent (no wall-clock grace), so a node
//     offline for any duration cannot cause a time-based resurrection.
//
// Fail-closed throughout: any peer-read uncertainty (unreachable / decode error)
// keeps the blob. Reuses orphan_shard_walker.go's gate machinery
// (orphanShardSweepAllowed + owningGroupHosted + effectiveOrphanShardAge floor —
// the age floor only avoids racing an in-flight write, NOT a propagation grace).
// Unlike the old FSM-oracle sweep, the decision now does cross-cluster reads (the
// tombstone lives only where the delete reached; a lagging node must fan out to
// learn it), so this is no longer a purely node-local oracle.

// perVersionBlobReclaimable reports whether THIS node's local per-version blob
// (decoded into cmd) is safe to delete under blob-primary reconciliation:
//   - a TOMBSTONE → reclaimable once tombstoneConverged (delete fully propagated);
//   - a DATA blob → reclaimable when the authoritative cross-cluster state for the
//     exact version is a tombstone (a peer hard-deleted it; this node missed it).
//
// Fail-closed: any read uncertainty returns false (keep the blob).
func (b *DistributedBackend) perVersionBlobReclaimable(cmd PutObjectMetaCmd) bool {
	if cmd.IsHardDeleted {
		return b.tombstoneConverged(cmd.Bucket, cmd.Key, cmd.VersionID, cmd.NodeIDs)
	}
	auth, ok, err := b.readQuorumMetaVersionDecodeStrict(cmd.Bucket, cmd.Key, cmd.VersionID)
	if err != nil || !ok {
		return false // uncertain or absent cluster-wide → keep (fail-closed)
	}
	return auth.IsHardDeleted // authoritative state is a tombstone → this data blob is stale
}

func (b *DistributedBackend) latestTombstoneReclaimable(cmd PutObjectMetaCmd) bool {
	if !cmd.IsDeleteMarker && !cmd.IsHardDeleted {
		return false
	}
	return b.latestTombstoneConverged(cmd.Bucket, cmd.Key, cmd.NodeIDs)
}

// tombstoneConverged reports whether a hard-delete tombstone has fully propagated:
// EVERY placement node (nodeIDs) is reachable AND none still holds a live (non-
// tombstone) data blob for the version. Only then is dropping the tombstone safe —
// no remaining data blob can resurrect the version. Fail-closed: an unreachable
// node, a read error, or empty placement returns false (keep the tombstone). This
// replaces a wall-clock grace, so convergence — not elapsed time — gates GC.
func (b *DistributedBackend) tombstoneConverged(bucket, key, versionID string, nodeIDs []string) bool {
	if b.shardSvc == nil || len(nodeIDs) == 0 {
		return false
	}
	self := b.currentSelfAddr()
	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()
	for _, node := range nodeIDs {
		var cmds []PutObjectMetaCmd
		var err error
		if node == self {
			cmds, err = b.shardSvc.readQuorumMetaVersionsLocal(bucket, key)
		} else {
			addr, aerr := b.shardSvc.resolvePeerAddress(node)
			if aerr != nil {
				return false // unreachable → cannot confirm convergence → keep
			}
			cmds, err = b.shardSvc.ReadQuorumMetaVersions(ctx, addr, bucket, key)
		}
		if err != nil {
			return false // read error → keep (fail-closed)
		}
		for _, c := range cmds {
			if c.VersionID == versionID && !c.IsHardDeleted {
				return false // a node still holds a live data blob → not converged
			}
		}
	}
	return true // all reachable, none holds a data blob → safe to GC the tombstone
}

// latestTombstoneConverged is the latest-only sibling of tombstoneConverged. A
// non-versioned DeleteObject writes an IsDeleteMarker blob under .quorum_meta; it
// is safe to drop a local marker only after every placement node is reachable and
// none still holds a live latest-only data blob for the key.
func (b *DistributedBackend) latestTombstoneConverged(bucket, key string, nodeIDs []string) bool {
	if b.shardSvc == nil || len(nodeIDs) == 0 {
		return false
	}
	self := b.currentSelfAddr()
	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()
	for _, node := range nodeIDs {
		var raw []byte
		var err error
		if node == self {
			raw, err = b.shardSvc.readQuorumMetaRaw(bucket, key)
			if errors.Is(err, storage.ErrObjectNotFound) {
				continue
			}
		} else {
			addr, aerr := b.shardSvc.resolvePeerAddress(node)
			if aerr != nil {
				return false
			}
			raw, err = b.shardSvc.ReadQuorumMetaRaw(ctx, addr, bucket, key)
			if err == nil && len(raw) == 0 {
				continue
			}
		}
		if err != nil {
			return false
		}
		cmd, derr := decodeQuorumMetaBlob(raw)
		if derr != nil || cmd.Bucket != bucket || cmd.Key != key {
			return false
		}
		if !cmd.IsDeleteMarker && !cmd.IsHardDeleted {
			return false
		}
	}
	return true
}

// WalkOrphanQuorumMetaVersions yields each per-version quorum-meta blob on this
// node's dataDirs[0]/.quorum_meta_versions subtree that is reclaimable under
// blob-primary tombstone reconciliation (perVersionBlobReclaimable: a stale data
// blob superseded by a cluster-authoritative tombstone, or a tombstone whose
// delete has fully propagated), whose bucket's owning group is locally hosted, and
// which is older than the floored age gate. Fully self-gated: it no-ops unless the
// sweep gate + all-hosted caught-up gate pass.
// Implements scrubber.OrphanQuorumMetaVersionWalkable.
func (b *DistributedBackend) WalkOrphanQuorumMetaVersions(
	fn func(bucket, key, versionID, path string) error,
) error {
	if b.shardSvc == nil || !b.orphanShardSweepAllowed() {
		return nil
	}
	dataDirs := b.shardSvc.DataDirs()
	if len(dataDirs) == 0 {
		return nil
	}
	root := filepath.Join(dataDirs[0], quorumMetaVersionsSubDir)
	cutoff := time.Now().Add(-b.effectiveOrphanShardAge())

	var stopErr error
	walkLatest := func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			if os.IsNotExist(werr) {
				return nil
			}
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() || isQuorumMetaTempName(d.Name()) {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil || info.ModTime().After(cutoff) {
			return nil
		}
		data, rerr := os.ReadFile(p)
		if rerr != nil {
			return nil
		}
		cmd, derr := b.shardSvc.decodeQuorumMetaCmdBlob(data)
		if derr != nil || cmd.Bucket == "" || cmd.Key == "" {
			return nil
		}
		if !b.owningGroupHosted(cmd.Bucket) || !b.latestTombstoneReclaimable(cmd) {
			return nil
		}
		if ferr := fn(cmd.Bucket, cmd.Key, "", p); ferr != nil {
			stopErr = ferr
			return filepath.SkipAll
		}
		return nil
	}
	latestRoot := filepath.Join(dataDirs[0], quorumMetaSubDir)
	if walkErr := filepath.WalkDir(latestRoot, walkLatest); walkErr != nil && !os.IsNotExist(walkErr) {
		return fmt.Errorf("walk quorum-meta root %s: %w", latestRoot, walkErr)
	}
	if stopErr != nil {
		return stopErr
	}

	walkErr := filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			if os.IsNotExist(werr) {
				return nil
			}
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() || isQuorumMetaTempName(d.Name()) {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil || info.ModTime().After(cutoff) {
			return nil // unreadable stat or within age floor → keep
		}
		data, rerr := os.ReadFile(p)
		if rerr != nil {
			return nil // transient unreadable blob → keep
		}
		cmd, derr := b.shardSvc.decodeQuorumMetaCmdBlob(data)
		if derr != nil || cmd.Bucket == "" || cmd.VersionID == "" {
			return nil // undecodable → keep (never delete what we can't authoritatively judge)
		}
		if !b.owningGroupHosted(cmd.Bucket) {
			return nil // owner not hosted here → can't judge → keep
		}
		if !b.perVersionBlobReclaimable(cmd) {
			return nil // live data blob, un-converged tombstone, or uncertain → keep
		}
		if ferr := fn(cmd.Bucket, cmd.Key, cmd.VersionID, p); ferr != nil {
			stopErr = ferr
			return filepath.SkipAll
		}
		return nil
	})
	if walkErr != nil {
		return fmt.Errorf("walk quorum-meta versions root %s: %w", root, walkErr)
	}
	return stopErr
}

// DeleteOrphanQuorumMetaVersion re-confirms the reclaim decision (TOCTOU close: the
// scrubber calls this a cycle after the walk) then deletes ONLY this node's local
// blob copy (data blob or tombstone). It re-reads the local blob so the re-confirm
// runs against the CURRENT on-disk state (a fresh PUT or a just-propagated tombstone
// may have changed it since the walk).
// Implements scrubber.OrphanQuorumMetaVersionWalkable.
func (b *DistributedBackend) DeleteOrphanQuorumMetaVersion(bucket, key, versionID string) error {
	if b.shardSvc == nil {
		return nil
	}
	if !b.orphanShardSweepAllowed() || !b.owningGroupHosted(bucket) {
		return nil // gate/ownership changed since the walk → do not delete
	}
	if versionID == "" {
		raw, err := b.shardSvc.readQuorumMetaRaw(bucket, key)
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil
		}
		if err != nil {
			return nil
		}
		cur, derr := b.shardSvc.decodeQuorumMetaCmdBlob(raw)
		if derr != nil || !b.latestTombstoneReclaimable(cur) {
			return nil
		}
		return b.shardSvc.deleteQuorumMetaLocal(bucket, key)
	}
	local, err := b.shardSvc.readQuorumMetaVersionsLocal(bucket, key)
	if err != nil {
		return nil // cannot re-read → do not delete (fail-closed)
	}
	var cur *PutObjectMetaCmd
	for i := range local {
		if local[i].VersionID == versionID {
			cur = &local[i]
			break
		}
	}
	if cur == nil || !b.perVersionBlobReclaimable(*cur) {
		return nil // already gone, or no longer reclaimable → do not delete
	}
	return b.shardSvc.deleteQuorumMetaVersionLocal(bucket, key, versionID)
}

// Compile-time assertion: DistributedBackend satisfies the scrubber interface.
var _ scrubber.OrphanQuorumMetaVersionWalkable = (*DistributedBackend)(nil)
