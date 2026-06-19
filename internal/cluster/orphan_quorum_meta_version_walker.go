package cluster

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// This file implements the per-version orphan reconciliation sweep: it reclaims
// dangling per-version quorum-meta blobs (.quorum_meta_versions/{bucket}/{key}/{vid})
// whose authoritative FSM obj: record no longer exists — the residual left when
// the S2a hard-delete's best-effort per-version dual-delete partially failed (a
// copy lingered on a missed/offline placement node). Until reclaimed, a lingering
// blob makes the S2b derive-by-scan LIST and the per-version HEAD/GET resurface a
// hard-deleted version.
//
// It is the direct analog of orphan_shard_walker.go (EC shard dirs) and reuses
// that file's safety machinery (orphanShardSweepAllowed: gate + all-hosted
// CaughtUp; owningGroupHosted; effectiveOrphanShardAge floor; hostedGroupBackends
// union). Two simplifications the blob form allows: (1) each blob IS a
// PutObjectMetaCmd, so decoding it yields the authoritative (bucket,key,versionID)
// directly — no reverse-parse of a cleaned on-disk path; (2) deletion is
// node-local (deleteQuorumMetaVersionLocal), because the VERIFIED INVARIANT below
// guarantees every node holding a copy is an owning-group member that runs this
// sweep, so each node reclaims its own copy with no cluster fan-out.
//
// VERIFIED SAFETY INVARIANT: a .quorum_meta_versions blob is written ONLY to its
// version's owning-group placement nodes (cmd.NodeIDs), and nothing relocates it
// (the only writers are writeQuorumMeta's fan-out and its peer RPC handler; no
// balancer/repair/reshard touches the subtree). Therefore a blob copy on this node
// implies this node hosts the version's owning generation-group, so the obj:
// record (under that group's ks prefix, raft-replicated by the same writeQuorumMeta
// call) is visible to versionRecordExistsAllHosted here — unless genuinely deleted.
// If a future change ever floats per-version blobs across groups, this invariant
// breaks and the liveness check must become generation-aware.

// versionRecordExistsAllHosted reports whether an FSM obj: record for
// (bucket,key,versionID) exists under ANY locally-hosted group's keyspace prefix,
// and whether that determination is CERTAIN. A delete-marker record counts as
// existing — its per-version blob legitimately represents that version in history
// (a hard delete removes the record entirely, so a true orphan has no record of
// any kind). Fail-closed: any read error returns (false, false) so the caller
// keeps the blob. Unlike hasLiveShardRecord, this is a per-version point lookup
// across hosted groups (NOT a latest-only quorum-meta fallback), so a non-latest
// live version whose obj: record lives under a sibling hosted group is protected.
func (b *DistributedBackend) versionRecordExistsAllHosted(bucket, key, versionID string) (exists, certain bool) {
	for _, gb := range b.hostedGroupBackends() {
		if gb == nil {
			return false, false // fail-closed
		}
		found, err := gb.lookupVersionRecord(bucket, key, versionID)
		if err != nil {
			return false, false // UNCERTAIN → keep
		}
		if found {
			return true, true
		}
	}
	return false, true
}

// lookupVersionRecord reports whether this backend's group holds an obj: record
// for the version (delete-marker or not). ErrMetaKeyNotFound → (false, nil).
func (b *DistributedBackend) lookupVersionRecord(bucket, key, versionID string) (found bool, err error) {
	verr := b.store.View(func(txn MetadataTxn) error {
		_, gerr := txn.Get(b.ks().ObjectMetaKeyV(bucket, key, versionID))
		if gerr != nil {
			if errors.Is(gerr, ErrMetaKeyNotFound) {
				return nil
			}
			return gerr
		}
		found = true
		return nil
	})
	return found, verr
}

// WalkOrphanQuorumMetaVersions yields each per-version quorum-meta blob on this
// node's dataDirs[0]/.quorum_meta_versions subtree whose authoritative FSM obj:
// record is certainly absent (across all hosted groups), whose bucket's owning
// group is locally hosted, and which is older than the floored age gate. Fully
// self-gated: it no-ops unless the sweep gate + all-hosted caught-up gate pass.
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
		exists, certain := b.versionRecordExistsAllHosted(cmd.Bucket, cmd.Key, cmd.VersionID)
		if exists || !certain {
			return nil // live OR uncertain → keep (fail-closed)
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

// DeleteOrphanQuorumMetaVersion re-confirms the orphan decision (TOCTOU close: the
// tombstone loop calls this a cycle after the walk) then deletes ONLY this node's
// local blob copy. Each placement node reclaims its own lingering copy; no cluster
// fan-out (see the VERIFIED SAFETY INVARIANT above).
// Implements scrubber.OrphanQuorumMetaVersionWalkable.
func (b *DistributedBackend) DeleteOrphanQuorumMetaVersion(bucket, key, versionID string) error {
	if b.shardSvc == nil {
		return nil
	}
	if !b.orphanShardSweepAllowed() || !b.owningGroupHosted(bucket) {
		return nil // gate/ownership changed since the walk → do not delete
	}
	if exists, certain := b.versionRecordExistsAllHosted(bucket, key, versionID); exists || !certain {
		return nil // now live or uncertain → do not delete
	}
	epoch, _ := b.GetBucketSoleAuthEpoch(bucket)
	return b.shardSvc.deleteQuorumMetaVersionLocal(bucket, key, versionID, epoch)
}

// Compile-time assertion: DistributedBackend satisfies the scrubber interface.
var _ scrubber.OrphanQuorumMetaVersionWalkable = (*DistributedBackend)(nil)
