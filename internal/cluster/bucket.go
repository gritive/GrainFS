package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/storage"
)

func (b *DistributedBackend) CreateBucket(ctx context.Context, bucket string) error {
	return b.createBucketInternal(ctx, bucket, false)
}

// CreateBucketBypassReserved creates a bucket even when its name is reserved.
// Use only from the bootstrap/seed path. Public API callers must use CreateBucket.
func (b *DistributedBackend) CreateBucketBypassReserved(ctx context.Context, bucket string) error {
	return b.createBucketInternal(ctx, bucket, true)
}

func (b *DistributedBackend) createBucketInternal(ctx context.Context, bucket string, bypassReserved bool) error {
	// All bucket-write proposals go through the meta-Raft via MetaBucketStore.
	// MetaBucketStore must be wired before any bucket-create call; it is set by
	// the composition root (serveruntime) at boot, and by tests via SetMetaBucketStore.
	mbs := b.MetaBucketStore()
	if mbs == nil {
		return fmt.Errorf("create bucket %q: MetaBucketStore not wired", bucket)
	}
	// Existence check via MetaBucketStore (authoritative): group-0 BucketKey
	// is no longer written by bucket creation.
	if _, ok := mbs.Record(bucket); ok {
		return storage.ErrBucketAlreadyExists
	}

	if err := os.MkdirAll(b.bucketDir(bucket), 0o755); err != nil {
		return fmt.Errorf("create bucket dir: %w", err)
	}

	groupID := b.resolveCreateGroupID(ctx, bucket)
	return mbs.CreateBucket(ctx, bucket, groupID, bypassReserved)
}

// resolveCreateGroupID determines the placement group ID for a new bucket.
// Returns "" when no routing is configured (single-group / test deployments).
func (b *DistributedBackend) resolveCreateGroupID(ctx context.Context, bucket string) string {
	if b.router == nil {
		return ""
	}
	if gid, ok := b.router.ExplicitGroup(bucket); ok {
		return gid
	}
	if b.shardGroup != nil {
		entries := b.shardGroup.ShardGroups()
		if group, selErr := SelectObjectPlacementGroup(bucket, "", entries, b.currentECConfig()); selErr == nil {
			return group.ID
		}
		ids := make([]string, 0, len(entries))
		for _, e := range entries {
			ids = append(ids, e.ID)
		}
		sort.Strings(ids)
		return HashAssign(bucket, ids)
	}
	if dg, routeErr := b.router.RouteKey(bucket, ""); routeErr == nil {
		return dg.ID()
	}
	return ""
}

func (b *DistributedBackend) HeadBucket(ctx context.Context, bucket string) error {
	if b.bypassBucketCheck {
		return nil
	}
	mbs := b.MetaBucketStore()
	if mbs == nil {
		return fmt.Errorf("head bucket %q: MetaBucketStore not wired", bucket)
	}
	if _, ok := mbs.Record(bucket); ok {
		return nil
	}
	return storage.ErrBucketNotFound
}

func (b *DistributedBackend) DeleteBucket(ctx context.Context, bucket string) error {
	// Existence check via HeadBucket (which reads from MetaBucketStore — the sole
	// authority). Task 12: the old inline BucketKey read is replaced here since
	// BucketKey is never written by the new meta path.
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}

	// Emptiness. Under blob-authoritative the per-version blob tree (incl. delete
	// markers) + carve-out FSM are the BLOB AUTHORITY; a stale non-carve-out FSM
	// obj: record is non-authoritative and must NOT make an authoritatively-empty
	// bucket look non-empty (the off-path obj: scan would). The authority probe
	// does cluster RPC and opens its OWN store.View (via the carve-out scan), so
	// it MUST run OUTSIDE any txn — never nest it under the existence View above.
	if on, serr := b.blobAuthReadOn(bucket); serr != nil {
		return serr // fail closed
	} else if on {
		vs, lerr := b.listObjectVersionsBlobAuth(bucket, "", 1)
		if lerr != nil {
			return lerr
		}
		if len(vs) > 0 {
			return storage.ErrBucketNotEmpty
		}
	} else {
		// NOT Enabled (never-versioned OR Suspended). Live objects can sit in TWO
		// blob trees and the old FSM obj: scan saw NEITHER (greenfield never writes
		// obj: records via apply), so a non-empty bucket
		// deleted silently (data loss). Both probes are CLUSTER-WIDE — objects are
		// key-hash-placed across shard groups, so a node-local scan would miss
		// objects on other nodes (the cluster coordinator always falls through to
		// this leaf, making it the authoritative cluster-wide emptiness gate). Probe
		// both, fail-closed:
		//   (1) latest-only quorum-meta tree (non-versioned / Suspended NEW writes,
		//       incl. appendable/coalesced carve-outs) via scanQuorumMetaClusterAll —
		//       local strict scan + fan-out to all peers, matching ForceDeleteBucket's
		//       cluster non-versioned enumerate. Filesystem walk + RPC → OUTSIDE any
		//       store.View txn. Tombstones are NOT live objects — skip both kinds,
		//       mirroring the LIST live-filter (filterAndSortEntries): IsDeleteMarker
		//       (the non-versioned / Suspended soft-delete the latest-only tree
		//       actually carries) and IsHardDeleted (defense-in-depth). So a bucket
		//       whose objects were all deleted still deletes.
		//   (2) per-version tree (versions preserved from a prior Enabled era for a
		//       Suspended bucket) via listObjectVersionsBlobAuth (already cluster-wide,
		//       drops IsHardDeleted; a per-version delete MARKER still counts — it is a
		//       version, matching the Enabled branch); empty no-op for a
		//       never-versioned bucket.
		if b.shardSvc != nil {
			cmds, qerr := b.scanQuorumMetaClusterAll(bucket)
			if qerr != nil {
				return fmt.Errorf("delete bucket: enumerate latest-only qmeta: %w", qerr)
			}
			for _, c := range cmds {
				if c.IsDeleteMarker || c.IsHardDeleted {
					continue // latest-only tombstone — the object is gone, not live
				}
				return storage.ErrBucketNotEmpty
			}
		}
		vs, lerr := b.listObjectVersionsBlobAuth(bucket, "", 1)
		if lerr != nil {
			return lerr
		}
		if len(vs) > 0 {
			return storage.ErrBucketNotEmpty
		}
	}

	// Consensus FIRST (via meta-Raft): commit the delete before physical
	// removal. If consensus fails we abort without touching the filesystem so
	// the bucket remains consistent and the caller can retry.
	mbs := b.MetaBucketStore()
	if mbs == nil {
		return fmt.Errorf("delete bucket %q: MetaBucketStore not wired", bucket)
	}
	if err := mbs.DeleteBucket(ctx, bucket); err != nil {
		return err
	}

	// Physical remove only after consensus has committed. The delete is now
	// COMMITTED, so physical cleanup is best-effort: a transient FS error must not
	// report the delete as failed (the bucket record is already gone, so a retry
	// hits HeadBucket-not-found and could never re-run this cleanup — it would just
	// leave inert residue AND a misleading error). Log residue for operator
	// visibility instead; os.RemoveAll is idempotent.
	// b.removeAll is always set in NewDistributedBackend (to os.RemoveAll by
	// default; tests may inject a spy). The nil guard was removed in Task 12.
	if err := b.removeAll(b.bucketDir(bucket)); err != nil {
		log.Warn().Err(err).Str("bucket", bucket).Msg("delete bucket: physical bucket-dir removal failed after committed delete (inert residue left)")
	}

	// os.RemoveAll(bucketDir) clears only {root}/data/{bucket}; the off-raft
	// quorum-meta blob trees live in a separate subtree and must be removed too,
	// else hard-delete tombstone blobs from purgePerVersionBlobs persist as residue.
	if b.shardSvc != nil {
		if err := b.shardSvc.RemoveBucketMetaTrees(bucket); err != nil {
			log.Warn().Err(err).Str("bucket", bucket).Msg("delete bucket: quorum-meta tree removal failed after committed delete (inert residue left)")
		}
	}
	return nil
}

// ForceDeleteBucket deletes all objects in the bucket and then removes it.
// Unlike DeleteBucket, it does not fail when the bucket is non-empty.
//
// Branches on bucket versioning:
//   - versioned (blob-authoritative): enumerate per-version blobs via
//     forceDeleteBucketBlobAuth and delete each via DeleteObjectVersion.
//   - non-versioned: enumerate live latest-only qmeta blobs via
//     scanQuorumMetaBucketStrict, then for each object physically purge shards
//     (fail-closed first) then the qmeta blob. ORDER IS CRITICAL (P0-1d): shards
//     must be gone before the qmeta blob is removed because the qmeta is the only
//     placement record for non-versioned objects; deleting it first would strand
//     shards permanently (the orphan-shard walker does NOT GC non-versioned shards).
func (b *DistributedBackend) ForceDeleteBucket(ctx context.Context, bucket string) error {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	// Versioned buckets: per-version blob tree is BLOB AUTHORITY.
	// Delegate to forceDeleteBucketBlobAuth which enumerates via
	// scanQuorumMetaVersionsClusterAll + DeleteObjectVersion (purges per-version
	// blob+shards). This is the single-DistributedBackend (single-node / direct)
	// path; the cluster path is ClusterCoordinator.
	if on, serr := b.blobAuthReadOn(bucket); serr != nil {
		return serr // fail closed
	} else if on {
		return b.forceDeleteBucketBlobAuth(ctx, bucket)
	}

	// Non-versioned path: enumerate live latest-only qmeta blobs (local-only for
	// the single-node backend; cluster uses scanQuorumMetaClusterAll). For each
	// object, purge shards fail-closed FIRST, then delete the qmeta blob.
	if b.shardSvc == nil {
		// No shardSvc means no qmeta blobs and no shards — fall straight to DeleteBucket.
		return b.DeleteBucket(ctx, bucket)
	}
	cmds, err := b.shardSvc.scanQuorumMetaBucketStrict(bucket)
	if err != nil {
		return fmt.Errorf("force delete: enumerate qmeta: %w", err)
	}
	for _, cmd := range cmds {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Fail-closed on empty placement: a corrupt/incomplete qmeta blob with no
		// NodeIDs would silently no-op both deleteShardsQuorum and
		// deleteQuorumMetaQuorum, stranding shards and the local qmeta blob.
		// Greenfield writers always populate NodeIDs, so an empty set indicates
		// a corrupt blob — abort rather than silently strand residue.
		if len(cmd.NodeIDs) == 0 {
			return fmt.Errorf("force delete: object %q has empty placement (corrupt qmeta blob) — aborting to avoid stranding shards", cmd.Key)
		}
		// Shards FIRST (fail-closed): abort if shards cannot be confirmed gone.
		// Deleting the qmeta first would strand shards with no placement record.
		shardKey := ecObjectShardKey(cmd.Key, "")
		if serr := b.deleteShardsQuorum(ctx, bucket, shardKey, cmd.NodeIDs); serr != nil {
			return fmt.Errorf("force delete: delete shards for %q: %w", cmd.Key, serr)
		}
		// Only after shards confirmed gone: remove the qmeta placement blob.
		if qerr := b.deleteQuorumMetaQuorum(ctx, bucket, cmd.Key, cmd.NodeIDs); qerr != nil {
			return fmt.Errorf("force delete: delete qmeta for %q: %w", cmd.Key, qerr)
		}
	}
	// A Suspended bucket also keeps versions preserved from a prior Enabled era in
	// the per-version tree; purge them too (no-op for a never-versioned bucket) so
	// the final DeleteBucket's per-version emptiness check passes.
	if err := b.purgePerVersionBlobs(ctx, bucket); err != nil {
		return err
	}
	return b.DeleteBucket(ctx, bucket)
}

// forceDeleteBucketBlobAuth is the blob-authoritative (versioned) leaf ForceDeleteBucket
// path (single-node / direct backend). It enumerates per-version blobs cluster-wide
// (incl. delete markers, fail-closed) via scanQuorumMetaVersionsClusterAll and
// deletes each via DeleteObjectVersion, which already purges the per-version blob +
// shards. Finishes with the blob-aware DeleteBucket.
//
// There is no FSM tail: object metadata lives only on the off-raft quorum-meta
// blob, so the per-version blob enumeration is exhaustive.
func (b *DistributedBackend) forceDeleteBucketBlobAuth(ctx context.Context, bucket string) error {
	if err := b.purgePerVersionBlobs(ctx, bucket); err != nil {
		return err
	}
	return b.DeleteBucket(ctx, bucket)
}

// purgePerVersionBlobs deletes every per-version blob (every version, incl. delete
// markers) in the bucket via DeleteObjectVersion (which purges the blob + its
// shards), fail-closed. No-op for a bucket whose per-version tree is empty
// (never-versioned). Shared by the Enabled force path and the non-versioned /
// Suspended force path so a Suspended bucket's versions preserved from a prior
// Enabled era are purged, not orphaned (otherwise the final DeleteBucket's
// per-version emptiness check would reject the force-delete).
func (b *DistributedBackend) purgePerVersionBlobs(ctx context.Context, bucket string) error {
	cmds, err := b.scanQuorumMetaVersionsClusterAll(bucket, "")
	if err != nil {
		return fmt.Errorf("force delete: enumerate per-version blobs: %w", err)
	}
	for _, c := range cmds {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if derr := b.DeleteObjectVersion(bucket, c.Key, c.VersionID); derr != nil {
			return fmt.Errorf("force delete: delete %q@%s: %w", c.Key, c.VersionID, derr)
		}
	}
	return nil
}

// SetBucketVersioning satisfies server.BucketVersioner. Replicates the
// versioning state change through Raft so all cluster nodes apply it atomically.
func (b *DistributedBackend) SetBucketVersioning(bucket, state string) error {
	ctx := context.Background()
	// Pre-check: verify bucket exists locally before proposing. The FSM also
	// checks, but propose() does not propagate FSM errors back to the caller.
	// This is the single-DistributedBackend path (tests, single-node EC
	// setups). The coordinator-driven cluster path uses
	// SetBucketVersioningPropose to bypass this local check after running its
	// own cluster-aware HeadBucket.
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	return b.SetBucketVersioningPropose(bucket, state)
}

// SetBucketVersioningPropose is the coordinator-facing entrypoint: it skips
// the local bucket-existence pre-check because the coordinator has already
// run a cluster-aware HeadBucket. All bucket-write proposals flow through
// the meta-Raft via MetaBucketStore.
func (b *DistributedBackend) SetBucketVersioningPropose(bucket, state string) error {
	ctx := context.Background()
	mbs := b.MetaBucketStore()
	if mbs == nil {
		return fmt.Errorf("set bucket versioning %q: MetaBucketStore not wired", bucket)
	}
	return mbs.SetVersioning(ctx, bucket, state)
}

// SetBucketPolicy satisfies storage.PolicyBackend. The policy document is
// replicated through Raft so every node observes the same bucket policy.
func (b *DistributedBackend) SetBucketPolicy(bucket string, policyJSON []byte) error {
	ctx := context.Background()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	return b.SetBucketPolicyPropose(bucket, policyJSON)
}

// SetBucketPolicyPropose is the coordinator-facing entrypoint: it skips the
// local bucket-existence pre-check after the coordinator has run a
// cluster-aware HeadBucket.
func (b *DistributedBackend) SetBucketPolicyPropose(bucket string, policyJSON []byte) error {
	ctx := context.Background()
	mbs := b.MetaBucketStore()
	if mbs == nil {
		return fmt.Errorf("set bucket policy %q: MetaBucketStore not wired", bucket)
	}
	return mbs.SetPolicy(ctx, bucket, policyJSON)
}

// GetBucketPolicy satisfies storage.PolicyBackend. Reads use the local
// meta-record snapshot; writes flow through meta-Raft via MetaBucketStore.
func (b *DistributedBackend) GetBucketPolicy(bucket string) ([]byte, error) {
	mbs := b.MetaBucketStore()
	if mbs == nil {
		return nil, fmt.Errorf("get bucket policy %q: MetaBucketStore not wired", bucket)
	}
	rec, ok := mbs.Record(bucket)
	if !ok {
		return nil, storage.ErrBucketNotFound
	}
	if len(rec.Policy) == 0 {
		// No policy set. Return ErrBucketNotFound to match the old BadgerDB
		// semantics: the policy key was absent → ErrBucketNotFound. The
		// CompiledPolicyStore loader (loadCommittedBucketPolicy) treats this
		// as "no enforceable policy" and caches a negative entry (allow-all).
		// The server's getBucketPolicy handler maps any error to 404
		// "NoSuchBucketPolicy", which is the correct S3 response.
		return nil, storage.ErrBucketNotFound
	}
	// Deep-copy already done by MetaFSM.BucketRecord.
	return rec.Policy, nil
}

// DeleteBucketPolicy satisfies storage.PolicyBackend.
func (b *DistributedBackend) DeleteBucketPolicy(bucket string) error {
	ctx := context.Background()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	return b.DeleteBucketPolicyPropose(bucket)
}

// DeleteBucketPolicyPropose is the coordinator-facing entrypoint.
func (b *DistributedBackend) DeleteBucketPolicyPropose(bucket string) error {
	ctx := context.Background()
	mbs := b.MetaBucketStore()
	if mbs == nil {
		return fmt.Errorf("delete bucket policy %q: MetaBucketStore not wired", bucket)
	}
	return mbs.DeletePolicy(ctx, bucket)
}

// SetObjectACL satisfies storage.ACLSetter. Updates the ACL via the quorum-meta
// blob RMW on the object's latest-only quorum-meta blob (blob authority — no
// raft path). HeadObject pre-check guarantees existence; a blob miss in the
// propose path is a real not-found or a race.
func (b *DistributedBackend) SetObjectACL(bucket, key string, acl uint8) error {
	ctx := context.Background()
	// Pre-check: verify object exists locally before proposing.
	if _, err := b.HeadObject(ctx, bucket, key); err != nil {
		return err
	}
	return b.SetObjectACLPropose(bucket, key, acl)
}

// SetObjectACLPropose is the coordinator-facing entrypoint: it skips the
// local object-existence pre-check after the coordinator has already resolved
// the object through the cluster-wide object index.
func (b *DistributedBackend) SetObjectACLPropose(bucket, key string, acl uint8) error {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return err
	}
	ctx := context.Background()
	// Blob RMW is the blob authority (data-plane raft-free Slice 2).
	// CmdSetObjectACL is retired; no raft fallback.
	unlock := b.objectMetaRMWLock(bucket, key)
	defer unlock()
	cmd, err := b.readQuorumMetaCmd(bucket, key)
	if err != nil {
		// ErrObjectNotFound here covers two legitimate cases: (1) a real
		// not-found or a race after the HeadObject pre-check, and (2) this node
		// has no shardSvc (non-owner) so readQuorumMetaCmd returns NotFound by
		// construction — the correct outcome, since the owning peer handles it.
		if errors.Is(err, storage.ErrObjectNotFound) {
			return storage.ErrObjectNotFound
		}
		return fmt.Errorf("set object acl quorum read: %w", err)
	}
	cmd.ACL = acl
	cmd.MetaSeq++ // strictly win the (ModTime,VersionID) LWW tie; serialized by the lock
	if werr := b.writeQuorumMeta(ctx, cmd); werr != nil {
		return fmt.Errorf("set object acl quorum: %w", werr)
	}
	return nil
}

// SetObjectTags satisfies storage.ObjectTagsSetter. Mutates tags via the
// quorum-meta blob RMW on the object's latest-only quorum-meta blob (sole
// authority — no raft path). Passing nil tags clears the tag set. Does not
// modify ETag, LastModified, ACL, or blob bytes.
//
// versionID is currently NOT version-scoped: the RMW reads and writes the
// latest-only blob via readQuorumMetaCmd(bucket, key), which ignores versionID.
// The only versionID-aware path was the now-retired CmdSetObjectTags raft
// command (data-plane raft-free Slice 2). Per-version tag targeting is a
// follow-up, not implemented here.
func (b *DistributedBackend) SetObjectTags(bucket, key, versionID string, tags []storage.Tag) error {
	ctx := context.Background()
	// Pre-check: object must exist locally before we propose. Mirrors SetObjectACL.
	if _, err := b.HeadObject(ctx, bucket, key); err != nil {
		return err
	}
	return b.SetObjectTagsPropose(bucket, key, versionID, tags)
}

// SetObjectTagsPropose is the coordinator-facing entrypoint: it skips the
// local object-existence pre-check after the coordinator has already resolved
// the object through the cluster-wide object index.
func (b *DistributedBackend) SetObjectTagsPropose(bucket, key, versionID string, tags []storage.Tag) error {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return err
	}
	ctx := context.Background()
	// Blob RMW is the blob authority (data-plane raft-free Slice 2).
	// CmdSetObjectTags is retired; no raft fallback.
	// objectMetaRMWLock serializes the RMW on THIS node. Sufficient because
	// ClusterCoordinator always forwards to the OWNING peer.
	unlock := b.objectMetaRMWLock(bucket, key)
	defer unlock()
	cmd, err := b.readQuorumMetaCmd(bucket, key)
	if err != nil {
		// ErrObjectNotFound here covers two legitimate cases: (1) a real
		// not-found or a race after the HeadObject pre-check, and (2) this node
		// has no shardSvc (non-owner) so readQuorumMetaCmd returns NotFound by
		// construction — the correct outcome, since the owning peer handles it.
		if errors.Is(err, storage.ErrObjectNotFound) {
			return storage.ErrObjectNotFound
		}
		return fmt.Errorf("set object tags quorum read: %w", err)
	}
	cmd.Tags = append([]storage.Tag(nil), tags...)
	cmd.MetaSeq++ // strictly win the (ModTime,VersionID) LWW tie; serialized by the lock
	if werr := b.writeQuorumMeta(ctx, cmd); werr != nil {
		return fmt.Errorf("set object tags quorum: %w", werr)
	}
	return nil
}

// GetObjectTags satisfies storage.ObjectTagsGetter. Object tags live on the
// off-raft quorum-meta blob (versioned: per-version blob; non-versioned:
// latest-only blob), so tags are read from there — there is no FSM read path.
func (b *DistributedBackend) GetObjectTags(bucket, key, versionID string) ([]storage.Tag, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, err
	}
	// Under blob-authoritative the per-version blob is the SOLE AUTHORITY for a
	// vid-bearing versioned object's tags. A blob MISS is a 404 (no tags) — blob
	// absence for a versioned object means the object is gone.
	if on, err := b.blobAuthReadOn(bucket); err != nil {
		return nil, err // fail closed
	} else if on {
		// DECODE-STRICT (mirrors the HEAD/GET on-branch): a corrupt per-version blob
		// must fail closed rather than be dropped and silently return stale/no tags.
		cmds, verr := b.readQuorumMetaVersionsDecodeStrict(bucket, key)
		if verr != nil {
			return nil, verr
		}
		if versionID == "" {
			// Latest: per-version blobs present are authoritative. A not-live
			// (delete-marker) latest means the versioned object is GONE → 404.
			if len(cmds) > 0 {
				cmd, live := deriveLatestVersion(cmds)
				if live {
					return append([]storage.Tag(nil), cmd.Tags...), nil
				}
				return nil, storage.ErrObjectNotFound
			}
		} else {
			// Specific version: a matching blob is authoritative. A delete-marker
			// blob folds like the object read.
			for _, c := range cmds {
				if c.VersionID == versionID {
					if c.IsHardDeleted {
						return nil, storage.ErrObjectNotFound
					}
					if c.IsDeleteMarker {
						return nil, storage.ErrMethodNotAllowed
					}
					return append([]storage.Tag(nil), c.Tags...), nil
				}
			}
		}
		// per-version MISS under blob authority → object is gone (404).
		return nil, storage.ErrObjectNotFound
	}

	// Non-versioned: tags live on the latest-only quorum-meta blob (SetObjectTags
	// RMWs the blob, not raft). A miss is a 404.
	obj, _, err := b.readQuorumMeta(bucket, key)
	if err != nil {
		return nil, err
	}
	// A soft-deleted non-versioned object leaves an IsDeleteMarker tombstone in
	// the latest-only blob; it is gone → 404 (mirrors headObjectMeta, so HEAD/GET
	// and tag reads agree). Without this a deleted object would return 200 + empty
	// tags.
	if obj.ETag == deleteMarkerETag {
		return nil, storage.ErrObjectNotFound
	}
	// A specific-version request must match the single latest-only blob version;
	// a mismatch means the requested version is not available → 404. Mirrors the
	// per-version guard in headObjectMetaV (object_version.go).
	if versionID != "" && obj.VersionID != versionID {
		return nil, storage.ErrObjectNotFound
	}
	if len(obj.Tags) == 0 {
		return nil, nil
	}
	return append([]storage.Tag(nil), obj.Tags...), nil
}

// GetBucketVersioning satisfies server.BucketVersioner. Returns "Unversioned"
// when no state has been set so the S3 semantic matches ECBackend's default.
//
// This is the LOCAL (best-effort) read: it may be stale on a lagging follower.
// READ paths (GET/LIST/scrub) use it — a slightly-stale versioning view only
// affects read-mode selection, never data integrity, and must stay available in
// a degraded (leaderless) cluster. MUTATING paths must use
// GetBucketVersioningLinearized instead: a stale read there silently mis-versions
// the written object.
func (b *DistributedBackend) GetBucketVersioning(bucket string) (string, error) {
	mbs := b.MetaBucketStore()
	if mbs == nil {
		return "", fmt.Errorf("get bucket versioning %q: MetaBucketStore not wired", bucket)
	}
	rec, _ := mbs.Record(bucket)
	if rec.Versioning == "" {
		return "Unversioned", nil
	}
	return rec.Versioning, nil
}

// GetBucketVersioningLinearized is the LINEARIZABLE read for the MUTATING S3
// edge. Bucket versioning lives in the group-0 raft FSM and a follower's local
// replica can lag (a just-joined follower observed Unversioned for ~90s after
// another node enabled versioning) — so a write resolving versioning from the
// local replica could mis-version the object. We confirm the leader's commit
// index (ReadIndex — forwarded to the group-0 leader when this node is a
// follower) and wait for the local FSM to apply up to it before the local read
// (same primitive object reads use, exec_policy.go ResolveRead).
//
// DEGRADE-TO-LOCAL (not fail-closed): if the ReadIndex barrier can't complete
// (a group-0 leaderless window), we fall back to the local read rather than
// erroring. Erroring here would couple EVERY object write — even to unversioned
// buckets — to group-0 (control-plane) leadership, defeating the multiraft
// property that data writes don't depend on any single group's leader. The
// fallback is the original behavior, and the bug it addresses (a ~90s apply lag)
// occurs while group-0 DOES have a leader, where the barrier succeeds. Non-raft
// backends (nil node) read locally.
func (b *DistributedBackend) GetBucketVersioningLinearized(ctx context.Context, bucket string) (string, error) {
	mbs := b.MetaBucketStore()
	if mbs == nil {
		return "", fmt.Errorf("get bucket versioning (linearized) %q: MetaBucketStore not wired", bucket)
	}
	rec, _, err := mbs.RecordLinearized(ctx, bucket)
	if err != nil {
		return "", err
	}
	if rec.Versioning == "" {
		return "Unversioned", nil
	}
	return rec.Versioning, nil
}

func (b *DistributedBackend) ListBuckets(ctx context.Context) ([]string, error) {
	mbs := b.MetaBucketStore()
	if mbs == nil {
		return nil, fmt.Errorf("list buckets: MetaBucketStore not wired")
	}
	all := mbs.AllRecords()
	buckets := make([]string, 0, len(all))
	for name := range all {
		buckets = append(buckets, name)
	}
	sort.Strings(buckets)
	return buckets, nil
}

// --- Object operations ---
