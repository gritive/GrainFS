package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

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
	// Check if already exists (read local)
	err := b.store.View(func(txn MetadataTxn) error {
		_, err := txn.Get(b.ks().BucketKey(bucket))
		return err
	})
	if err == nil {
		return storage.ErrBucketAlreadyExists
	}
	if err != ErrMetaKeyNotFound {
		return err
	}

	if err := os.MkdirAll(b.bucketDir(bucket), 0o755); err != nil {
		return fmt.Errorf("create bucket dir: %w", err)
	}

	// PR-D: persist bucket→group assignment in meta-Raft before data-Raft create.
	// assigner nil means single-node or not-yet-wired (legacy skip).
	// If ProposeBucketAssignment succeeds but b.propose(CmdCreateBucket) below fails,
	// the assignment is durable but the bucket key won't exist yet. A retry will
	// re-propose (idempotent overwrite) and re-create — safe by design.
	if b.assigner != nil {
		if b.router == nil {
			return fmt.Errorf("create bucket %q: router not configured", bucket)
		}
		// Determine target group:
		//   1. If meta-FSM has an explicit assignment (e.g., from rebalance), preserve it.
		//   2. Else, hash-assign across active groups (if shardGroup wired).
		//   3. Else, fall back to the router's default group (single-group / test deployments).
		groupID := ""
		if gid, ok := b.router.ExplicitGroup(bucket); ok {
			groupID = gid
		}
		if groupID == "" && b.shardGroup != nil {
			entries := b.shardGroup.ShardGroups()
			if group, selErr := SelectObjectPlacementGroup(bucket, "", entries, b.currentECConfig()); selErr == nil {
				groupID = group.ID
			} else {
				ids := make([]string, 0, len(entries))
				for _, e := range entries {
					ids = append(ids, e.ID)
				}
				sort.Strings(ids) // deterministic legacy fallback
				groupID = HashAssign(bucket, ids)
			}
		}
		if groupID == "" {
			if dg, routeErr := b.router.RouteKey(bucket, ""); routeErr == nil {
				groupID = dg.ID()
			}
		}
		if groupID == "" {
			return fmt.Errorf("create bucket %q: no active groups for assignment", bucket)
		}
		if propErr := b.assigner.ProposeBucketAssignment(ctx, bucket, groupID); propErr != nil {
			return fmt.Errorf("propose bucket assignment: %w", propErr)
		}
	}

	return b.propose(ctx, CmdCreateBucket, CreateBucketCmd{Bucket: bucket, BypassReserved: bypassReserved})
}

func (b *DistributedBackend) HeadBucket(ctx context.Context, bucket string) error {
	if b.bypassBucketCheck {
		return nil
	}
	return b.store.View(func(txn MetadataTxn) error {
		_, err := txn.Get(b.ks().BucketKey(bucket))
		if err == ErrMetaKeyNotFound {
			return storage.ErrBucketNotFound
		}
		return err
	})
}

func (b *DistributedBackend) DeleteBucket(ctx context.Context, bucket string) error {
	// Existence check (always).
	if err := b.store.View(func(txn MetadataTxn) error {
		_, err := txn.Get(b.ks().BucketKey(bucket))
		if err == ErrMetaKeyNotFound {
			return storage.ErrBucketNotFound
		}
		return err
	}); err != nil {
		return err
	}

	// Emptiness. Under soleauth=on the per-version blob tree (incl. delete
	// markers) + carve-out FSM are the SOLE AUTHORITY; a stale non-carve-out FSM
	// obj: record is non-authoritative and must NOT make an authoritatively-empty
	// bucket look non-empty (the off-path obj: scan would). The authority probe
	// does cluster RPC and opens its OWN store.View (via the carve-out scan), so
	// it MUST run OUTSIDE any txn — never nest it under the existence View above.
	if on, serr := b.soleAuthReadOn(bucket); serr != nil {
		return serr // fail closed
	} else if on {
		vs, lerr := b.listObjectVersionsSoleAuth(bucket, "", 1)
		if lerr != nil {
			return lerr
		}
		if len(vs) > 0 {
			return storage.ErrBucketNotEmpty
		}
	} else {
		// NOT Enabled (never-versioned OR Suspended). Live objects can sit in TWO
		// blob trees and the old FSM obj: scan saw NEITHER (greenfield never writes
		// obj: records -- CmdPutObjectMeta apply is a no-op), so a non-empty bucket
		// deleted silently (data loss). Both probes are CLUSTER-WIDE — objects are
		// key-hash-placed across shard groups, so a node-local scan would miss
		// objects on other nodes (the cluster coordinator always falls through to
		// this leaf, making it the authoritative cluster-wide emptiness gate). Probe
		// both, fail-closed:
		//   (1) latest-only quorum-meta tree (non-versioned / Suspended NEW writes,
		//       incl. appendable/coalesced carve-outs) via scanQuorumMetaClusterAll —
		//       local strict scan + fan-out to all peers, matching ForceDeleteBucket's
		//       cluster non-versioned enumerate. Filesystem walk + RPC → OUTSIDE any
		//       store.View txn. Hard-delete tombstones (IsHardDeleted) are NOT live
		//       objects — skip them (mirrors dropHardDeletedVersions) so a bucket whose
		//       objects were all deleted still deletes.
		//   (2) per-version tree (versions preserved from a prior Enabled era for a
		//       Suspended bucket) via listObjectVersionsSoleAuth (already cluster-wide,
		//       already tombstone-filtered, delete markers still count); empty no-op
		//       for a never-versioned bucket.
		if b.shardSvc != nil {
			cmds, qerr := b.scanQuorumMetaClusterAll(bucket)
			if qerr != nil {
				return fmt.Errorf("delete bucket: enumerate latest-only qmeta: %w", qerr)
			}
			for _, c := range cmds {
				if c.IsHardDeleted {
					continue // hard-delete tombstone — the object is gone, not live
				}
				return storage.ErrBucketNotEmpty
			}
		}
		vs, lerr := b.listObjectVersionsSoleAuth(bucket, "", 1)
		if lerr != nil {
			return lerr
		}
		if len(vs) > 0 {
			return storage.ErrBucketNotEmpty
		}
	}

	if err := os.RemoveAll(b.bucketDir(bucket)); err != nil {
		return fmt.Errorf("remove bucket dir: %w", err)
	}

	return b.propose(ctx, CmdDeleteBucket, DeleteBucketCmd{Bucket: bucket})
}

// ForceDeleteBucket deletes all objects in the bucket and then removes it.
// Unlike DeleteBucket, it does not fail when the bucket is non-empty.
//
// Branches on bucket versioning:
//   - versioned (soleauth=on): enumerate per-version blobs via
//     forceDeleteBucketSoleAuth and delete each via DeleteObjectVersion.
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
	// Versioned buckets: per-version blob tree is SOLE AUTHORITY.
	// Delegate to forceDeleteBucketSoleAuth which enumerates via
	// scanQuorumMetaVersionsClusterAll + DeleteObjectVersion (purges per-version
	// blob+shards). This is the single-DistributedBackend (single-node / direct)
	// path; the cluster path is ClusterCoordinator.
	if on, serr := b.soleAuthReadOn(bucket); serr != nil {
		return serr // fail closed
	} else if on {
		return b.forceDeleteBucketSoleAuth(ctx, bucket)
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
	return b.DeleteBucket(ctx, bucket)
}

// forceDeleteBucketSoleAuth is the soleauth=on (versioned) leaf ForceDeleteBucket
// path (single-node / direct backend). It enumerates per-version blobs cluster-wide
// (incl. delete markers, fail-closed) via scanQuorumMetaVersionsClusterAll and
// deletes each via DeleteObjectVersion, which already purges the per-version blob +
// shards. Finishes with the blob-aware DeleteBucket.
//
// The legacy raft tail (scanFsmCarveoutVersions + HardDeleteLegacyObject) has been
// dropped: greenfield versioned buckets have no legacy-bare FSM carve-outs, and Task
// 4c will retire CmdDeleteObject entirely.
func (b *DistributedBackend) forceDeleteBucketSoleAuth(ctx context.Context, bucket string) error {
	// Versioned blobs (every version, incl. delete markers), fail-closed.
	cmds, err := b.scanQuorumMetaVersionsClusterAll(bucket, "")
	if err != nil {
		return fmt.Errorf("force delete (soleauth): enumerate blobs: %w", err)
	}
	for _, c := range cmds {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if derr := b.DeleteObjectVersion(bucket, c.Key, c.VersionID); derr != nil {
			return fmt.Errorf("force delete (soleauth): delete %q@%s: %w", c.Key, c.VersionID, derr)
		}
	}
	return b.DeleteBucket(ctx, bucket)
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
// run a cluster-aware HeadBucket. On a freshly bootstrapped cluster a
// follower may have the meta-Raft bucket assignment without having applied
// the data-Raft CmdCreateBucket entry locally; calling SetBucketVersioning
// from that follower would falsely reject the request with NoSuchBucket.
func (b *DistributedBackend) SetBucketVersioningPropose(bucket, state string) error {
	return b.propose(context.Background(), CmdSetBucketVersioning, SetBucketVersioningCmd{
		Bucket: bucket,
		State:  state,
	})
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
	return b.propose(ctx, CmdSetBucketPolicy, SetBucketPolicyCmd{
		Bucket:     bucket,
		PolicyJSON: append([]byte(nil), policyJSON...),
	})
}

// GetBucketPolicy satisfies storage.PolicyBackend. Reads use the local
// FSM-consistent view; writes flow through Raft.
func (b *DistributedBackend) GetBucketPolicy(bucket string) ([]byte, error) {
	var data []byte
	err := b.store.View(func(txn MetadataTxn) error {
		item, err := txn.Get(b.ks().BucketPolicyKey(bucket))
		if err == ErrMetaKeyNotFound {
			return storage.ErrBucketNotFound
		}
		if err != nil {
			return err
		}
		data, err = b.itemValueCopy(item)
		return err
	})
	return data, err
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
	return b.propose(ctx, CmdDeleteBucketPolicy, DeleteBucketPolicyCmd{Bucket: bucket})
}

// SetObjectACL satisfies storage.ACLSetter. Updates the ACL via the quorum-meta
// blob RMW on the object's latest-only quorum-meta blob (sole authority — no
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
	// Blob RMW is the sole authority (data-plane raft-free Slice 2).
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
	// Blob RMW is the sole authority (data-plane raft-free Slice 2).
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

// GetObjectTags satisfies storage.ObjectTagsGetter. Reads from the local
// FSM-consistent view; writes flow through Raft and replicate to every
// node, so the local view is always current modulo replication lag.
func (b *DistributedBackend) GetObjectTags(bucket, key, versionID string) ([]storage.Tag, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, err
	}
	// S4c-c-read1 T3: under soleauth=on the per-version blob is the SOLE
	// AUTHORITY for a vid-bearing versioned object's tags. A blob MISS never
	// falls through to a stale vid-bearing FSM record — blob absence for a
	// versioned object is a 404 (no tags). Only carve-out classes
	// (appendable/coalesced/legacy bare-unversioned) stay FSM-authoritative.
	if on, err := b.soleAuthReadOn(bucket); err != nil {
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
			// (delete-marker) latest means the versioned object is GONE → 404; do
			// NOT fall through to a carve-out (codex code-gate [P1]). Only a true
			// per-version MISS (no blobs for this key) is eligible for carve-out.
			if len(cmds) > 0 {
				cmd, live := deriveLatestVersion(cmds)
				if live {
					return append([]storage.Tag(nil), cmd.Tags...), nil
				}
				return nil, storage.ErrObjectNotFound
			}
		} else {
			// Specific version: a matching blob is authoritative. A delete-marker
			// blob folds like the object read (codex code-gate [P2]). A vid not in
			// the blob tree falls to carve-out (mirrors T2 headObjectMetaV).
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
		// per-version MISS under on → carve-out classes ONLY.
		obj, _, carve, cerr := b.fsmCarveoutObject(bucket, key, versionID)
		if cerr != nil {
			return nil, cerr
		}
		if carve {
			return append([]storage.Tag(nil), obj.Tags...), nil
		}
		// No vid-bearing-versioned FSM resurrection under sole authority.
		return nil, storage.ErrObjectNotFound
	}

	var result []storage.Tag
	err := b.store.View(func(txn MetadataTxn) error {
		dbKey := b.ks().ObjectMetaKey(bucket, key)
		if versionID != "" {
			dbKey = b.ks().ObjectMetaKeyV(bucket, key, versionID)
		}
		item, err := txn.Get(dbKey)
		if err == ErrMetaKeyNotFound {
			return storage.ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		val, err := b.itemValueCopy(item)
		if err != nil {
			return err
		}
		m, err := unmarshalObjectMeta(val)
		if err != nil {
			return err
		}
		if len(m.Tags) > 0 {
			result = append([]storage.Tag(nil), m.Tags...)
		}
		return nil
	})
	return result, err
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
	var state string
	err := b.store.View(func(txn MetadataTxn) error {
		item, err := txn.Get(b.ks().BucketVerKey(bucket))
		if err == ErrMetaKeyNotFound {
			state = "Unversioned"
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			state = string(v)
			return nil
		})
	})
	return state, err
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
	if b.node != nil {
		readCtx, cancel := context.WithTimeout(ctx, localExecFollowerReadDeadline)
		idx, err := b.ReadIndex(readCtx)
		if err == nil {
			err = b.WaitApplied(readCtx, idx)
		}
		cancel()
		if err != nil {
			b.logger.Debug().Err(err).Str("bucket", bucket).
				Msg("bucket-versioning linearizing read barrier unavailable; degrading to local read")
		}
	}
	return b.GetBucketVersioning(bucket)
}

func (b *DistributedBackend) ListBuckets(ctx context.Context) ([]string, error) {
	var buckets []string
	err := b.store.View(func(txn MetadataTxn) error {
		return b.ks().scanGroupPrefix(txn, []byte("bucket:"), func(rawKey []byte, item MetaItem) error {
			name := strings.TrimPrefix(string(rawKey), "bucket:")
			buckets = append(buckets, name)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(buckets)
	return buckets, nil
}

// --- Object operations ---
