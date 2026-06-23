package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/google/uuid"
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
	} else if err := b.store.View(func(txn MetadataTxn) error {
		// off/pending: the existing FSM obj: prefix scan, verbatim.
		prefix := b.ks().Prefix([]byte("obj:" + bucket + "/"))
		it := txn.NewIterator(MetaIteratorOptions{PrefetchValues: true})
		defer it.Close()
		it.Seek(prefix)
		if it.ValidForPrefix(prefix) {
			return storage.ErrBucketNotEmpty
		}
		return nil
	}); err != nil {
		return err
	}

	if err := os.RemoveAll(b.bucketDir(bucket)); err != nil {
		return fmt.Errorf("remove bucket dir: %w", err)
	}

	return b.propose(ctx, CmdDeleteBucket, DeleteBucketCmd{Bucket: bucket})
}

// ForceDeleteBucket deletes all objects in the bucket and then removes it.
// Unlike DeleteBucket, it does not fail when the bucket is non-empty.
//
// Scans all obj:<bucket>/ keys directly (not via WalkObjects) so that older
// versions of multi-version objects are collected too. WalkObjects only returns
// the latest version per key; skipping older versions would leave their Badger
// keys behind, causing DeleteBucket to still see them and return ErrBucketNotEmpty.
func (b *DistributedBackend) ForceDeleteBucket(ctx context.Context, bucket string) error {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	// Under soleauth=on the per-version blob tree + carve-out FSM are the SOLE
	// AUTHORITY: enumerate the deletion set from there (NOT the stale FSM obj:
	// scan), purge blobs, and hard-delete carve-outs. Branch once at the top so
	// off/pending keep the FSM-scan + two-pass forceDeleteObject verbatim (and
	// never read soleauth per-object). This is the single-DistributedBackend
	// (single-node / direct) path; the cluster path is ClusterCoordinator.
	if on, serr := b.soleAuthReadOn(bucket); serr != nil {
		return serr // fail closed
	} else if on {
		return b.forceDeleteBucketSoleAuth(ctx, bucket)
	}
	// Collect all obj: refs first so the Badger View is closed before any
	// Raft propose. Calling propose inside db.View holds the MVCC snapshot for
	// N×RTT and blocks Badger GC.
	type objRef struct {
		key       string
		versionID string // empty for legacy unversioned keys
	}
	var refs []objRef
	if err := b.store.View(func(txn MetadataTxn) error {
		// Build latMap so we can distinguish versioned sub-keys from unversioned
		// legacy keys. A key of the form obj:<bucket>/<base>/<vid> is a versioned
		// object iff <base> appears in latMap (i.e. lat:<bucket>/<base> exists)
		// AND <vid> is a valid UUID (all version IDs are UUID v4/v7). The UUID
		// check prevents misclassifying a legacy key like "a/b" as key="a"
		// versionID="b" when a versioned key "a" happens to share its prefix.
		latMap := make(map[string]struct{})
		rawLatPfx := []byte("lat:" + bucket + "/")
		latPfx := b.ks().Prefix(rawLatPfx)
		itLat := txn.NewIterator(MetaIteratorOptions{PrefetchValues: true})
		for itLat.Seek(latPfx); itLat.ValidForPrefix(latPfx); itLat.Next() {
			rawK := b.ks().MustStrip(itLat.Item().Key())
			latMap[string(rawK[len(rawLatPfx):])] = struct{}{}
		}
		itLat.Close()

		rawBucketPfx := []byte("obj:" + bucket + "/")
		pfx := b.ks().Prefix(rawBucketPfx)
		it := txn.NewIterator(MetaIteratorOptions{PrefetchValues: true})
		defer it.Close()
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			rawK := b.ks().MustStrip(it.Item().Key())
			rest := string(rawK[len(rawBucketPfx):])
			key, versionID := rest, ""
			if slash := strings.LastIndex(rest, "/"); slash >= 0 {
				candidateBase := rest[:slash]
				candidateVID := rest[slash+1:]
				if _, inLat := latMap[candidateBase]; inLat {
					if _, err := uuid.Parse(candidateVID); err == nil {
						key, versionID = candidateBase, candidateVID
					}
				}
			}
			refs = append(refs, objRef{key: key, versionID: versionID})
		}
		return nil
	}); err != nil {
		return fmt.Errorf("force delete: scan objects: %w", err)
	}
	// Two-pass deletion to prevent ring refcount double-decRef:
	//
	// Pass 1 — versioned refs first. applyDeleteObjectVersion calls decRef(rv)
	// for each version's ring. When the last versioned ref for a key is removed,
	// applyDeleteObjectVersion also deletes the unversioned ObjectMetaKey, so
	// Pass 2 finds it absent and skips decRef.
	//
	// Pass 2 — unversioned refs. applyDeleteObject("") only calls decRef if
	// ObjectMetaKey still exists. If Pass 1 already removed it, rv stays 0 and
	// decRef is not called, preventing a double-decRef of the ring refcount.
	for _, ref := range refs {
		if ref.versionID == "" {
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := b.forceDeleteObject(ctx, bucket, ref.key, ref.versionID); err != nil {
			return fmt.Errorf("force delete: %q: %w", ref.key, err)
		}
	}
	for _, ref := range refs {
		if ref.versionID != "" {
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := b.forceDeleteObject(ctx, bucket, ref.key, ref.versionID); err != nil {
			return fmt.Errorf("force delete: %q: %w", ref.key, err)
		}
	}
	return b.DeleteBucket(ctx, bucket)
}

// forceDeleteObject hard-deletes one Badger record for a single object without
// creating a tombstone. Used only by ForceDeleteBucket.
//
// For versioned objects (versionID != ""): removes the versioned obj: key via
// CmdDeleteObjectVersion. applyDeleteObjectVersion promotes the next-oldest
// version to latest, or removes lat:/legacy obj: keys when the last version is
// gone — so the final CmdDeleteObjectVersion call on each key leaves no traces.
// For legacy unversioned objects (versionID == ""): CmdDeleteObject with empty
// VersionID hard-deletes the unversioned obj: key (no tombstone written).
func (b *DistributedBackend) forceDeleteObject(ctx context.Context, bucket, key, versionID string) error {
	if versionID != "" {
		_ = os.Remove(b.objectPathV(bucket, key, versionID))
		return b.propose(ctx, CmdDeleteObjectVersion, DeleteObjectVersionCmd{
			Bucket:    bucket,
			Key:       key,
			VersionID: versionID,
		})
	}
	// Legacy unversioned key: hard-delete, no tombstone.
	_ = os.Remove(b.objectPath(bucket, key))
	return b.propose(ctx, CmdDeleteObject, DeleteObjectCmd{
		Bucket:    bucket,
		Key:       key,
		VersionID: "", // empty = legacy hard delete, no tombstone
	})
}

// forceDeleteBucketSoleAuth is the soleauth=on leaf ForceDeleteBucket path
// (single-node / direct backend). It enumerates the authoritative deletion set —
// per-version blobs (incl. delete markers, fail-closed cluster-wide) + this
// node's local carve-out FSM records (appendable/coalesced/legacy-bare) — then
// removes each: vid-bearing entries via DeleteObjectVersion (S2a dual-delete
// purges the per-version blob); legacy-bare (VersionID=="") via the hard-delete
// path. Non-carve-out vid-bearing FSM records are NON-authoritative under `on`
// and intentionally left as orphans (the orphan scrubber GCs them; they do not
// count toward emptiness). Finishes with the blob-aware DeleteBucket.
func (b *DistributedBackend) forceDeleteBucketSoleAuth(ctx context.Context, bucket string) error {
	type objRef struct {
		key       string
		versionID string
	}
	var refs []objRef
	seen := map[[2]string]bool{}
	add := func(key, vid string) {
		k := [2]string{key, vid}
		if !seen[k] {
			seen[k] = true
			refs = append(refs, objRef{key: key, versionID: vid})
		}
	}
	// Versioned blobs (every version, incl. delete markers), fail-closed.
	cmds, err := b.scanQuorumMetaVersionsClusterAll(bucket, "")
	if err != nil {
		return fmt.Errorf("force delete (soleauth): enumerate blobs: %w", err)
	}
	for _, c := range cmds {
		add(c.Key, c.VersionID)
	}
	// Local carve-out records (blob-wins collisions already excluded via `seen`).
	carve, err := b.scanFsmCarveoutVersions(bucket, "", seen)
	if err != nil {
		return fmt.Errorf("force delete (soleauth): enumerate carve-out: %w", err)
	}
	for _, v := range carve {
		add(v.Key, v.VersionID)
	}
	for _, ref := range refs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if ref.versionID != "" {
			if derr := b.DeleteObjectVersion(bucket, ref.key, ref.versionID); derr != nil {
				return fmt.Errorf("force delete (soleauth): delete %q@%s: %w", ref.key, ref.versionID, derr)
			}
			continue
		}
		if derr := b.HardDeleteLegacyObject(ctx, bucket, ref.key); derr != nil {
			return fmt.Errorf("force delete (soleauth): hard delete %q: %w", ref.key, derr)
		}
	}
	return b.DeleteBucket(ctx, bucket)
}

// HardDeleteLegacyObject hard-deletes a legacy unversioned bare obj:{bucket}/{key}
// record (CmdDeleteObject with VersionID="", apply.go hard-delete path) — NO
// tombstone. Used ONLY by the soleauth=on force-delete path (leaf + the
// coordinator's all-groups fan-out) to remove a legacy-bare carve-out; off-path
// force-delete uses forceDeleteObject's legacy branch directly. Idempotent: a
// no-op when the bare record is absent.
func (b *DistributedBackend) HardDeleteLegacyObject(ctx context.Context, bucket, key string) error {
	_ = os.Remove(b.objectPath(bucket, key))
	return b.propose(ctx, CmdDeleteObject, DeleteObjectCmd{Bucket: bucket, Key: key, VersionID: ""})
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
// blob RMW (sole authority — no raft path). HeadObject pre-check guarantees
// existence; a blob miss here is a real not-found or a race.
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
// quorum-meta blob RMW (sole authority — no raft path). VersionID="" targets
// the current version; VersionID!="" targets a specific version. Passing nil
// tags clears the tag set. Does not modify ETag, LastModified, ACL, or blob bytes.
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
