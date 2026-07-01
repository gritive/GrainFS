package cluster

import (
	"context"
	"fmt"
	"sort"

	"github.com/gritive/GrainFS/internal/storage"
)

func (c *ClusterCoordinator) CreateBucket(ctx context.Context, bucket string) error {
	return c.base.CreateBucket(ctx, bucket)
}

// CreateBucketBypassReserved seeds a reserved bucket via the meta-Raft, bypassing
// the reserved-name guard. Called only during bootstrap to create "default" and "_grainfs".
func (c *ClusterCoordinator) CreateBucketBypassReserved(ctx context.Context, bucket string) error {
	type bypassSeeder interface {
		CreateBucketBypassReserved(ctx context.Context, bucket string) error
	}
	if s, ok := c.base.(bypassSeeder); ok {
		return s.CreateBucketBypassReserved(ctx, bucket)
	}
	return c.base.CreateBucket(ctx, bucket)
}

func (c *ClusterCoordinator) HeadBucket(ctx context.Context, bucket string) error {
	err := c.base.HeadBucket(ctx, bucket)
	if err == nil {
		return nil
	}
	if c.bucketAssigned(bucket) {
		return nil
	}
	return err
}
func (c *ClusterCoordinator) DeleteBucket(ctx context.Context, bucket string) error {
	if c.router != nil && c.meta != nil {
		// Under blob-authoritative, ListObjects (latest-live-only, delete markers
		// excluded) is not the authority; use ListObjectVersions, which under `on`
		// returns the cluster-wide per-version blobs (incl. delete markers) + every
		// group's carve-out FSM records. len>0 (any version/marker/carve-out) =
		// non-empty, matching the leaf. Fail-closed on the blob-authority read.
		if on, serr := c.bucketBlobAuthOn(bucket); serr != nil {
			return serr
		} else if on {
			vs, err := c.ListObjectVersions(ctx, bucket, "", 1)
			if err != nil {
				return err
			}
			if len(vs) > 0 {
				return storage.ErrBucketNotEmpty
			}
		} else {
			objects, err := c.ListObjects(ctx, bucket, "", 1)
			if err != nil {
				return err
			}
			if len(objects) > 0 {
				return storage.ErrBucketNotEmpty
			}
		}
	}
	return c.base.DeleteBucket(ctx, bucket)
}

// ForceDeleteBucket deletes all objects in the bucket and then removes it.
// Unlike DeleteBucket, it does not fail when the bucket is non-empty.
//
// Branches on bucket versioning when routing is wired:
//   - versioned (blob-authoritative): forceDeleteBucketBlobAuth enumerates per-version
//     blobs cluster-wide and purges each via DeleteObjectVersion.
//   - non-versioned: enumerate live latest-only qmeta blobs cluster-wide via
//     scanQuorumMetaClusterAll (NOT the local-only scanQuorumMetaBucketStrict), then
//     for each object purge shards fail-closed FIRST, then the qmeta blob.
func (c *ClusterCoordinator) ForceDeleteBucket(ctx context.Context, bucket string) error {
	if c.router == nil || c.meta == nil {
		// Routing not wired — delegate to the leaf backend which handles its own branches.
		return c.base.ForceDeleteBucket(ctx, bucket)
	}
	on, serr := c.bucketBlobAuthOn(bucket)
	if serr != nil {
		return serr
	}
	if on {
		return c.forceDeleteBucketBlobAuth(ctx, bucket)
	}
	// Non-versioned cluster path: enumerate cluster-wide (fail-closed fan-out to
	// all peers) so objects placed on remote nodes are included. Then per object:
	// shards first (fail-closed), then qmeta blob.
	b, ok := c.base.(*DistributedBackend)
	if !ok || b == nil {
		return c.base.ForceDeleteBucket(ctx, bucket)
	}
	cmds, err := b.scanQuorumMetaClusterAll(bucket)
	if err != nil {
		return fmt.Errorf("force delete cluster: enumerate qmeta: %w", err)
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
			return fmt.Errorf("force delete cluster: object %q has empty placement (corrupt qmeta blob) — aborting to avoid stranding shards", cmd.Key)
		}
		// Shards FIRST (fail-closed): abort if shards cannot be confirmed gone.
		shardKey := ecObjectShardKey(cmd.Key, "")
		if serr := b.deleteShardsQuorum(ctx, bucket, shardKey, cmd.NodeIDs); serr != nil {
			return fmt.Errorf("force delete cluster: delete shards for %q: %w", cmd.Key, serr)
		}
		// Only after shards confirmed gone: remove the qmeta placement blob.
		if qerr := b.deleteQuorumMetaQuorum(ctx, bucket, cmd.Key, cmd.NodeIDs); qerr != nil {
			return fmt.Errorf("force delete cluster: delete qmeta for %q: %w", cmd.Key, qerr)
		}
	}
	// A Suspended bucket also keeps versions preserved from a prior Enabled era in
	// the per-version tree; purge them too (no-op for a never-versioned bucket) so
	// the final DeleteBucket's per-version emptiness check passes.
	if err := c.purgePerVersionBlobs(ctx, bucket); err != nil {
		return err
	}
	return c.DeleteBucket(ctx, bucket)
}

// forceDeleteBucketBlobAuth is the blob-authoritative (versioned) cluster force-delete:
// enumerate per-version blobs cluster-wide via ListObjectVersions, delete each
// vid-bearing entry via DeleteObjectVersion (blob-purging), then finish with the
// blob-aware DeleteBucket. Object metadata lives only in the off-raft quorum-meta
// blob, so the per-version blob enumeration is exhaustive (no FSM tail).
func (c *ClusterCoordinator) forceDeleteBucketBlobAuth(ctx context.Context, bucket string) error {
	if err := c.purgePerVersionBlobs(ctx, bucket); err != nil {
		return err
	}
	return c.DeleteBucket(ctx, bucket)
}

// purgePerVersionBlobs deletes every per-version blob (cluster-wide, incl. delete
// markers) via DeleteObjectVersion (routed blob + shard purging), fail-closed. No-op
// for a bucket whose per-version tree is empty (never-versioned). Shared by the
// Enabled force path and the non-versioned / Suspended force path so a Suspended
// bucket's versions preserved from a prior Enabled era are purged, not orphaned.
//
// Enumeration uses the RAW per-version blob scan (scanQuorumMetaVersionsClusterAll),
// the SAME source DeleteBucket's emptiness check reads — NOT ListObjectVersions,
// which is versioning-state-gated and returns nothing for a Suspended bucket, so
// purge and recheck must agree or a Suspended bucket would survive the purge.
func (c *ClusterCoordinator) purgePerVersionBlobs(ctx context.Context, bucket string) error {
	b, ok := c.base.(*DistributedBackend)
	if !ok || b == nil {
		// Non-DistributedBackend base: fall back to the version-listing enumeration.
		versions, lerr := c.ListObjectVersions(ctx, bucket, "", 0)
		if lerr != nil {
			return fmt.Errorf("force delete: enumerate per-version blobs: %w", lerr)
		}
		for _, v := range versions {
			if v == nil || v.VersionID == "" {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if derr := c.DeleteObjectVersion(bucket, v.Key, v.VersionID); derr != nil {
				return fmt.Errorf("force delete: delete %q@%s: %w", v.Key, v.VersionID, derr)
			}
		}
		return nil
	}
	cmds, err := b.scanQuorumMetaVersionsClusterAll(bucket, "")
	if err != nil {
		return fmt.Errorf("force delete: enumerate per-version blobs: %w", err)
	}
	for _, cmd := range cmds {
		if cmd.VersionID == "" {
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if derr := c.DeleteObjectVersion(bucket, cmd.Key, cmd.VersionID); derr != nil {
			return fmt.Errorf("force delete: delete %q@%s: %w", cmd.Key, cmd.VersionID, derr)
		}
	}
	return nil
}

func (c *ClusterCoordinator) ListBuckets(ctx context.Context) ([]string, error) {
	buckets, err := c.base.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(buckets))
	for _, bucket := range buckets {
		seen[bucket] = struct{}{}
	}
	if src, ok := c.meta.(interface{ BucketAssignments() map[string]string }); ok {
		for bucket := range src.BucketAssignments() {
			seen[bucket] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for bucket := range seen {
		out = append(out, bucket)
	}
	sort.Strings(out)
	return out, nil
}

func (c *ClusterCoordinator) bucketAssigned(bucket string) bool {
	if c.router != nil {
		if _, ok := c.router.ExplicitGroup(bucket); ok {
			return true
		}
	}
	if src, ok := c.meta.(interface{ BucketAssignments() map[string]string }); ok {
		_, ok := src.BucketAssignments()[bucket]
		return ok
	}
	return false
}

func (c *ClusterCoordinator) SetBucketVersioning(bucket, state string) error {
	type proposer interface {
		SetBucketVersioningPropose(bucket, state string) error
	}
	type bucketVersioner interface {
		SetBucketVersioning(bucket, state string) error
	}
	// Cluster-aware pre-check: the base layer's local-only pre-check would
	// reject the request with NoSuchBucket on a lagging follower where the
	// meta-Raft bucket record exists but the local FSM has not yet applied it.
	// The cluster-aware HeadBucket consults the meta-Raft directly.
	if err := c.HeadBucket(context.Background(), bucket); err != nil {
		return err
	}
	// Prefer the propose-only entrypoint when the base exposes it; that
	// skips the duplicate local HeadBucket pre-check inside
	// DistributedBackend.SetBucketVersioning, which would otherwise reject
	// the follower path we just allowed through the cluster-aware check.
	if p, ok := c.base.(proposer); ok {
		return p.SetBucketVersioningPropose(bucket, state)
	}
	v, ok := c.base.(bucketVersioner)
	if !ok {
		return ErrCoordinatorNoRouter
	}
	return v.SetBucketVersioning(bucket, state)
}

func (c *ClusterCoordinator) GetBucketVersioning(bucket string) (string, error) {
	type bucketVersioner interface {
		GetBucketVersioning(bucket string) (string, error)
	}
	v, ok := c.base.(bucketVersioner)
	if !ok {
		return "", ErrCoordinatorNoRouter
	}
	return v.GetBucketVersioning(bucket)
}

// GetBucketVersioningLinearized is the linearizing read used by the MUTATING S3
// edge (see DistributedBackend.GetBucketVersioningLinearized — it degrades to a
// local read during a group-0 leaderless window, not fail-closed). Falls back to
// the plain read for a base backend that doesn't support linearization.
func (c *ClusterCoordinator) GetBucketVersioningLinearized(ctx context.Context, bucket string) (string, error) {
	type linearizableVersioner interface {
		GetBucketVersioningLinearized(ctx context.Context, bucket string) (string, error)
	}
	if v, ok := c.base.(linearizableVersioner); ok {
		return v.GetBucketVersioningLinearized(ctx, bucket)
	}
	return c.GetBucketVersioning(bucket)
}

func (c *ClusterCoordinator) SetBucketPolicy(bucket string, policyJSON []byte) error {
	type proposer interface {
		SetBucketPolicyPropose(bucket string, policyJSON []byte) error
	}
	type policyBackend interface {
		SetBucketPolicy(bucket string, policyJSON []byte) error
	}
	if err := c.HeadBucket(context.Background(), bucket); err != nil {
		return err
	}
	if p, ok := c.base.(proposer); ok {
		return p.SetBucketPolicyPropose(bucket, policyJSON)
	}
	p, ok := c.base.(policyBackend)
	if !ok {
		return ErrCoordinatorNoRouter
	}
	return p.SetBucketPolicy(bucket, policyJSON)
}

func (c *ClusterCoordinator) GetBucketPolicy(bucket string) ([]byte, error) {
	type policyBackend interface {
		GetBucketPolicy(bucket string) ([]byte, error)
	}
	p, ok := c.base.(policyBackend)
	if !ok {
		return nil, ErrCoordinatorNoRouter
	}
	return p.GetBucketPolicy(bucket)
}

func (c *ClusterCoordinator) DeleteBucketPolicy(bucket string) error {
	type proposer interface {
		DeleteBucketPolicyPropose(bucket string) error
	}
	type policyBackend interface {
		DeleteBucketPolicy(bucket string) error
	}
	if err := c.HeadBucket(context.Background(), bucket); err != nil {
		return err
	}
	if p, ok := c.base.(proposer); ok {
		return p.DeleteBucketPolicyPropose(bucket)
	}
	p, ok := c.base.(policyBackend)
	if !ok {
		return ErrCoordinatorNoRouter
	}
	return p.DeleteBucketPolicy(bucket)
}
