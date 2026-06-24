package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

func (b *DistributedBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	return b.deleteObjectCtx(ctx, bucket, key)
}

// DeleteObjectReturningMarker satisfies server.VersionedSoftDeleter. Same
// tombstone semantics as DeleteObject but returns the delete marker's
// VersionID so the S3 handler can surface it in the response header.
func (b *DistributedBackend) DeleteObjectReturningMarker(bucket, key string) (string, error) {
	return b.deleteObjectWithMarker(context.Background(), bucket, key)
}

// deleteObjectCtx is the ctx-aware core used by DeleteObject so that HTTP
// request cancellation propagates into the Raft propose call.
func (b *DistributedBackend) deleteObjectCtx(ctx context.Context, bucket, key string) error {
	_, err := b.deleteObjectWithMarker(ctx, bucket, key)
	return err
}

// deleteObjectWithMarker is the single implementation shared by DeleteObject
// and DeleteObjectReturningMarker.
//
// Tombstone semantics: creates a delete marker as a new version. Prior version
// data remains addressable via GetObjectVersion and is NOT physically removed
// here. Hard-delete of a specific version goes through DeleteObjectVersion
// (used by lifecycle/scrubber).
//
// For backward compatibility with the legacy N× on-disk layout, we also
// remove the unversioned local object file if present — it's guaranteed to
// be stale (superseded by a versioned path) and keeping it risks GetObject
// serving it as a fallback read.
func (b *DistributedBackend) deleteObjectWithMarker(ctx context.Context, bucket, key string) (string, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return "", err
	}
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return "", err
	}
	os.Remove(b.objectPath(bucket, key))
	markerID := newVersionID()

	// Blob-primary (raft-free): if the object exists as a versioned object — i.e. it
	// has per-version blobs — the delete marker is a durable per-version blob
	// (IsDeleteMarker) with NO raft propose, reusing the object's placement. Reads,
	// LIST, and latest-derive all treat the max-VID marker blob as "deleted".
	//
	// The decision is made from the object's ACTUAL storage (a cluster-wide
	// per-version read), NOT the bucket's versioning meta-state: the delete leaf may
	// not be the meta authority (a data-group node) and would misread versioning as
	// Unversioned, falling to the legacy FSM path — leaving the marker invisible to
	// the blob-primary readers (a 404 instead of 405). A key with no per-version
	// blobs (non-versioned object, or a key that never existed) falls through to the
	// legacy path below.
	if b.shardSvc != nil {
		// Decode-strict (fail-closed) read, mirroring the hard-delete path: a read
		// ERROR must NOT fall through to the legacy FSM propose (which would write a
		// blob-invisible marker for an actually-versioned object), so surface it. A
		// genuine empty result (no per-version blobs anywhere reachable) is the
		// non-versioned / never-existed case → legacy path below.
		cmds, verr := b.readQuorumMetaVersionsDecodeStrict(bucket, key)
		if verr != nil {
			return "", fmt.Errorf("resolve per-version blobs for delete marker %s/%s: %w", bucket, key, verr)
		}
		if len(cmds) > 0 {
			placement := cmds[0]
			for i := range cmds {
				if cmds[i].VersionID > placement.VersionID {
					placement = cmds[i]
				}
			}
			if len(placement.NodeIDs) > 0 {
				marker := PutObjectMetaCmd{
					Bucket:           bucket,
					Key:              key,
					VersionID:        markerID,
					ModTime:          time.Now().Unix(),
					IsDeleteMarker:   true,
					ECData:           placement.ECData,
					ECParity:         placement.ECParity,
					NodeIDs:          placement.NodeIDs,
					PlacementGroupID: placement.PlacementGroupID,
				}
				blob, eerr := encodeQuorumMetaBlob(marker)
				if eerr != nil {
					return "", fmt.Errorf("encode delete marker %s/%s: %w", bucket, key, eerr)
				}
				if werr := b.fanOutPerVersionBlob(ctx, marker, blob); werr != nil {
					return "", fmt.Errorf("write delete marker %s/%s: %w", bucket, key, werr)
				}
				return markerID, nil
			}
		}
	}

	// Non-versioned / never-existed key: blob-authoritative delete, RAFT-FREE. A
	// non-versioned PUT writes ONLY the latest-only blob (writeQuorumMeta, no FSM
	// obj: propose), so there is no FSM record to delete — the old CmdDeleteObject
	// propose was vestigial. Instead, overwrite the latest-only key with an
	// IsDeleteMarker tombstone, FAIL-CLOSED, reusing the existing object's
	// placement. The write is LWW-guarded (fresh ModTime) so it converges against a
	// concurrent PUT; read/LIST fold the marker → 404/absent; the overwritten data
	// blob's segments become orphan-eligible for the segment GC. A never-existed (or
	// already-deleted) key has no live blob → no-op success (S3 delete-of-absent).
	if b.shardSvc != nil {
		existing, qerr := b.readQuorumMetaCmd(bucket, key)
		switch {
		case qerr == nil && len(existing.NodeIDs) > 0:
			if werr := b.writeQuorumMeta(ctx, PutObjectMetaCmd{
				Bucket:           bucket,
				Key:              key,
				VersionID:        markerID,
				ModTime:          time.Now().Unix(),
				IsDeleteMarker:   true,
				ECData:           existing.ECData,
				ECParity:         existing.ECParity,
				NodeIDs:          existing.NodeIDs,
				PlacementGroupID: existing.PlacementGroupID,
			}); werr != nil {
				return "", fmt.Errorf("write non-versioned delete tombstone %s/%s: %w", bucket, key, werr)
			}
		case qerr != nil && !errors.Is(qerr, storage.ErrObjectNotFound):
			// Fail closed on a read error — do NOT report a successful delete when we
			// could not establish the object's state.
			return "", fmt.Errorf("resolve non-versioned object for delete %s/%s: %w", bucket, key, qerr)
		}
		// ErrObjectNotFound (never-existed / already-deleted) → no-op success.
	}
	return markerID, nil
}
