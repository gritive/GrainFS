package cluster

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/storage"
)

// ErrStalePlacement signals the placement group changed between the
// coordinator's placement resolve and FSM apply (rebalance window). The
// coordinator (Task 21) performs transparent retry up to 2 times before
// returning 503 SlowDown to the client.
var ErrStalePlacement = errors.New("append: placement group changed mid-request")

// AppendObject implements storage.AppendObjecter for DistributedBackend.
// Owner-node entry point — ClusterCoordinator handles non-owner forwarding
// (Task 21).
//
// Phase A flow:
//  1. Cluster-aware pre-check via HeadObject (fast reject for offset / cap).
//  2. Write segment blob to owner-node disk.
//  3. Propose CmdAppendObject via data-Raft; b.propose surfaces apply errors
//     transparently (Phase A Tasks 14-16).
//  4. Re-HeadObject for fresh result reflecting committed segment list.
func (b *DistributedBackend) AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*storage.Object, error) {
	// Step 1: cluster-aware pre-check.
	existing, err := b.HeadObject(ctx, bucket, key)
	if err != nil && !errors.Is(err, storage.ErrObjectNotFound) {
		return nil, err
	}
	if existing == nil {
		if expectedOffset != 0 {
			return nil, storage.ErrAppendOffsetMismatch
		}
	} else {
		if !existing.IsAppendable {
			return nil, storage.ErrAppendNotSupported
		}
		if existing.Size != expectedOffset {
			return nil, storage.ErrAppendOffsetMismatch
		}
		if len(existing.Segments) >= storage.MaxAppendSegments {
			return nil, storage.ErrAppendCapExceeded
		}
	}

	// Step 2: write segment blob to owner-node disk.
	seg, err := b.writeSegmentBlobForAppend(bucket, key, r)
	if err != nil {
		return nil, fmt.Errorf("write segment blob: %w", err)
	}

	// Step 3: resolve placement group at propose time (cmd captures PG so the
	// FSM can reject if it has moved since — see apply.go ErrStalePlacement).
	pgID := b.lookupPlacementGroupForAppend(ctx, existing)

	// Step 4: propose via data-Raft. b.propose returns FSM apply error
	// transparently (Phase A).
	cmd := AppendObjectCmd{
		Bucket:           bucket,
		Key:              key,
		ExpectedOffset:   expectedOffset,
		BlobID:           seg.BlobID,
		SegmentSize:      seg.Size,
		SegmentETag:      seg.ETag,
		PlacementGroupID: pgID,
	}
	if err := b.propose(ctx, CmdAppendObject, cmd); err != nil {
		// Best-effort cleanup of orphan segment blob on apply rejection.
		_ = os.Remove(b.segmentBlobPath(bucket, key, seg.BlobID))
		return nil, err
	}

	// Step 5: re-HeadObject for fresh result.
	return b.HeadObject(ctx, bucket, key)
}

// writeSegmentBlobForAppend writes one segment blob to owner-node disk under
// <root>/data/<bucket>/<key>_segments/<blobID>. Mirrors LocalBackend.WriteSegmentBlob
// but uses the cluster backend's own root and (optional) shard-service encryptor.
func (b *DistributedBackend) writeSegmentBlobForAppend(bucket, key string, r io.Reader) (storage.SegmentRef, error) {
	blobID := uuid.Must(uuid.NewV7()).String()
	path := b.segmentBlobPath(bucket, key, blobID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return storage.SegmentRef{}, err
	}

	h := md5.New()
	f, err := os.Create(path)
	if err != nil {
		return storage.SegmentRef{}, err
	}
	tr := io.TeeReader(r, h)
	size, copyErr := io.Copy(f, tr)
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(path)
		return storage.SegmentRef{}, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(path)
		return storage.SegmentRef{}, closeErr
	}

	return storage.SegmentRef{
		BlobID: blobID,
		Size:   size,
		ETag:   hex.EncodeToString(h.Sum(nil)),
	}, nil
}

// segmentBlobPath returns the on-disk path for one append-segment blob.
func (b *DistributedBackend) segmentBlobPath(bucket, key, blobID string) string {
	return filepath.Join(b.objectPath(bucket, key)+"_segments", blobID)
}

// lookupPlacementGroupForAppend resolves the placement group ID to freeze into
// AppendObjectCmd. Order:
//  1. existing objectMeta's PG (anchors subsequent appends to the original PG).
//  2. PlacementGroupFromContext (coordinator-provided).
//  3. default "group-0" (single-node / test path).
func (b *DistributedBackend) lookupPlacementGroupForAppend(ctx context.Context, existing *storage.Object) string {
	// Phase A: storage.Object does not carry PlacementGroupID directly — the FSM
	// reads it from objectMeta at apply time. For the propose-time hint we fall
	// back to context / default; the FSM's stale-placement check still works
	// because applyAppendObjectFromCmd compares cmd.PlacementGroupID against
	// the freshly-read existing objectMeta.PlacementGroupID.
	if existing != nil {
		// existing was decoded from objectMeta; PG isn't exposed on storage.Object,
		// so fall through to context / default. (Task 21 coordinator threads PG
		// via context explicitly.)
		_ = existing
	}
	if pg, ok := PlacementGroupFromContext(ctx); ok {
		return pg
	}
	return "group-0"
}
