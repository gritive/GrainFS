package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// ErrRelocateSkipped marks a relocation that became a no-op because the object
// changed (ETag/VersionID drift, delete marker, already redundant) or the
// cluster no longer offers redundant placement capacity. Callers treat it as a
// benign skip, not a failure.
var ErrRelocateSkipped = errors.New("relocate: object changed or no longer a candidate")

// relocateInput identifies the LATEST version of an object to relocate into a
// redundant placement group. VersionID/ExpectedETag pin the exact record so a
// concurrent overwrite causes a skip rather than corrupting a fresher object.
//
//nolint:unused // wired up by the orphan/redundancy-upgrade sweep in a later task.
type relocateInput struct {
	Bucket, Key, VersionID, ExpectedETag string
}

// relocationStillEligible is the pure eligibility guard. An object qualifies for
// redundancy upgrade iff it is a non-redundant EC object (DataShards >= 1,
// ParityShards == 0 — the single-node write fingerprint), it is not a delete
// marker, its ETag and VersionID still match the pinned candidate, and the
// cluster currently has redundant placement capacity. Any mismatch returns
// ErrRelocateSkipped.
//
//nolint:unused // wired up by the orphan/redundancy-upgrade sweep in a later task.
func relocationStillEligible(cur PutObjectMetaCmd, in relocateInput, clusterRedundant bool) error {
	switch {
	case !clusterRedundant:
		return fmt.Errorf("%w: cluster not redundant", ErrRelocateSkipped)
	case cur.IsDeleteMarker:
		return fmt.Errorf("%w: delete marker", ErrRelocateSkipped)
	case cur.ECParity != 0:
		return fmt.Errorf("%w: already redundant (parity=%d)", ErrRelocateSkipped, cur.ECParity)
	case cur.ECData < 1:
		return fmt.Errorf("%w: no data shards", ErrRelocateSkipped)
	case cur.ETag != in.ExpectedETag:
		return fmt.Errorf("%w: etag drift", ErrRelocateSkipped)
	case cur.VersionID != in.VersionID:
		return fmt.Errorf("%w: version drift", ErrRelocateSkipped)
	default:
		return nil
	}
}

// relocateObjectToRedundantGroup re-encodes the LATEST record of (bucket,key) —
// a non-redundant 1+0 object written while the cluster was single-node — into a
// redundant wide EC group, preserving ALL identity (versionID/ETag/size/
// contentType/userMetadata/tags/ACL/LastModified) and atomically swapping the
// placement via a CAS-honoring FSM propose. OLD shards are never deleted here; a
// separate orphan-segment sweep reclaims them. LATEST-VERSION-ONLY.
//
//nolint:unused // wired up by the orphan/redundancy-upgrade sweep in a later task.
func (b *DistributedBackend) relocateObjectToRedundantGroup(ctx context.Context, in relocateInput) error {
	cur, err := b.readQuorumMetaCmd(in.Bucket, in.Key)
	if err != nil {
		return err
	}

	clusterRedundant := clusterHasRedundantCapacity(b.shardGroup.ShardGroups(), b.currentECConfig(), metaNodeCount(b.shardGroup))
	if err := relocationStillEligible(cur, in, clusterRedundant); err != nil {
		return err
	}

	rc, obj, err := b.GetObject(ctx, in.Bucket, in.Key)
	if err != nil {
		return err
	}
	defer rc.Close()

	// Pre-allocate blobIDs + placements sized to the exact segment count, mirroring
	// putObjectChunked. A 0-byte object still gets one (empty) segment.
	chunkSize := int64(b.effectiveChunkedPutChunkSize())
	numSegments := int((obj.Size + chunkSize - 1) / chunkSize)
	if numSegments < 1 {
		numSegments = 1
	}
	blobIDs := make([]string, numSegments)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := &clusterSegmentBackend{
		b:            b,
		bucket:       in.Bucket,
		key:          in.Key,
		versionID:    in.VersionID,
		blobIDs:      blobIDs,
		contentType:  obj.ContentType,
		userMetadata: obj.UserMetadata,
		sseAlgorithm: obj.SSEAlgorithm,
		acl:          cur.ACL,
		placements:   make([]segmentPlacement, numSegments),
		chunkSize:    int(chunkSize),
	}

	// The override does ONLY the authoritative CAS-honoring propose (NOT the
	// quorum-meta mirror). If propose fails (incl. CAS reject), runChunkedPut keeps
	// committed=false so its cleanup defer deletes the NEW blobs — safe, because the
	// FSM is unchanged and the OLD blobs are untouched. If propose succeeds, the
	// override returns nil → committed=true → NEW blobs are retained (the committed
	// FSM record now references them). The quorum-meta mirror is deferred to AFTER
	// runChunkedPut returns success (below) so a mirror failure can never trip the
	// cleanup that would delete FSM-referenced new blobs (phantom-commit safety).
	var committedCmd *PutObjectMetaCmd
	csb.writeQuorumMetaFn = func(ctx context.Context, cmd PutObjectMetaCmd) error {
		cmd.MetaSeq = cur.MetaSeq + 1      // strictly win the (ModTime,VersionID) LWW tie
		cmd.ExpectedETag = in.ExpectedETag // CAS guard (also passed via runChunkedPut arg)
		if err := b.propose(ctx, CmdPutObjectMeta, cmd); err != nil {
			return err
		}
		stamped := cmd
		committedCmd = &stamped
		return nil
	}

	_, err = runChunkedPut(ctx, csb, rc, in.Bucket, in.Key, in.VersionID, obj.ContentType,
		obj.UserMetadata, obj.SSEAlgorithm, cur.ModTime, true /*preserveModTime*/, in.ExpectedETag, nil, nil, obj.Tags)
	if err != nil {
		if errors.Is(err, ErrPutObjectMetaCAS) {
			// Object changed under us; the new blobs were already cleaned by the
			// runChunkedPut defer. Benign skip — the sweep re-detects if still 1+0.
			return fmt.Errorf("%w: %v", ErrRelocateSkipped, err)
		}
		return fmt.Errorf("relocate %s/%s: %w", in.Bucket, in.Key, err)
	}

	// Mirror the committed record into quorum-meta (LIST-visibility / read fallback)
	// AFTER the FSM commit succeeded. Best-effort: the object is already durable via
	// the FSM, so a mirror failure must NOT fail the op or delete the committed
	// blobs. The next sweep cycle re-detects (quorum-meta may still show parity 0)
	// and retries idempotently.
	if committedCmd != nil {
		if perr := b.writeQuorumMeta(ctx, *committedCmd); perr != nil {
			log.Warn().Err(perr).Str("bucket", in.Bucket).Str("key", in.Key).Str("version_id", in.VersionID).
				Msg("relocate: quorum-meta mirror failed (object committed via FSM; LIST visibility deferred until next sweep)")
		}
	}

	return nil
}
