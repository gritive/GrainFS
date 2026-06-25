package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/uuidutil"
)

// ErrRelocateSkipped marks a relocation that became a no-op because the object
// changed (ETag/VersionID drift, delete marker, already redundant) or the
// cluster no longer offers redundant placement capacity. Callers treat it as a
// benign skip, not a failure.
var ErrRelocateSkipped = errors.New("relocate: object changed or no longer a candidate")

// relocateInput identifies the LATEST version of an object to relocate into a
// redundant placement group. VersionID/ExpectedETag pin the exact record so a
// concurrent overwrite causes a skip rather than corrupting a fresher object.
type relocateInput struct {
	Bucket, Key, VersionID, ExpectedETag string
}

// relocationStillEligible is the pure eligibility guard. An object qualifies for
// redundancy upgrade iff it is a non-redundant EC object (DataShards >= 1,
// ParityShards == 0 — the single-node write fingerprint), it is not a delete
// marker, its ETag and VersionID still match the pinned candidate, and the
// cluster currently has redundant placement capacity. Any mismatch returns
// ErrRelocateSkipped.
func relocationStillEligible(cur PutObjectMetaCmd, in relocateInput, clusterRedundant bool) error {
	switch {
	case !clusterRedundant:
		return fmt.Errorf("%w: cluster not redundant", ErrRelocateSkipped)
	case cur.IsDeleteMarker:
		return fmt.Errorf("%w: delete marker", ErrRelocateSkipped)
	case cur.IsAppendable || len(cur.Coalesced) > 0:
		// Relocation re-encodes the body via runChunkedPut, which writes a plain
		// chunked manifest with no IsAppendable / Coalesced / AppendCallMD5s. Relocating
		// an appendable/coalesced object would therefore drop its append-call digest
		// history (resetting the composite ETag on the next append) and its coalesced
		// refs. Skip it — it stays 1+0 (no redundancy upgrade) rather than be corrupted.
		// Redundancy upgrade for appendable/coalesced objects is a separate feature.
		return fmt.Errorf("%w: appendable/coalesced object (manifest shape not preserved by chunked re-encode)", ErrRelocateSkipped)
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
func (b *DistributedBackend) relocateObjectToRedundantGroup(ctx context.Context, in relocateInput) error {
	unlock := b.objectMetaRMWLock(in.Bucket, in.Key)
	defer unlock()

	cur, err := b.readQuorumMetaCmd(in.Bucket, in.Key)
	if err != nil {
		return err
	}

	// Cross-generation read correctness note: the relocation writes the NEW record
	// to a different placement group and never overwrites the OLD nodes' quorum-meta
	// files, so reads must consult the cross-generation LWW merge (multiGeneration)
	// to surface the higher-MetaSeq winner. That merge is armed exactly when there is
	// >1 placement generation — which is implied here: redundant capacity requires a
	// grown (1→N) topology, and growth records a new placement generation. So a
	// relocation can only run once the read path is already multi-generation.
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
		blobIDs[i] = uuidutil.MustNewV7()
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
		sizeHint:     obj.Size,
	}

	// The override does the authoritative meta commit via the SAME path a normal
	// chunked PUT uses (b.writeQuorumMeta): quorum-meta fan-out for user buckets
	// (and the per-version blob for versioning-enabled buckets). The genesis
	// 1+0 object's authoritative metadata lives in quorum-meta — NOT the data-raft
	// FSM, which a chunked PUT never populates for user buckets — so a raw FSM
	// object-meta propose here would CAS against an absent FSM key ("key not
	// found") and the relocation would always fail. We stamp MetaSeq = cur+1 so the
	// re-write strictly wins the (ModTime,VersionID) LWW tie (preserved identity
	// keeps both equal), which is the relocation ordering mechanism documented on
	// quorumMetaBlobWins. The ETag/VersionID drift guard already ran above against a
	// freshly-read quorum-meta record, so the FSM-only ExpectedETag CAS is dropped
	// (it cannot apply to a quorum-meta-resident object); we clear ExpectedETag to
	// avoid the versioned-bucket sub-propose hitting the same absent-FSM-key CAS.
	//
	// Commit semantics are unchanged: writeQuorumMeta success → committed=true →
	// NEW blobs retained; failure → committed=false → runChunkedPut's cleanup defer
	// deletes the NEW blobs while the OLD blobs and the prior winning record are
	// untouched (the stale record still wins LWW until a later successful re-write).
	csb.writeQuorumMetaFn = func(ctx context.Context, cmd PutObjectMetaCmd) error {
		cmd.MetaSeq = cur.MetaSeq + 1 // strictly win the (ModTime,VersionID) LWW tie
		cmd.ExpectedETag = ""         // eligibility already enforced ETag/VersionID; FSM CAS N/A for quorum-meta objects
		return b.writeQuorumMeta(ctx, cmd)
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

	return nil
}
