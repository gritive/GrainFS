package cluster

import (
	"fmt"
	"io"

	"github.com/gritive/GrainFS/internal/storage"
)

type appendObjectAdmissionInput struct {
	Existing       *storage.Object
	ExpectedOffset int64
	ChunkSize      int64
	SizeCapBytes   int64
}

func planAppendObjectAdmission(in appendObjectAdmissionInput) error {
	if in.Existing == nil {
		if in.ExpectedOffset != 0 {
			return storage.ErrAppendOffsetMismatch
		}
		return nil
	}
	if in.Existing.Size != in.ExpectedOffset {
		return storage.ErrAppendOffsetMismatch
	}
	if len(in.Existing.Segments) >= storage.MaxAppendSegments {
		return storage.ErrAppendCapExceeded
	}
	if in.ChunkSize > 0 && in.SizeCapBytes > 0 && in.Existing.Size+in.ChunkSize > in.SizeCapBytes {
		return storage.ErrAppendObjectTooLarge
	}
	return nil
}

func appendChunkSize(r io.Reader) int64 {
	seek, ok := r.(io.Seeker)
	if !ok {
		return -1
	}
	cur, err := seek.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1
	}
	end, err := seek.Seek(0, io.SeekEnd)
	if err != nil {
		return -1
	}
	_, _ = seek.Seek(cur, io.SeekStart)
	return end - cur
}

// appendBlobRMWInput carries the inputs for one owner-side blob CAS append.
// base is the latest-only quorum-meta manifest read at the start of the RMW
// (absent → baseExists=false, a brand-new appendable object).
type appendBlobRMWInput struct {
	Bucket           string
	Key              string
	ExpectedOffset   int64
	Segment          storage.SegmentRef
	PlacementGroupID string
	VersionID        string
	ModifiedUnixSec  int64
	Base             PutObjectMetaCmd
	BaseExists       bool
	SizeCapBytes     int64
	// NewObjectNodeIDs/ECData/ECParity are the manifest placement for the FIRST
	// append (baseExists=false). On a subsequent append the base manifest's own
	// placement is reused (an append never relocates the manifest).
	NewObjectNodeIDs  []string
	NewObjectECData   uint8
	NewObjectECParity uint8
	UseSideRecords    bool
	BaseSummary       storage.AppendSummary
	BaseHasSummary    bool
}

// planAppendObjectBlobRMWWithSide validates an append against the base manifest and
// builds the next-generation PutObjectMetaCmd (MetaSeqCAS, MetaSeq=base+1).
// It lifts the offset/segment-cap/size-cap/placement/composite-ETag/ModTime
// checks that previously lived in the FSM apply (applyAppendObjectTransition)
// into the owner-side RMW. The returned cmd is ready for writeQuorumMeta.
//
// The offset check (offset == base.Size) is the primary client-retry dedup:
// BlobID is a fresh UUIDv7 per call (not content-addressed), so a retried
// append at a stale offset correctly fails the offset check rather than being
// silently deduped.
func planAppendObjectBlobRMWWithSide(in appendBlobRMWInput) (PutObjectMetaCmd, storage.AppendSummary, bool, error) {
	seg := in.Segment
	useSide := in.UseSideRecords && len(in.Base.Coalesced) == 0

	if !in.BaseExists {
		if in.ExpectedOffset != 0 {
			return PutObjectMetaCmd{}, storage.AppendSummary{}, false, storage.ErrAppendOffsetMismatch
		}
		if useSide {
			state, count, err := storage.AppendETagStateAppend(nil, 0, seg.Checksum)
			if err != nil {
				return PutObjectMetaCmd{}, storage.AppendSummary{}, false, err
			}
			etag, err := storage.CompositeETagFromState(state, count)
			if err != nil {
				return PutObjectMetaCmd{}, storage.AppendSummary{}, false, err
			}
			cmd := PutObjectMetaCmd{
				Bucket:           in.Bucket,
				Key:              in.Key,
				Size:             seg.Size,
				ContentType:      "application/octet-stream",
				ETag:             etag,
				ModTime:          in.ModifiedUnixSec,
				VersionID:        in.VersionID,
				PlacementGroupID: in.PlacementGroupID,
				NodeIDs:          cloneStringSlice(in.NewObjectNodeIDs),
				ECData:           in.NewObjectECData,
				ECParity:         in.NewObjectECParity,
				IsAppendable:     true,
				MetaSeqCAS:       true,
				MetaSeq:          in.Base.MetaSeq + 1,
			}
			return cmd, storage.AppendSummary{Size: cmd.Size, SegmentCount: 1, ETagPartCount: count, ETagDigestState: state}, true, nil
		}
		return PutObjectMetaCmd{
			Bucket:           in.Bucket,
			Key:              in.Key,
			Size:             seg.Size,
			ContentType:      "application/octet-stream",
			ETag:             storage.CompositeETag([][]byte{seg.Checksum}),
			AppendCallMD5s:   [][]byte{append([]byte(nil), seg.Checksum...)},
			ModTime:          in.ModifiedUnixSec,
			VersionID:        in.VersionID,
			PlacementGroupID: in.PlacementGroupID,
			NodeIDs:          cloneStringSlice(in.NewObjectNodeIDs),
			ECData:           in.NewObjectECData,
			ECParity:         in.NewObjectECParity,
			Segments:         segmentRefsToMetaEntries([]storage.SegmentRef{seg}),
			IsAppendable:     true,
			MetaSeqCAS:       true,
			MetaSeq:          in.Base.MetaSeq + 1, // base absent → 0, so MetaSeq=1
		}, storage.AppendSummary{}, false, nil
	}

	base := in.Base
	// Placement must not have moved since the manifest was written (mirrors the
	// FSM stale-placement gate in applyAppendObjectTransition).
	if in.PlacementGroupID != "" && base.PlacementGroupID != "" &&
		in.PlacementGroupID != base.PlacementGroupID {
		return PutObjectMetaCmd{}, storage.AppendSummary{}, false, ErrStalePlacement
	}
	if base.Size != in.ExpectedOffset {
		return PutObjectMetaCmd{}, storage.AppendSummary{}, false, storage.ErrAppendOffsetMismatch
	}
	if useSide && in.BaseHasSummary {
		if in.BaseSummary.Size != base.Size {
			return PutObjectMetaCmd{}, storage.AppendSummary{}, false, fmt.Errorf("append side summary size %d does not match object size %d", in.BaseSummary.Size, base.Size)
		}
		if in.BaseSummary.SegmentCount >= storage.MaxAppendSegments {
			return PutObjectMetaCmd{}, storage.AppendSummary{}, false, storage.ErrAppendCapExceeded
		}
	} else if len(base.Segments) >= storage.MaxAppendSegments {
		return PutObjectMetaCmd{}, storage.AppendSummary{}, false, storage.ErrAppendCapExceeded
	}
	if in.SizeCapBytes > 0 && base.Size+seg.Size > in.SizeCapBytes {
		return PutObjectMetaCmd{}, storage.AppendSummary{}, false, storage.ErrAppendObjectTooLarge
	}

	next := base
	next.Bucket = in.Bucket
	next.Key = in.Key
	if next.Key == "" {
		next.Key = in.Key
	}
	if next.ContentType == "" {
		next.ContentType = "application/octet-stream"
	}
	// First append onto a plain (non-appendable) PUT: synthesize the base
	// coalesced ref from the existing manifest so the older bytes remain
	// readable (mirrors applyAppendObjectTransition). buildPutObjectMeta gives an
	// objectMeta view of the base cmd so appendBaseCoalescedRef sees the same
	// EC fields it did on the FSM path.
	if !base.IsAppendable {
		if len(base.Segments) == 0 && len(base.Coalesced) == 0 && base.Size > 0 {
			baseMeta := buildPutObjectMeta(base)
			next.Coalesced = []CoalescedShardRef{appendBaseCoalescedRef(in.Key, base.VersionID, &baseMeta)}
		}
	}
	next.IsAppendable = true

	if useSide && in.BaseHasSummary {
		state, count, err := storage.AppendETagStateAppend(in.BaseSummary.ETagDigestState, in.BaseSummary.ETagPartCount, seg.Checksum)
		if err != nil {
			return PutObjectMetaCmd{}, storage.AppendSummary{}, false, err
		}
		next.Segments = nil
		next.Size = base.Size + seg.Size
		next.AppendCallMD5s = nil
		next.ETag, err = storage.CompositeETagFromState(state, count)
		if err != nil {
			return PutObjectMetaCmd{}, storage.AppendSummary{}, false, err
		}
		next.ModTime = in.ModifiedUnixSec
		next.VersionID = in.VersionID
		next.PlacementGroupID = in.PlacementGroupID
		next.MetaSeqCAS = true
		next.MetaSeq = base.MetaSeq + 1
		next.IsDeleteMarker = false
		next.IsHardDeleted = false
		next.ExpectedETag = ""
		next.PreserveLatest = false
		return next, storage.AppendSummary{Size: next.Size, SegmentCount: in.BaseSummary.SegmentCount + 1, ETagPartCount: count, ETagDigestState: state}, true, nil
	}

	// Append the new segment and recompute Size.
	segs := append(segmentMetaEntriesToRefs(base.Segments), seg)
	next.Segments = segmentRefsToMetaEntries(segs)
	next.Size = base.Size + seg.Size
	// Accumulate the per-call digest history. base.Segments is emptied by a coalesce,
	// so deriving the ETag from Segments alone would drop the coalesced calls;
	// base.AppendCallMD5s carries the full history. When the base has no history yet
	// (first append onto a plain OR chunked PUT), SEED from the base's current segment
	// checksums so the composite ETag stays byte-identical to the pre-fix value for
	// that case. Then append this call's digest.
	callMD5s := make([][]byte, 0, len(base.AppendCallMD5s)+len(base.Segments)+1)
	if len(base.AppendCallMD5s) > 0 {
		for _, d := range base.AppendCallMD5s {
			callMD5s = append(callMD5s, append([]byte(nil), d...))
		}
	} else {
		for _, s := range base.Segments {
			if len(s.Checksum) > 0 {
				callMD5s = append(callMD5s, append([]byte(nil), s.Checksum...))
			}
		}
	}
	callMD5s = append(callMD5s, append([]byte(nil), seg.Checksum...))
	next.AppendCallMD5s = callMD5s
	next.ETag = storage.CompositeETag(callMD5s)
	next.ModTime = in.ModifiedUnixSec
	next.VersionID = in.VersionID
	next.PlacementGroupID = in.PlacementGroupID
	next.MetaSeqCAS = true
	next.MetaSeq = base.MetaSeq + 1
	// An append never carries forward a delete-marker / hard-delete state.
	next.IsDeleteMarker = false
	next.IsHardDeleted = false
	next.ExpectedETag = ""
	next.PreserveLatest = false
	return next, storage.AppendSummary{}, false, nil
}
