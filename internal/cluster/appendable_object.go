package cluster

import (
	"encoding/hex"
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

//nolint:unused // referenced by appendable_object_test.go and apply_test.go
type appendObjectTransitionInput struct {
	Existing          *objectMeta
	ExistingVersionID string
	Cmd               AppendObjectCmd
	ModifiedUnixSec   int64
	CoalesceCfg       CoalesceConfig
}

//nolint:unused // referenced by appendable_object_test.go
type appendObjectTransitionResult struct {
	Noop            bool
	SizeCapRejected bool
}

//nolint:unused // referenced by appendable_object_test.go
func applyAppendObjectTransition(in appendObjectTransitionInput) (objectMeta, appendObjectTransitionResult, error) {
	cmd := in.Cmd
	existing := in.Existing
	if existing != nil && cmd.PlacementGroupID != "" && existing.PlacementGroupID != "" &&
		cmd.PlacementGroupID != existing.PlacementGroupID {
		return objectMeta{}, appendObjectTransitionResult{}, ErrStalePlacement
	}

	segDigest, _ := hex.DecodeString(cmd.SegmentETag)
	seg := storage.SegmentRef{
		BlobID:   cmd.BlobID,
		Size:     cmd.SegmentSize,
		Checksum: segDigest,
	}

	if appendObjectCommandAlreadyApplied(existing, cmd.BlobID) {
		return objectMeta{}, appendObjectTransitionResult{Noop: true}, nil
	}

	if existing == nil {
		if cmd.ExpectedOffset != 0 {
			return objectMeta{}, appendObjectTransitionResult{}, storage.ErrAppendOffsetMismatch
		}
		return objectMeta{
			Key:              cmd.Key,
			Size:             seg.Size,
			ContentType:      "application/octet-stream",
			ETag:             storage.CompositeETag([][]byte{segDigest}),
			LastModified:     in.ModifiedUnixSec,
			PlacementGroupID: cmd.PlacementGroupID,
			Segments:         []storage.SegmentRef{seg},
			IsAppendable:     true,
		}, appendObjectTransitionResult{}, nil
	}

	if existing.Size != cmd.ExpectedOffset {
		return objectMeta{}, appendObjectTransitionResult{}, storage.ErrAppendOffsetMismatch
	}
	if len(existing.Segments) >= storage.MaxAppendSegments {
		return objectMeta{}, appendObjectTransitionResult{}, storage.ErrAppendCapExceeded
	}
	if in.CoalesceCfg.SizeCapBytes > 0 && existing.Size+seg.Size > in.CoalesceCfg.SizeCapBytes {
		return objectMeta{}, appendObjectTransitionResult{SizeCapRejected: true}, storage.ErrAppendObjectTooLarge
	}

	segs := append(append([]storage.SegmentRef(nil), existing.Segments...), seg)
	updated := *existing
	if !updated.IsAppendable {
		updated.IsAppendable = true
		if len(updated.Segments) == 0 && len(updated.Coalesced) == 0 && updated.Size > 0 {
			updated.Coalesced = []CoalescedShardRef{appendBaseCoalescedRef(cmd.Key, in.ExistingVersionID, existing)}
		}
	}
	updated.Segments = segs
	updated.Size = existing.Size + seg.Size
	callDigests := make([][]byte, 0, len(segs))
	for _, s := range segs {
		if len(s.Checksum) > 0 {
			callDigests = append(callDigests, s.Checksum)
		}
	}
	updated.ETag = storage.CompositeETag(callDigests)
	updated.LastModified = in.ModifiedUnixSec
	return updated, appendObjectTransitionResult{}, nil
}

//nolint:unused // referenced by object_meta_resolve.go (used in object_meta_resolve_test.go)
func appendObjectCommandAlreadyApplied(existing *objectMeta, blobID string) bool {
	if existing == nil {
		return false
	}
	for _, s := range existing.Segments {
		if s.BlobID == blobID {
			return true
		}
	}
	return false
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
}

// planAppendObjectBlobRMW validates an append against the base manifest and
// builds the next-generation PutObjectMetaCmd (MetaSeqCAS, MetaSeq=base+1).
// It lifts the offset/segment-cap/size-cap/placement/composite-ETag/ModTime
// checks that previously lived in the FSM apply (applyAppendObjectTransition)
// into the owner-side RMW. The returned cmd is ready for writeQuorumMeta.
//
// The offset check (offset == base.Size) is the primary client-retry dedup:
// BlobID is a fresh UUIDv7 per call (not content-addressed), so a retried
// append at a stale offset correctly fails the offset check rather than being
// silently deduped.
func planAppendObjectBlobRMW(in appendBlobRMWInput) (PutObjectMetaCmd, error) {
	seg := in.Segment

	if !in.BaseExists {
		if in.ExpectedOffset != 0 {
			return PutObjectMetaCmd{}, storage.ErrAppendOffsetMismatch
		}
		return PutObjectMetaCmd{
			Bucket:           in.Bucket,
			Key:              in.Key,
			Size:             seg.Size,
			ContentType:      "application/octet-stream",
			ETag:             storage.CompositeETag([][]byte{seg.Checksum}),
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
		}, nil
	}

	base := in.Base
	// Placement must not have moved since the manifest was written (mirrors the
	// FSM stale-placement gate in applyAppendObjectTransition).
	if in.PlacementGroupID != "" && base.PlacementGroupID != "" &&
		in.PlacementGroupID != base.PlacementGroupID {
		return PutObjectMetaCmd{}, ErrStalePlacement
	}
	if base.Size != in.ExpectedOffset {
		return PutObjectMetaCmd{}, storage.ErrAppendOffsetMismatch
	}
	if len(base.Segments) >= storage.MaxAppendSegments {
		return PutObjectMetaCmd{}, storage.ErrAppendCapExceeded
	}
	if in.SizeCapBytes > 0 && base.Size+seg.Size > in.SizeCapBytes {
		return PutObjectMetaCmd{}, storage.ErrAppendObjectTooLarge
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

	// Append the new segment and recompute Size + composite ETag byte-identically
	// to the FSM path (storage.CompositeETag over per-segment checksums).
	segs := append(segmentMetaEntriesToRefs(base.Segments), seg)
	next.Segments = segmentRefsToMetaEntries(segs)
	next.Size = base.Size + seg.Size
	callDigests := make([][]byte, 0, len(segs))
	for _, s := range segs {
		if len(s.Checksum) > 0 {
			callDigests = append(callDigests, s.Checksum)
		}
	}
	next.ETag = storage.CompositeETag(callDigests)
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
	return next, nil
}
