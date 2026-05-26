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

type appendObjectCommandInput struct {
	Bucket           string
	Key              string
	ExpectedOffset   int64
	Segment          storage.SegmentRef
	PlacementGroupID string
	VersionID        string
	ModifiedUnixSec  int64
}

func buildAppendObjectCommand(in appendObjectCommandInput) AppendObjectCmd {
	return AppendObjectCmd{
		Bucket:           in.Bucket,
		Key:              in.Key,
		ExpectedOffset:   in.ExpectedOffset,
		BlobID:           in.Segment.BlobID,
		SegmentSize:      in.Segment.Size,
		SegmentETag:      hex.EncodeToString(in.Segment.Checksum),
		PlacementGroupID: in.PlacementGroupID,
		VersionID:        in.VersionID,
		ModifiedUnixSec:  in.ModifiedUnixSec,
	}
}

type appendObjectTransitionInput struct {
	Existing          *objectMeta
	ExistingVersionID string
	Cmd               AppendObjectCmd
	ModifiedUnixSec   int64
	CoalesceCfg       CoalesceConfig
}

type appendObjectTransitionResult struct {
	Noop            bool
	SizeCapRejected bool
}

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
