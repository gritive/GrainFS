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
