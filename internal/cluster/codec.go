package cluster

import (
	"encoding/hex"
	"fmt"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/storage"
)

var clusterBuilderPool = pool.New(func() *flatbuffers.Builder { return flatbuffers.NewBuilder(256) })

// objectMeta is a local struct for serializing object metadata to BadgerDB.
type objectMeta struct {
	Key          string
	Size         int64
	ContentType  string
	ETag         string
	LastModified int64
	ACL          uint8    // s3auth.ACLGrant bitmask; 0 = private (backward compat)
	ECData       uint8    // EC k (data shards)
	ECParity     uint8    // EC m (parity shards)
	NodeIDs      []string // shard placement nodes (index i = shard i); empty for N× objects
	// PlacementGroupID is the data Raft group that owns this object version.
	PlacementGroupID string
	UserMetadata     map[string]string
	SSEAlgorithm     string
	// Segments holds appendable owner-local segments or chunked PUT segment
	// refs. Empty/nil for legacy single-blob objects.
	Segments []storage.SegmentRef
	// Coalesced records merged segment blobs. Phase B2 stores each entry
	// owner-locally; Phase B3 distributes via EC and adds placement params.
	Coalesced []CoalescedShardRef
	// IsAppendable indicates the object was created via AppendObject and may
	// continue to be appended to. False for legacy / multipart / PUT objects.
	IsAppendable bool
	// Parts carries the multipart parts list for CompleteMultipartUpload
	// objects; nil for legacy single-blob and appendable objects.
	Parts []storage.MultipartPartEntry
	Tags  []storage.Tag // Task 12a
}

// CoalescedShardRef references a single coalesced blob produced by merging a
// prefix of objectMeta.Segments. Phase B2 holds it owner-locally; Phase B3
// distributes it via EC across NodeIDs and the EC params (NodeIDs / ECData /
// ECParity) are required for the reader to reconstruct.
//
// EC fields are zero-valued for legacy/B2 entries (owner-local only).
type CoalescedShardRef struct {
	CoalescedID string
	Size        int64
	ETag        string
	ShardKey    string
	Version     int32
	ECData      uint8
	ECParity    uint8
	NodeIDs     []string
}

// clusterMultipartMeta holds metadata about an in-progress multipart upload
// as stored in BadgerDB.
type clusterMultipartMeta struct {
	Bucket           string
	Key              string
	CreatedAt        int64
	ContentType      string
	PlacementGroupID string
	// Tags carried from CreateMultipartUploadWithTags; materialised onto the
	// finalised object at CompleteMultipartUpload.
	Tags []storage.Tag
}

// --- helpers ---

func fbFinish(b *flatbuffers.Builder, root flatbuffers.UOffsetT) []byte {
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	b.Reset()
	clusterBuilderPool.Put(b)
	return out
}

// fbSafe wraps a FlatBuffers decode call with a panic→error conversion.
// FlatBuffers panics on malformed data; this makes those panics into errors.
func fbSafe[T any](data []byte, fn func([]byte) T) (t T, err error) {
	if len(data) == 0 {
		return t, fmt.Errorf("empty data")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("invalid flatbuffer: %v", r)
		}
	}()
	return fn(data), nil
}

// --- Command encode/decode ---

func encodeCreateBucketCmd(c CreateBucketCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	clusterpb.CreateBucketCmdStart(b)
	clusterpb.CreateBucketCmdAddBucket(b, bucketOff)
	clusterpb.CreateBucketCmdAddBypassReserved(b, c.BypassReserved)
	return fbFinish(b, clusterpb.CreateBucketCmdEnd(b)), nil
}

// encodeCreateBucketCmdBypass encodes a CreateBucketCmd with bypass_reserved=true.
// Use only from the bootstrap path to seed reserved buckets (e.g. "default", "_grainfs").
// Public-API callers must use encodeCreateBucketCmd with BypassReserved=false (the default).
//
//nolint:unused // referenced by codec_test.go.
func encodeCreateBucketCmdBypass(bucket string) ([]byte, error) {
	return encodeCreateBucketCmd(CreateBucketCmd{Bucket: bucket, BypassReserved: true})
}

func decodeCreateBucketCmd(data []byte) (CreateBucketCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.CreateBucketCmd {
		return clusterpb.GetRootAsCreateBucketCmd(d, 0)
	})
	if err != nil {
		return CreateBucketCmd{}, err
	}
	return CreateBucketCmd{Bucket: string(t.Bucket()), BypassReserved: t.BypassReserved()}, nil
}

func encodeDeleteBucketCmd(c DeleteBucketCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	clusterpb.DeleteBucketCmdStart(b)
	clusterpb.DeleteBucketCmdAddBucket(b, bucketOff)
	return fbFinish(b, clusterpb.DeleteBucketCmdEnd(b)), nil
}

func decodeDeleteBucketCmd(data []byte) (DeleteBucketCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.DeleteBucketCmd {
		return clusterpb.GetRootAsDeleteBucketCmd(d, 0)
	})
	if err != nil {
		return DeleteBucketCmd{}, err
	}
	return DeleteBucketCmd{Bucket: string(t.Bucket())}, nil
}

func encodePutObjectMetaCmd(c PutObjectMetaCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	ctOff := b.CreateString(c.ContentType)
	etagOff := b.CreateString(c.ETag)
	vidOff := b.CreateString(c.VersionID)
	pgOff := b.CreateString(c.PlacementGroupID)
	var sseOff flatbuffers.UOffsetT
	if c.SSEAlgorithm != "" {
		sseOff = b.CreateString(c.SSEAlgorithm)
	}
	var expectedETagOff flatbuffers.UOffsetT
	if c.ExpectedETag != "" {
		expectedETagOff = b.CreateString(c.ExpectedETag)
	}
	var nodeIDsOff flatbuffers.UOffsetT
	if len(c.NodeIDs) > 0 {
		nodeIDsOff = buildStringVector(b, c.NodeIDs, clusterpb.PutObjectMetaCmdStartNodeIdsVector)
	}
	metadataOff := buildKeyValuePropertiesVector(b, c.UserMetadata, clusterpb.PutObjectMetaCmdStartUserMetadataVector)
	// segments — build child SegmentMetaEntry tables BEFORE
	// PutObjectMetaCmdStart. Per the flatbuffers nested-vector rule, each
	// segment's checksum + node_ids vector + strings must also be built
	// before that segment's SegmentMetaEntryStart.
	var segmentsOff flatbuffers.UOffsetT
	if len(c.Segments) > 0 {
		segOffs := make([]flatbuffers.UOffsetT, len(c.Segments))
		for i, s := range c.Segments {
			blobOff := b.CreateString(s.BlobID)
			pgOff := b.CreateString(s.PlacementGroupID)
			var checksumOff flatbuffers.UOffsetT
			if len(s.Checksum) > 0 {
				checksumOff = b.CreateByteVector(s.Checksum)
			}
			var nodeIdsOff flatbuffers.UOffsetT
			if len(s.NodeIDs) > 0 {
				nodeIdsOff = buildStringVector(b, s.NodeIDs, clusterpb.SegmentMetaEntryStartNodeIdsVector)
			}
			clusterpb.SegmentMetaEntryStart(b)
			clusterpb.SegmentMetaEntryAddBlobId(b, blobOff)
			clusterpb.SegmentMetaEntryAddSize(b, s.Size)
			if checksumOff != 0 {
				clusterpb.SegmentMetaEntryAddChecksum(b, checksumOff)
			}
			clusterpb.SegmentMetaEntryAddPlacementGroupId(b, pgOff)
			clusterpb.SegmentMetaEntryAddShardSize(b, s.ShardSize)
			clusterpb.SegmentMetaEntryAddSegmentIdx(b, s.SegmentIdx)
			if nodeIdsOff != 0 {
				clusterpb.SegmentMetaEntryAddNodeIds(b, nodeIdsOff)
			}
			clusterpb.SegmentMetaEntryAddEcData(b, s.ECData)
			clusterpb.SegmentMetaEntryAddEcParity(b, s.ECParity)
			segOffs[i] = clusterpb.SegmentMetaEntryEnd(b)
		}
		clusterpb.PutObjectMetaCmdStartSegmentsVector(b, len(segOffs))
		for i := len(segOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(segOffs[i])
		}
		segmentsOff = b.EndVector(len(segOffs))
	}
	// parts — build child MultipartPartEntry tables BEFORE PutObjectMetaCmdStart.
	var partsOff flatbuffers.UOffsetT
	if len(c.Parts) > 0 {
		partOffs := make([]flatbuffers.UOffsetT, len(c.Parts))
		for i, p := range c.Parts {
			etOff := b.CreateString(p.ETag)
			clusterpb.MultipartPartEntryStart(b)
			clusterpb.MultipartPartEntryAddPartNumber(b, int32(p.PartNumber))
			clusterpb.MultipartPartEntryAddSize(b, p.Size)
			clusterpb.MultipartPartEntryAddEtag(b, etOff)
			partOffs[i] = clusterpb.MultipartPartEntryEnd(b)
		}
		clusterpb.PutObjectMetaCmdStartPartsVector(b, len(partOffs))
		for i := len(partOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(partOffs[i])
		}
		partsOff = b.EndVector(len(partOffs))
	}
	tagsVec := buildTagsVector(b, c.Tags, clusterpb.PutObjectMetaCmdStartTagsVector)
	clusterpb.PutObjectMetaCmdStart(b)
	clusterpb.PutObjectMetaCmdAddBucket(b, bucketOff)
	clusterpb.PutObjectMetaCmdAddKey(b, keyOff)
	clusterpb.PutObjectMetaCmdAddSize(b, c.Size)
	clusterpb.PutObjectMetaCmdAddContentType(b, ctOff)
	clusterpb.PutObjectMetaCmdAddEtag(b, etagOff)
	clusterpb.PutObjectMetaCmdAddModTime(b, c.ModTime)
	clusterpb.PutObjectMetaCmdAddVersionId(b, vidOff)
	clusterpb.PutObjectMetaCmdAddEcData(b, c.ECData)
	clusterpb.PutObjectMetaCmdAddEcParity(b, c.ECParity)
	if nodeIDsOff != 0 {
		clusterpb.PutObjectMetaCmdAddNodeIds(b, nodeIDsOff)
	}
	if metadataOff != 0 {
		clusterpb.PutObjectMetaCmdAddUserMetadata(b, metadataOff)
	}
	if c.PreserveLatest {
		clusterpb.PutObjectMetaCmdAddPreserveLatest(b, true)
	}
	if c.IsDeleteMarker {
		clusterpb.PutObjectMetaCmdAddIsDeleteMarker(b, true)
	}
	clusterpb.PutObjectMetaCmdAddPlacementGroupId(b, pgOff)
	if sseOff != 0 {
		clusterpb.PutObjectMetaCmdAddSseAlgorithm(b, sseOff)
	}
	if expectedETagOff != 0 {
		clusterpb.PutObjectMetaCmdAddExpectedEtag(b, expectedETagOff)
	}
	if partsOff != 0 {
		clusterpb.PutObjectMetaCmdAddParts(b, partsOff)
	}
	if segmentsOff != 0 {
		clusterpb.PutObjectMetaCmdAddSegments(b, segmentsOff)
	}
	if tagsVec != 0 {
		clusterpb.PutObjectMetaCmdAddTags(b, tagsVec)
	}
	return fbFinish(b, clusterpb.PutObjectMetaCmdEnd(b)), nil
}

func decodePutObjectMetaCmd(data []byte) (PutObjectMetaCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.PutObjectMetaCmd {
		return clusterpb.GetRootAsPutObjectMetaCmd(d, 0)
	})
	if err != nil {
		return PutObjectMetaCmd{}, err
	}
	var nodeIDs []string
	if n := t.NodeIdsLength(); n > 0 {
		nodeIDs = make([]string, n)
		for i := range nodeIDs {
			nodeIDs[i] = string(t.NodeIds(i))
		}
	}
	var parts []storage.MultipartPartEntry
	if n := t.PartsLength(); n > 0 {
		parts = make([]storage.MultipartPartEntry, n)
		var pe clusterpb.MultipartPartEntry
		for i := 0; i < n; i++ {
			if !t.Parts(&pe, i) {
				return PutObjectMetaCmd{}, fmt.Errorf("decode parts[%d]", i)
			}
			parts[i] = storage.MultipartPartEntry{
				PartNumber: int(pe.PartNumber()),
				Size:       pe.Size(),
				ETag:       string(pe.Etag()),
			}
		}
	}
	var segments []SegmentMetaEntry
	if n := t.SegmentsLength(); n > 0 {
		segments = make([]SegmentMetaEntry, n)
		var se clusterpb.SegmentMetaEntry
		for i := 0; i < n; i++ {
			if !t.Segments(&se, i) {
				return PutObjectMetaCmd{}, fmt.Errorf("decode segments[%d]", i)
			}
			var nodeIDs []string
			if nn := se.NodeIdsLength(); nn > 0 {
				nodeIDs = make([]string, nn)
				for j := 0; j < nn; j++ {
					nodeIDs[j] = string(se.NodeIds(j))
				}
			}
			var checksum []byte
			if cb := se.ChecksumBytes(); len(cb) > 0 {
				checksum = append([]byte(nil), cb...)
			}
			segments[i] = SegmentMetaEntry{
				BlobID:           string(se.BlobId()),
				Size:             se.Size(),
				Checksum:         checksum,
				PlacementGroupID: string(se.PlacementGroupId()),
				ShardSize:        se.ShardSize(),
				SegmentIdx:       se.SegmentIdx(),
				NodeIDs:          nodeIDs,
				ECData:           se.EcData(),
				ECParity:         se.EcParity(),
			}
		}
	}
	return PutObjectMetaCmd{
		Bucket:           string(t.Bucket()),
		Key:              string(t.Key()),
		Size:             t.Size(),
		ContentType:      string(t.ContentType()),
		ETag:             string(t.Etag()),
		ModTime:          t.ModTime(),
		VersionID:        string(t.VersionId()),
		ECData:           t.EcData(),
		ECParity:         t.EcParity(),
		NodeIDs:          nodeIDs,
		PlacementGroupID: string(t.PlacementGroupId()),
		UserMetadata:     readKeyValueProperties(t.UserMetadataLength(), t.UserMetadata),
		SSEAlgorithm:     string(t.SseAlgorithm()),
		ExpectedETag:     string(t.ExpectedEtag()),
		PreserveLatest:   t.PreserveLatest(),
		IsDeleteMarker:   t.IsDeleteMarker(),
		Parts:            parts,
		Segments:         segments,
		Tags:             readTagsVector(t.TagsLength(), t.Tags),
	}, nil
}

func encodeDeleteObjectCmd(c DeleteObjectCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	vidOff := b.CreateString(c.VersionID)
	clusterpb.DeleteObjectCmdStart(b)
	clusterpb.DeleteObjectCmdAddBucket(b, bucketOff)
	clusterpb.DeleteObjectCmdAddKey(b, keyOff)
	clusterpb.DeleteObjectCmdAddVersionId(b, vidOff)
	return fbFinish(b, clusterpb.DeleteObjectCmdEnd(b)), nil
}

func decodeDeleteObjectCmd(data []byte) (DeleteObjectCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.DeleteObjectCmd {
		return clusterpb.GetRootAsDeleteObjectCmd(d, 0)
	})
	if err != nil {
		return DeleteObjectCmd{}, err
	}
	return DeleteObjectCmd{
		Bucket:    string(t.Bucket()),
		Key:       string(t.Key()),
		VersionID: string(t.VersionId()),
	}, nil
}

func encodeDeleteObjectVersionCmd(c DeleteObjectVersionCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	vidOff := b.CreateString(c.VersionID)
	clusterpb.DeleteObjectVersionCmdStart(b)
	clusterpb.DeleteObjectVersionCmdAddBucket(b, bucketOff)
	clusterpb.DeleteObjectVersionCmdAddKey(b, keyOff)
	clusterpb.DeleteObjectVersionCmdAddVersionId(b, vidOff)
	return fbFinish(b, clusterpb.DeleteObjectVersionCmdEnd(b)), nil
}

func decodeDeleteObjectVersionCmd(data []byte) (DeleteObjectVersionCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.DeleteObjectVersionCmd {
		return clusterpb.GetRootAsDeleteObjectVersionCmd(d, 0)
	})
	if err != nil {
		return DeleteObjectVersionCmd{}, err
	}
	return DeleteObjectVersionCmd{
		Bucket:    string(t.Bucket()),
		Key:       string(t.Key()),
		VersionID: string(t.VersionId()),
	}, nil
}

func encodeCreateMultipartUploadCmd(c CreateMultipartUploadCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	uidOff := b.CreateString(c.UploadID)
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	ctOff := b.CreateString(c.ContentType)
	pgOff := b.CreateString(c.PlacementGroupID)
	tagsVec := buildTagsVector(b, c.Tags, clusterpb.CreateMultipartUploadCmdStartTagsVector)
	clusterpb.CreateMultipartUploadCmdStart(b)
	clusterpb.CreateMultipartUploadCmdAddUploadId(b, uidOff)
	clusterpb.CreateMultipartUploadCmdAddBucket(b, bucketOff)
	clusterpb.CreateMultipartUploadCmdAddKey(b, keyOff)
	clusterpb.CreateMultipartUploadCmdAddContentType(b, ctOff)
	clusterpb.CreateMultipartUploadCmdAddCreatedAt(b, c.CreatedAt)
	clusterpb.CreateMultipartUploadCmdAddPlacementGroupId(b, pgOff)
	if tagsVec != 0 {
		clusterpb.CreateMultipartUploadCmdAddTags(b, tagsVec)
	}
	return fbFinish(b, clusterpb.CreateMultipartUploadCmdEnd(b)), nil
}

func decodeCreateMultipartUploadCmd(data []byte) (CreateMultipartUploadCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.CreateMultipartUploadCmd {
		return clusterpb.GetRootAsCreateMultipartUploadCmd(d, 0)
	})
	if err != nil {
		return CreateMultipartUploadCmd{}, err
	}
	return CreateMultipartUploadCmd{
		UploadID:         string(t.UploadId()),
		Bucket:           string(t.Bucket()),
		Key:              string(t.Key()),
		ContentType:      string(t.ContentType()),
		CreatedAt:        t.CreatedAt(),
		PlacementGroupID: string(t.PlacementGroupId()),
		Tags:             readTagsVector(t.TagsLength(), t.Tags),
	}, nil
}

func encodeCompleteMultipartCmd(c CompleteMultipartCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	uidOff := b.CreateString(c.UploadID)
	ctOff := b.CreateString(c.ContentType)
	etagOff := b.CreateString(c.ETag)
	vidOff := b.CreateString(c.VersionID)
	pgOff := b.CreateString(c.PlacementGroupID)
	var nodeIDsOff flatbuffers.UOffsetT
	if len(c.NodeIDs) > 0 {
		nodeIDsOff = buildStringVector(b, c.NodeIDs, clusterpb.CompleteMultipartCmdStartNodeIdsVector)
	}
	var partsOff flatbuffers.UOffsetT
	if len(c.Parts) > 0 {
		partOffs := make([]flatbuffers.UOffsetT, len(c.Parts))
		for i, p := range c.Parts {
			etOff := b.CreateString(p.ETag)
			clusterpb.MultipartPartEntryStart(b)
			clusterpb.MultipartPartEntryAddPartNumber(b, int32(p.PartNumber))
			clusterpb.MultipartPartEntryAddSize(b, p.Size)
			clusterpb.MultipartPartEntryAddEtag(b, etOff)
			partOffs[i] = clusterpb.MultipartPartEntryEnd(b)
		}
		clusterpb.CompleteMultipartCmdStartPartsVector(b, len(partOffs))
		for i := len(partOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(partOffs[i])
		}
		partsOff = b.EndVector(len(partOffs))
	}
	var segmentsOff flatbuffers.UOffsetT
	if len(c.Segments) > 0 {
		segOffs := make([]flatbuffers.UOffsetT, len(c.Segments))
		for i, s := range c.Segments {
			blobOff := b.CreateString(s.BlobID)
			pgOff := b.CreateString(s.PlacementGroupID)
			var checksumOff flatbuffers.UOffsetT
			if len(s.Checksum) > 0 {
				checksumOff = b.CreateByteVector(s.Checksum)
			}
			var nodeIdsOff flatbuffers.UOffsetT
			if len(s.NodeIDs) > 0 {
				nodeIdsOff = buildStringVector(b, s.NodeIDs, clusterpb.SegmentMetaEntryStartNodeIdsVector)
			}
			clusterpb.SegmentMetaEntryStart(b)
			clusterpb.SegmentMetaEntryAddBlobId(b, blobOff)
			clusterpb.SegmentMetaEntryAddSize(b, s.Size)
			if checksumOff != 0 {
				clusterpb.SegmentMetaEntryAddChecksum(b, checksumOff)
			}
			clusterpb.SegmentMetaEntryAddPlacementGroupId(b, pgOff)
			clusterpb.SegmentMetaEntryAddShardSize(b, s.ShardSize)
			clusterpb.SegmentMetaEntryAddSegmentIdx(b, s.SegmentIdx)
			if nodeIdsOff != 0 {
				clusterpb.SegmentMetaEntryAddNodeIds(b, nodeIdsOff)
			}
			clusterpb.SegmentMetaEntryAddEcData(b, s.ECData)
			clusterpb.SegmentMetaEntryAddEcParity(b, s.ECParity)
			segOffs[i] = clusterpb.SegmentMetaEntryEnd(b)
		}
		clusterpb.CompleteMultipartCmdStartSegmentsVector(b, len(segOffs))
		for i := len(segOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(segOffs[i])
		}
		segmentsOff = b.EndVector(len(segOffs))
	}
	tagsOff := buildTagsVector(b, c.Tags, clusterpb.CompleteMultipartCmdStartTagsVector)
	clusterpb.CompleteMultipartCmdStart(b)
	clusterpb.CompleteMultipartCmdAddBucket(b, bucketOff)
	clusterpb.CompleteMultipartCmdAddKey(b, keyOff)
	clusterpb.CompleteMultipartCmdAddUploadId(b, uidOff)
	clusterpb.CompleteMultipartCmdAddSize(b, c.Size)
	clusterpb.CompleteMultipartCmdAddContentType(b, ctOff)
	clusterpb.CompleteMultipartCmdAddEtag(b, etagOff)
	clusterpb.CompleteMultipartCmdAddModTime(b, c.ModTime)
	clusterpb.CompleteMultipartCmdAddVersionId(b, vidOff)
	clusterpb.CompleteMultipartCmdAddPlacementGroupId(b, pgOff)
	clusterpb.CompleteMultipartCmdAddEcData(b, c.ECData)
	clusterpb.CompleteMultipartCmdAddEcParity(b, c.ECParity)
	if nodeIDsOff != 0 {
		clusterpb.CompleteMultipartCmdAddNodeIds(b, nodeIDsOff)
	}
	if partsOff != 0 {
		clusterpb.CompleteMultipartCmdAddParts(b, partsOff)
	}
	if segmentsOff != 0 {
		clusterpb.CompleteMultipartCmdAddSegments(b, segmentsOff)
	}
	if tagsOff != 0 {
		clusterpb.CompleteMultipartCmdAddTags(b, tagsOff)
	}
	return fbFinish(b, clusterpb.CompleteMultipartCmdEnd(b)), nil
}

func decodeCompleteMultipartCmd(data []byte) (CompleteMultipartCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.CompleteMultipartCmd {
		return clusterpb.GetRootAsCompleteMultipartCmd(d, 0)
	})
	if err != nil {
		return CompleteMultipartCmd{}, err
	}
	var nodeIDs []string
	if n := t.NodeIdsLength(); n > 0 {
		nodeIDs = make([]string, n)
		for i := range nodeIDs {
			nodeIDs[i] = string(t.NodeIds(i))
		}
	}
	var parts []storage.MultipartPartEntry
	if n := t.PartsLength(); n > 0 {
		parts = make([]storage.MultipartPartEntry, n)
		var pe clusterpb.MultipartPartEntry
		for i := 0; i < n; i++ {
			if !t.Parts(&pe, i) {
				return CompleteMultipartCmd{}, fmt.Errorf("decode complete multipart parts[%d]", i)
			}
			parts[i] = storage.MultipartPartEntry{
				PartNumber: int(pe.PartNumber()),
				Size:       pe.Size(),
				ETag:       string(pe.Etag()),
			}
		}
	}
	var segments []SegmentMetaEntry
	if n := t.SegmentsLength(); n > 0 {
		segments = make([]SegmentMetaEntry, n)
		var se clusterpb.SegmentMetaEntry
		for i := 0; i < n; i++ {
			if !t.Segments(&se, i) {
				return CompleteMultipartCmd{}, fmt.Errorf("decode complete multipart segments[%d]", i)
			}
			var nodeIDs []string
			if nn := se.NodeIdsLength(); nn > 0 {
				nodeIDs = make([]string, nn)
				for j := 0; j < nn; j++ {
					nodeIDs[j] = string(se.NodeIds(j))
				}
			}
			var checksum []byte
			if cb := se.ChecksumBytes(); len(cb) > 0 {
				checksum = append([]byte(nil), cb...)
			}
			segments[i] = SegmentMetaEntry{
				BlobID:           string(se.BlobId()),
				Size:             se.Size(),
				Checksum:         checksum,
				PlacementGroupID: string(se.PlacementGroupId()),
				ShardSize:        se.ShardSize(),
				SegmentIdx:       se.SegmentIdx(),
				NodeIDs:          nodeIDs,
				ECData:           se.EcData(),
				ECParity:         se.EcParity(),
			}
		}
	}
	return CompleteMultipartCmd{
		Bucket:           string(t.Bucket()),
		Key:              string(t.Key()),
		UploadID:         string(t.UploadId()),
		Size:             t.Size(),
		ContentType:      string(t.ContentType()),
		ETag:             string(t.Etag()),
		ModTime:          t.ModTime(),
		VersionID:        string(t.VersionId()),
		PlacementGroupID: string(t.PlacementGroupId()),
		ECData:           t.EcData(),
		ECParity:         t.EcParity(),
		NodeIDs:          nodeIDs,
		Parts:            parts,
		Segments:         segments,
		Tags:             readTagsVector(t.TagsLength(), t.Tags),
	}, nil
}

func encodeAbortMultipartCmd(c AbortMultipartCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	uidOff := b.CreateString(c.UploadID)
	clusterpb.AbortMultipartCmdStart(b)
	clusterpb.AbortMultipartCmdAddBucket(b, bucketOff)
	clusterpb.AbortMultipartCmdAddKey(b, keyOff)
	clusterpb.AbortMultipartCmdAddUploadId(b, uidOff)
	return fbFinish(b, clusterpb.AbortMultipartCmdEnd(b)), nil
}

func decodeAbortMultipartCmd(data []byte) (AbortMultipartCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.AbortMultipartCmd {
		return clusterpb.GetRootAsAbortMultipartCmd(d, 0)
	})
	if err != nil {
		return AbortMultipartCmd{}, err
	}
	return AbortMultipartCmd{
		Bucket:   string(t.Bucket()),
		Key:      string(t.Key()),
		UploadID: string(t.UploadId()),
	}, nil
}

func encodeSetBucketPolicyCmd(c SetBucketPolicyCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	var policyOff flatbuffers.UOffsetT
	if len(c.PolicyJSON) > 0 {
		policyOff = b.CreateByteVector(c.PolicyJSON)
	}
	clusterpb.SetBucketPolicyCmdStart(b)
	clusterpb.SetBucketPolicyCmdAddBucket(b, bucketOff)
	if len(c.PolicyJSON) > 0 {
		clusterpb.SetBucketPolicyCmdAddPolicyJson(b, policyOff)
	}
	return fbFinish(b, clusterpb.SetBucketPolicyCmdEnd(b)), nil
}

func decodeSetBucketPolicyCmd(data []byte) (SetBucketPolicyCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.SetBucketPolicyCmd {
		return clusterpb.GetRootAsSetBucketPolicyCmd(d, 0)
	})
	if err != nil {
		return SetBucketPolicyCmd{}, err
	}
	return SetBucketPolicyCmd{Bucket: string(t.Bucket()), PolicyJSON: t.PolicyJsonBytes()}, nil
}

func encodeDeleteBucketPolicyCmd(c DeleteBucketPolicyCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	clusterpb.DeleteBucketPolicyCmdStart(b)
	clusterpb.DeleteBucketPolicyCmdAddBucket(b, bucketOff)
	return fbFinish(b, clusterpb.DeleteBucketPolicyCmdEnd(b)), nil
}

func decodeDeleteBucketPolicyCmd(data []byte) (DeleteBucketPolicyCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.DeleteBucketPolicyCmd {
		return clusterpb.GetRootAsDeleteBucketPolicyCmd(d, 0)
	})
	if err != nil {
		return DeleteBucketPolicyCmd{}, err
	}
	return DeleteBucketPolicyCmd{Bucket: string(t.Bucket())}, nil
}

// buildTagsVector encodes []storage.Tag as a FlatBuffers Tag vector using the
// provided parent-table startVector func (e.g.
// clusterpb.CreateMultipartUploadCmdStartTagsVector). Returns 0 when len==0
// so callers can guard the Add call. Tag child tables MUST be built BEFORE
// the parent table's Start.
func buildTagsVector(b *flatbuffers.Builder, tags []storage.Tag, startVec func(*flatbuffers.Builder, int) flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	if len(tags) == 0 {
		return 0
	}
	tagOffs := make([]flatbuffers.UOffsetT, len(tags))
	for i, t := range tags {
		kOff := b.CreateString(t.Key)
		vOff := b.CreateString(t.Value)
		clusterpb.TagStart(b)
		clusterpb.TagAddKey(b, kOff)
		clusterpb.TagAddValue(b, vOff)
		tagOffs[i] = clusterpb.TagEnd(b)
	}
	startVec(b, len(tagOffs))
	for i := len(tagOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(tagOffs[i])
	}
	return b.EndVector(len(tagOffs))
}

// readTagsVector decodes a FlatBuffers Tag vector via the accessor (length,
// element-by-mutating-receiver). Returns nil when length==0.
func readTagsVector(length int, get func(*clusterpb.Tag, int) bool) []storage.Tag {
	if length == 0 {
		return nil
	}
	out := make([]storage.Tag, length)
	for i := 0; i < length; i++ {
		var tag clusterpb.Tag
		if get(&tag, i) {
			out[i] = storage.Tag{Key: string(tag.Key()), Value: string(tag.Value())}
		}
	}
	return out
}

// buildStringVector encodes a []string as a FlatBuffers vector using the
// provided startVector function (e.g. clusterpb.ObjectMetaStartNodeIdsVector).
// All strings must be created BEFORE calling Start on the parent table.
func buildStringVector(b *flatbuffers.Builder, ss []string, startVec func(*flatbuffers.Builder, int) flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	offs := make([]flatbuffers.UOffsetT, len(ss))
	for i, s := range ss {
		offs[i] = b.CreateString(s)
	}
	startVec(b, len(ss))
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(ss))
}

// --- ObjectMeta codec ---

func marshalObjectMeta(m objectMeta) ([]byte, error) {
	b := clusterBuilderPool.Get()
	keyOff := b.CreateString(m.Key)
	ctOff := b.CreateString(m.ContentType)
	etagOff := b.CreateString(m.ETag)
	pgOff := b.CreateString(m.PlacementGroupID)
	var sseOff flatbuffers.UOffsetT
	if m.SSEAlgorithm != "" {
		sseOff = b.CreateString(m.SSEAlgorithm)
	}
	var nodeIDsOff flatbuffers.UOffsetT
	if len(m.NodeIDs) > 0 {
		nodeIDsOff = buildStringVector(b, m.NodeIDs, clusterpb.ObjectMetaStartNodeIdsVector)
	}
	metadataOff := buildKeyValuePropertiesVector(b, m.UserMetadata, clusterpb.ObjectMetaStartUserMetadataVector)
	// segments — build child SegmentRef tables BEFORE ObjectMetaStart.
	var segmentsOff flatbuffers.UOffsetT
	if len(m.Segments) > 0 {
		segOffs := make([]flatbuffers.UOffsetT, len(m.Segments))
		for i, s := range m.Segments {
			blobOff := b.CreateString(s.BlobID)
			pgOff := b.CreateString(s.PlacementGroupID)
			// TODO(Phase 2): clusterpb.SegmentRef still uses Etag on the wire;
			// hex-encode Checksum (MD5 in cluster Phase 1) for backwards compat
			// until the cluster schema migrates to Checksum bytes.
			etOff := b.CreateString(hex.EncodeToString(s.Checksum))
			var checksumOff flatbuffers.UOffsetT
			if len(s.Checksum) > 0 {
				checksumOff = b.CreateByteVector(s.Checksum)
			}
			var nodeIDsOff flatbuffers.UOffsetT
			if len(s.NodeIDs) > 0 {
				nodeIDsOff = buildStringVector(b, s.NodeIDs, clusterpb.SegmentRefStartNodeIdsVector)
			}
			clusterpb.SegmentRefStart(b)
			clusterpb.SegmentRefAddBlobId(b, blobOff)
			clusterpb.SegmentRefAddSize(b, s.Size)
			clusterpb.SegmentRefAddEtag(b, etOff)
			if checksumOff != 0 {
				clusterpb.SegmentRefAddChecksum(b, checksumOff)
			}
			clusterpb.SegmentRefAddPlacementGroupId(b, pgOff)
			if s.ShardSize != 0 {
				clusterpb.SegmentRefAddShardSize(b, s.ShardSize)
			}
			if nodeIDsOff != 0 {
				clusterpb.SegmentRefAddNodeIds(b, nodeIDsOff)
			}
			if s.ECData != 0 {
				clusterpb.SegmentRefAddEcData(b, s.ECData)
			}
			if s.ECParity != 0 {
				clusterpb.SegmentRefAddEcParity(b, s.ECParity)
			}
			segOffs[i] = clusterpb.SegmentRefEnd(b)
		}
		clusterpb.ObjectMetaStartSegmentsVector(b, len(segOffs))
		for i := len(segOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(segOffs[i])
		}
		segmentsOff = b.EndVector(len(segOffs))
	}
	// coalesced — build child CoalescedShardRef tables BEFORE ObjectMetaStart.
	// Note: node_ids vector for each ref MUST also be built BEFORE its
	// owning table Start (flatbuffers nested-vector rule).
	var coalescedOff flatbuffers.UOffsetT
	if len(m.Coalesced) > 0 {
		cOffs := make([]flatbuffers.UOffsetT, len(m.Coalesced))
		for i, c := range m.Coalesced {
			idOff := b.CreateString(c.CoalescedID)
			etOff := b.CreateString(c.ETag)
			skOff := b.CreateString(c.ShardKey)
			var nodesOff flatbuffers.UOffsetT
			if len(c.NodeIDs) > 0 {
				nodesOff = buildStringVector(b, c.NodeIDs, clusterpb.CoalescedShardRefStartNodeIdsVector)
			}
			clusterpb.CoalescedShardRefStart(b)
			clusterpb.CoalescedShardRefAddCoalescedId(b, idOff)
			clusterpb.CoalescedShardRefAddSize(b, c.Size)
			clusterpb.CoalescedShardRefAddEtag(b, etOff)
			clusterpb.CoalescedShardRefAddShardKey(b, skOff)
			clusterpb.CoalescedShardRefAddVersion(b, c.Version)
			clusterpb.CoalescedShardRefAddEcData(b, c.ECData)
			clusterpb.CoalescedShardRefAddEcParity(b, c.ECParity)
			if nodesOff != 0 {
				clusterpb.CoalescedShardRefAddNodeIds(b, nodesOff)
			}
			cOffs[i] = clusterpb.CoalescedShardRefEnd(b)
		}
		clusterpb.ObjectMetaStartCoalescedVector(b, len(cOffs))
		for i := len(cOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(cOffs[i])
		}
		coalescedOff = b.EndVector(len(cOffs))
	}
	// parts — build child MultipartPartEntry tables BEFORE ObjectMetaStart.
	var partsOff flatbuffers.UOffsetT
	if len(m.Parts) > 0 {
		partOffs := make([]flatbuffers.UOffsetT, len(m.Parts))
		for i, p := range m.Parts {
			etOff := b.CreateString(p.ETag)
			clusterpb.MultipartPartEntryStart(b)
			clusterpb.MultipartPartEntryAddPartNumber(b, int32(p.PartNumber))
			clusterpb.MultipartPartEntryAddSize(b, p.Size)
			clusterpb.MultipartPartEntryAddEtag(b, etOff)
			partOffs[i] = clusterpb.MultipartPartEntryEnd(b)
		}
		clusterpb.ObjectMetaStartPartsVector(b, len(partOffs))
		for i := len(partOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(partOffs[i])
		}
		partsOff = b.EndVector(len(partOffs))
	}
	// tags — build child Tag tables BEFORE ObjectMetaStart.
	var tagsVec flatbuffers.UOffsetT
	if len(m.Tags) > 0 {
		tagOffsets := make([]flatbuffers.UOffsetT, len(m.Tags))
		for i, t := range m.Tags {
			kOff := b.CreateString(t.Key)
			vOff := b.CreateString(t.Value)
			clusterpb.TagStart(b)
			clusterpb.TagAddKey(b, kOff)
			clusterpb.TagAddValue(b, vOff)
			tagOffsets[i] = clusterpb.TagEnd(b)
		}
		clusterpb.ObjectMetaStartTagsVector(b, len(tagOffsets))
		for i := len(tagOffsets) - 1; i >= 0; i-- {
			b.PrependUOffsetT(tagOffsets[i])
		}
		tagsVec = b.EndVector(len(tagOffsets))
	}
	clusterpb.ObjectMetaStart(b)
	clusterpb.ObjectMetaAddKey(b, keyOff)
	clusterpb.ObjectMetaAddSize(b, m.Size)
	clusterpb.ObjectMetaAddContentType(b, ctOff)
	clusterpb.ObjectMetaAddEtag(b, etagOff)
	clusterpb.ObjectMetaAddLastModified(b, m.LastModified)
	clusterpb.ObjectMetaAddAcl(b, m.ACL)
	clusterpb.ObjectMetaAddEcData(b, m.ECData)
	clusterpb.ObjectMetaAddEcParity(b, m.ECParity)
	if nodeIDsOff != 0 {
		clusterpb.ObjectMetaAddNodeIds(b, nodeIDsOff)
	}
	if metadataOff != 0 {
		clusterpb.ObjectMetaAddUserMetadata(b, metadataOff)
	}
	clusterpb.ObjectMetaAddPlacementGroupId(b, pgOff)
	if sseOff != 0 {
		clusterpb.ObjectMetaAddSseAlgorithm(b, sseOff)
	}
	if segmentsOff != 0 {
		clusterpb.ObjectMetaAddSegments(b, segmentsOff)
	}
	if coalescedOff != 0 {
		clusterpb.ObjectMetaAddCoalesced(b, coalescedOff)
	}
	if m.IsAppendable {
		clusterpb.ObjectMetaAddIsAppendable(b, true)
	}
	if partsOff != 0 {
		clusterpb.ObjectMetaAddParts(b, partsOff)
	}
	if tagsVec != 0 {
		clusterpb.ObjectMetaAddTags(b, tagsVec)
	}
	return fbFinish(b, clusterpb.ObjectMetaEnd(b)), nil
}

func unmarshalObjectMeta(data []byte) (objectMeta, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.ObjectMeta {
		return clusterpb.GetRootAsObjectMeta(d, 0)
	})
	if err != nil {
		return objectMeta{}, fmt.Errorf("unmarshal ObjectMeta: %w", err)
	}
	var nodeIDs []string
	if n := t.NodeIdsLength(); n > 0 {
		nodeIDs = make([]string, n)
		for i := range nodeIDs {
			nodeIDs[i] = string(t.NodeIds(i))
		}
	}
	var segments []storage.SegmentRef
	if n := t.SegmentsLength(); n > 0 {
		segments = make([]storage.SegmentRef, n)
		var seg clusterpb.SegmentRef
		for i := 0; i < n; i++ {
			if !t.Segments(&seg, i) {
				continue
			}
			var nodeIDs []string
			if nn := seg.NodeIdsLength(); nn > 0 {
				nodeIDs = make([]string, nn)
				for j := 0; j < nn; j++ {
					nodeIDs[j] = string(seg.NodeIds(j))
				}
			}
			checksum := append([]byte(nil), seg.ChecksumBytes()...)
			if len(checksum) == 0 {
				// TODO(Phase 2): wire Etag is hex-encoded MD5 (cluster Phase 1);
				// decode into Checksum bytes for legacy records that do not carry
				// ChecksumBytes.
				checksum, _ = hex.DecodeString(string(seg.Etag()))
			}
			segments[i] = storage.SegmentRef{
				BlobID:           string(seg.BlobId()),
				Size:             seg.Size(),
				Checksum:         checksum,
				PlacementGroupID: string(seg.PlacementGroupId()),
				ShardSize:        seg.ShardSize(),
				ECData:           seg.EcData(),
				ECParity:         seg.EcParity(),
				NodeIDs:          nodeIDs,
			}
		}
	}
	var coalesced []CoalescedShardRef
	if n := t.CoalescedLength(); n > 0 {
		coalesced = make([]CoalescedShardRef, n)
		var c clusterpb.CoalescedShardRef
		for i := 0; i < n; i++ {
			if !t.Coalesced(&c, i) {
				continue
			}
			var nodeIDs []string
			if nn := c.NodeIdsLength(); nn > 0 {
				nodeIDs = make([]string, nn)
				for j := 0; j < nn; j++ {
					nodeIDs[j] = string(c.NodeIds(j))
				}
			}
			coalesced[i] = CoalescedShardRef{
				CoalescedID: string(c.CoalescedId()),
				Size:        c.Size(),
				ETag:        string(c.Etag()),
				ShardKey:    string(c.ShardKey()),
				Version:     c.Version(),
				ECData:      c.EcData(),
				ECParity:    c.EcParity(),
				NodeIDs:     nodeIDs,
			}
		}
	}
	var parts []storage.MultipartPartEntry
	if n := t.PartsLength(); n > 0 {
		parts = make([]storage.MultipartPartEntry, n)
		var pe clusterpb.MultipartPartEntry
		for i := 0; i < n; i++ {
			if !t.Parts(&pe, i) {
				return objectMeta{}, fmt.Errorf("decode parts[%d]", i)
			}
			parts[i] = storage.MultipartPartEntry{
				PartNumber: int(pe.PartNumber()),
				Size:       pe.Size(),
				ETag:       string(pe.Etag()),
			}
		}
	}
	var tags []storage.Tag
	if n := t.TagsLength(); n > 0 {
		tags = make([]storage.Tag, n)
		for i := 0; i < n; i++ {
			var tag clusterpb.Tag
			if t.Tags(&tag, i) {
				tags[i] = storage.Tag{Key: string(tag.Key()), Value: string(tag.Value())}
			}
		}
	}
	return objectMeta{
		Key:              string(t.Key()),
		Size:             t.Size(),
		ContentType:      string(t.ContentType()),
		ETag:             string(t.Etag()),
		LastModified:     t.LastModified(),
		ACL:              t.Acl(),
		ECData:           t.EcData(),
		ECParity:         t.EcParity(),
		NodeIDs:          nodeIDs,
		PlacementGroupID: string(t.PlacementGroupId()),
		UserMetadata:     readKeyValueProperties(t.UserMetadataLength(), t.UserMetadata),
		SSEAlgorithm:     string(t.SseAlgorithm()),
		Segments:         segments,
		Coalesced:        coalesced,
		IsAppendable:     t.IsAppendable(),
		Parts:            parts,
		Tags:             tags,
	}, nil
}

// --- SnapshotState codec ---

func marshalSnapshotState(state map[string][]byte) ([]byte, error) {
	b := clusterBuilderPool.Get()

	// Sort keys for deterministic output.
	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build each KeyValue in reverse order (FlatBuffers vectors are prepended).
	kvOffsets := make([]flatbuffers.UOffsetT, len(keys))
	for i := len(keys) - 1; i >= 0; i-- {
		k := keys[i]
		v := state[k]
		keyOff := b.CreateString(k)
		var valOff flatbuffers.UOffsetT
		if len(v) > 0 {
			valOff = b.CreateByteVector(v)
		}
		clusterpb.KeyValueStart(b)
		clusterpb.KeyValueAddKey(b, keyOff)
		if len(v) > 0 {
			clusterpb.KeyValueAddValue(b, valOff)
		}
		kvOffsets[i] = clusterpb.KeyValueEnd(b)
	}

	// Build entries vector.
	clusterpb.SnapshotStateStartEntriesVector(b, len(kvOffsets))
	for i := len(kvOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(kvOffsets[i])
	}
	entriesVec := b.EndVector(len(kvOffsets))

	clusterpb.SnapshotStateStart(b)
	clusterpb.SnapshotStateAddEntries(b, entriesVec)
	return fbFinish(b, clusterpb.SnapshotStateEnd(b)), nil
}

func unmarshalSnapshotState(data []byte) (result map[string][]byte, err error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("unmarshal SnapshotState: empty data")
	}
	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = fmt.Errorf("unmarshal SnapshotState: invalid flatbuffer: %v", r)
		}
	}()
	ss := clusterpb.GetRootAsSnapshotState(data, 0)
	result = make(map[string][]byte, ss.EntriesLength())
	var kv clusterpb.KeyValue
	for i := 0; i < ss.EntriesLength(); i++ {
		if !ss.Entries(&kv, i) {
			continue
		}
		result[string(kv.Key())] = kv.ValueBytes()
	}
	return result, nil
}

// --- ClusterMultipartMeta codec ---

func marshalClusterMultipartMeta(m clusterMultipartMeta) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(m.Bucket)
	keyOff := b.CreateString(m.Key)
	ctOff := b.CreateString(m.ContentType)
	pgOff := b.CreateString(m.PlacementGroupID)
	tagsVec := buildTagsVector(b, m.Tags, clusterpb.MultipartMetaStartTagsVector)
	clusterpb.MultipartMetaStart(b)
	clusterpb.MultipartMetaAddContentType(b, ctOff)
	clusterpb.MultipartMetaAddPlacementGroupId(b, pgOff)
	clusterpb.MultipartMetaAddBucket(b, bucketOff)
	clusterpb.MultipartMetaAddKey(b, keyOff)
	clusterpb.MultipartMetaAddCreatedAt(b, m.CreatedAt)
	if tagsVec != 0 {
		clusterpb.MultipartMetaAddTags(b, tagsVec)
	}
	return fbFinish(b, clusterpb.MultipartMetaEnd(b)), nil
}

func unmarshalClusterMultipartMeta(data []byte) (clusterMultipartMeta, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MultipartMeta {
		return clusterpb.GetRootAsMultipartMeta(d, 0)
	})
	if err != nil {
		return clusterMultipartMeta{}, fmt.Errorf("unmarshal MultipartMeta: %w", err)
	}
	return clusterMultipartMeta{
		Bucket:           string(t.Bucket()),
		Key:              string(t.Key()),
		CreatedAt:        t.CreatedAt(),
		ContentType:      string(t.ContentType()),
		PlacementGroupID: string(t.PlacementGroupId()),
		Tags:             readTagsVector(t.TagsLength(), t.Tags),
	}, nil
}

// --- MigrateShard / MigrationDone codec ---

func encodeMigrateShardCmd(c MigrateShardFSMCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	vidOff := b.CreateString(c.VersionID)
	srcOff := b.CreateString(c.SrcNode)
	dstOff := b.CreateString(c.DstNode)
	clusterpb.MigrateShardCmdStart(b)
	clusterpb.MigrateShardCmdAddBucket(b, bucketOff)
	clusterpb.MigrateShardCmdAddKey(b, keyOff)
	clusterpb.MigrateShardCmdAddVersionId(b, vidOff)
	clusterpb.MigrateShardCmdAddSrcNode(b, srcOff)
	clusterpb.MigrateShardCmdAddDstNode(b, dstOff)
	return fbFinish(b, clusterpb.MigrateShardCmdEnd(b)), nil
}

func decodeMigrateShardCmd(data []byte) (MigrateShardFSMCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MigrateShardCmd {
		return clusterpb.GetRootAsMigrateShardCmd(d, 0)
	})
	if err != nil {
		return MigrateShardFSMCmd{}, err
	}
	return MigrateShardFSMCmd{
		Bucket:    string(t.Bucket()),
		Key:       string(t.Key()),
		VersionID: string(t.VersionId()),
		SrcNode:   string(t.SrcNode()),
		DstNode:   string(t.DstNode()),
	}, nil
}

func decodeMigrationDoneCmd(data []byte) (MigrationDoneFSMCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MigrationDoneCmd {
		return clusterpb.GetRootAsMigrationDoneCmd(d, 0)
	})
	if err != nil {
		return MigrationDoneFSMCmd{}, err
	}
	return MigrationDoneFSMCmd{
		Bucket:    string(t.Bucket()),
		Key:       string(t.Key()),
		VersionID: string(t.VersionId()),
		SrcNode:   string(t.SrcNode()),
		DstNode:   string(t.DstNode()),
	}, nil
}

func encodeMigrationDoneCmd(c MigrationDoneFSMCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	vidOff := b.CreateString(c.VersionID)
	srcOff := b.CreateString(c.SrcNode)
	dstOff := b.CreateString(c.DstNode)
	clusterpb.MigrationDoneCmdStart(b)
	clusterpb.MigrationDoneCmdAddBucket(b, bucketOff)
	clusterpb.MigrationDoneCmdAddKey(b, keyOff)
	clusterpb.MigrationDoneCmdAddVersionId(b, vidOff)
	clusterpb.MigrationDoneCmdAddSrcNode(b, srcOff)
	clusterpb.MigrationDoneCmdAddDstNode(b, dstOff)
	return fbFinish(b, clusterpb.MigrationDoneCmdEnd(b)), nil
}

func encodeSetBucketVersioningCmd(c SetBucketVersioningCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	stateOff := b.CreateString(c.State)
	clusterpb.SetBucketVersioningCmdStart(b)
	clusterpb.SetBucketVersioningCmdAddBucket(b, bucketOff)
	clusterpb.SetBucketVersioningCmdAddState(b, stateOff)
	return fbFinish(b, clusterpb.SetBucketVersioningCmdEnd(b)), nil
}

func decodeSetBucketVersioningCmd(data []byte) (SetBucketVersioningCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.SetBucketVersioningCmd {
		return clusterpb.GetRootAsSetBucketVersioningCmd(d, 0)
	})
	if err != nil {
		return SetBucketVersioningCmd{}, err
	}
	return SetBucketVersioningCmd{
		Bucket: string(t.Bucket()),
		State:  string(t.State()),
	}, nil
}

func encodeSetObjectACLCmd(c SetObjectACLCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	clusterpb.SetObjectACLCmdStart(b)
	clusterpb.SetObjectACLCmdAddBucket(b, bucketOff)
	clusterpb.SetObjectACLCmdAddKey(b, keyOff)
	clusterpb.SetObjectACLCmdAddAcl(b, c.ACL)
	return fbFinish(b, clusterpb.SetObjectACLCmdEnd(b)), nil
}

func decodeSetObjectACLCmd(data []byte) (SetObjectACLCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.SetObjectACLCmd {
		return clusterpb.GetRootAsSetObjectACLCmd(d, 0)
	})
	if err != nil {
		return SetObjectACLCmd{}, err
	}
	return SetObjectACLCmd{
		Bucket: string(t.Bucket()),
		Key:    string(t.Key()),
		ACL:    t.Acl(),
	}, nil
}

func encodeSetObjectTagsCmd(c SetObjectTagsCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	verOff := b.CreateString(c.VersionID)

	var tagsVec flatbuffers.UOffsetT
	if len(c.Tags) > 0 {
		tagOffs := make([]flatbuffers.UOffsetT, len(c.Tags))
		for i, t := range c.Tags {
			kOff := b.CreateString(t.Key)
			vOff := b.CreateString(t.Value)
			clusterpb.TagStart(b)
			clusterpb.TagAddKey(b, kOff)
			clusterpb.TagAddValue(b, vOff)
			tagOffs[i] = clusterpb.TagEnd(b)
		}
		clusterpb.SetObjectTagsCmdStartTagsVector(b, len(tagOffs))
		for i := len(tagOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(tagOffs[i])
		}
		tagsVec = b.EndVector(len(tagOffs))
	}

	clusterpb.SetObjectTagsCmdStart(b)
	clusterpb.SetObjectTagsCmdAddBucket(b, bucketOff)
	clusterpb.SetObjectTagsCmdAddKey(b, keyOff)
	clusterpb.SetObjectTagsCmdAddVersionId(b, verOff)
	if tagsVec != 0 {
		clusterpb.SetObjectTagsCmdAddTags(b, tagsVec)
	}
	return fbFinish(b, clusterpb.SetObjectTagsCmdEnd(b)), nil
}

func decodeSetObjectTagsCmd(data []byte) (SetObjectTagsCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.SetObjectTagsCmd {
		return clusterpb.GetRootAsSetObjectTagsCmd(d, 0)
	})
	if err != nil {
		return SetObjectTagsCmd{}, err
	}
	out := SetObjectTagsCmd{
		Bucket:    string(t.Bucket()),
		Key:       string(t.Key()),
		VersionID: string(t.VersionId()),
	}
	if n := t.TagsLength(); n > 0 {
		out.Tags = make([]storage.Tag, n)
		for i := 0; i < n; i++ {
			var tg clusterpb.Tag
			if t.Tags(&tg, i) {
				out.Tags[i] = storage.Tag{Key: string(tg.Key()), Value: string(tg.Value())}
			}
		}
	}
	return out, nil
}

func encodeAppendObjectCmd(c AppendObjectCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	blobIDOff := b.CreateString(c.BlobID)
	etagOff := b.CreateString(c.SegmentETag)
	pgOff := b.CreateString(c.PlacementGroupID)
	vidOff := b.CreateString(c.VersionID)
	clusterpb.AppendObjectCmdStart(b)
	clusterpb.AppendObjectCmdAddBucket(b, bucketOff)
	clusterpb.AppendObjectCmdAddKey(b, keyOff)
	clusterpb.AppendObjectCmdAddExpectedOffset(b, c.ExpectedOffset)
	clusterpb.AppendObjectCmdAddBlobId(b, blobIDOff)
	clusterpb.AppendObjectCmdAddSegmentSize(b, c.SegmentSize)
	clusterpb.AppendObjectCmdAddSegmentEtag(b, etagOff)
	clusterpb.AppendObjectCmdAddPlacementGroupId(b, pgOff)
	clusterpb.AppendObjectCmdAddVersionId(b, vidOff)
	clusterpb.AppendObjectCmdAddModifiedUnixSec(b, c.ModifiedUnixSec)
	return fbFinish(b, clusterpb.AppendObjectCmdEnd(b)), nil
}

func decodeAppendObjectCmd(data []byte) (AppendObjectCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.AppendObjectCmd {
		return clusterpb.GetRootAsAppendObjectCmd(d, 0)
	})
	if err != nil {
		return AppendObjectCmd{}, err
	}
	return AppendObjectCmd{
		Bucket:           string(t.Bucket()),
		Key:              string(t.Key()),
		ExpectedOffset:   t.ExpectedOffset(),
		BlobID:           string(t.BlobId()),
		SegmentSize:      t.SegmentSize(),
		SegmentETag:      string(t.SegmentEtag()),
		PlacementGroupID: string(t.PlacementGroupId()),
		VersionID:        string(t.VersionId()),
		ModifiedUnixSec:  t.ModifiedUnixSec(),
	}, nil
}

func encodeCoalesceSegmentsCmd(c CoalesceSegmentsCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	cidOff := b.CreateString(c.CoalescedID)
	skOff := b.CreateString(c.ShardKey)
	etOff := b.CreateString(c.ETag)
	var consumedOff flatbuffers.UOffsetT
	if len(c.ConsumedSegmentIDs) > 0 {
		consumedOff = buildStringVector(b, c.ConsumedSegmentIDs, clusterpb.CoalesceSegmentsCmdStartConsumedSegmentIdsVector)
	}
	var placementOff flatbuffers.UOffsetT
	if len(c.Placement) > 0 {
		placementOff = buildStringVector(b, c.Placement, clusterpb.CoalesceSegmentsCmdStartPlacementVector)
	}
	clusterpb.CoalesceSegmentsCmdStart(b)
	clusterpb.CoalesceSegmentsCmdAddBucket(b, bucketOff)
	clusterpb.CoalesceSegmentsCmdAddKey(b, keyOff)
	clusterpb.CoalesceSegmentsCmdAddCoalescedId(b, cidOff)
	clusterpb.CoalesceSegmentsCmdAddShardKey(b, skOff)
	clusterpb.CoalesceSegmentsCmdAddSize(b, c.Size)
	clusterpb.CoalesceSegmentsCmdAddEtag(b, etOff)
	if consumedOff != 0 {
		clusterpb.CoalesceSegmentsCmdAddConsumedSegmentIds(b, consumedOff)
	}
	if placementOff != 0 {
		clusterpb.CoalesceSegmentsCmdAddPlacement(b, placementOff)
	}
	clusterpb.CoalesceSegmentsCmdAddEcData(b, c.ECData)
	clusterpb.CoalesceSegmentsCmdAddEcParity(b, c.ECParity)
	return fbFinish(b, clusterpb.CoalesceSegmentsCmdEnd(b)), nil
}

func decodeCoalesceSegmentsCmd(data []byte) (CoalesceSegmentsCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.CoalesceSegmentsCmd {
		return clusterpb.GetRootAsCoalesceSegmentsCmd(d, 0)
	})
	if err != nil {
		return CoalesceSegmentsCmd{}, err
	}
	var consumed []string
	if n := t.ConsumedSegmentIdsLength(); n > 0 {
		consumed = make([]string, n)
		for i := range consumed {
			consumed[i] = string(t.ConsumedSegmentIds(i))
		}
	}
	var placement []string
	if n := t.PlacementLength(); n > 0 {
		placement = make([]string, n)
		for i := range placement {
			placement[i] = string(t.Placement(i))
		}
	}
	return CoalesceSegmentsCmd{
		Bucket:             string(t.Bucket()),
		Key:                string(t.Key()),
		CoalescedID:        string(t.CoalescedId()),
		ShardKey:           string(t.ShardKey()),
		Size:               t.Size(),
		ETag:               string(t.Etag()),
		ConsumedSegmentIDs: consumed,
		Placement:          placement,
		ECData:             t.EcData(),
		ECParity:           t.EcParity(),
	}, nil
}

// encodeSetRingCmd serializes a SetRingCmd for Raft proposal.
func encodeSetRingCmd(c SetRingCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	// VNodeEntry 객체들을 먼저 역순으로 빌드 (FlatBuffers vector prepend 방식)
	vnOffsets := make([]flatbuffers.UOffsetT, len(c.VNodes))
	for i := len(c.VNodes) - 1; i >= 0; i-- {
		nodeIDOff := b.CreateString(c.VNodes[i].NodeID)
		clusterpb.VNodeEntryStart(b)
		clusterpb.VNodeEntryAddToken(b, c.VNodes[i].Token)
		clusterpb.VNodeEntryAddNodeId(b, nodeIDOff)
		vnOffsets[i] = clusterpb.VNodeEntryEnd(b)
	}
	clusterpb.SetRingCmdStartVnodesVector(b, len(vnOffsets))
	for i := len(vnOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(vnOffsets[i])
	}
	vnodesVec := b.EndVector(len(vnOffsets))
	clusterpb.SetRingCmdStart(b)
	clusterpb.SetRingCmdAddVersion(b, c.Version)
	clusterpb.SetRingCmdAddVnodes(b, vnodesVec)
	clusterpb.SetRingCmdAddVperNode(b, uint32(c.VPerNode))
	return fbFinish(b, clusterpb.SetRingCmdEnd(b)), nil
}

// decodeSetRingCmd deserializes a SetRingCmd from Raft log data.
func decodeSetRingCmd(data []byte) (SetRingCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.SetRingCmd {
		return clusterpb.GetRootAsSetRingCmd(d, 0)
	})
	if err != nil {
		return SetRingCmd{}, err
	}
	vnodes := make([]virtualNode, t.VnodesLength())
	for i := 0; i < t.VnodesLength(); i++ {
		var vn clusterpb.VNodeEntry
		t.Vnodes(&vn, i)
		vnodes[i] = virtualNode{Token: vn.Token(), NodeID: string(vn.NodeId())}
	}
	return SetRingCmd{
		Version:  t.Version(),
		VNodes:   vnodes,
		VPerNode: int(t.VperNode()),
	}, nil
}

// --- Payload encoding dispatch ---

func encodePayload(cmdType CommandType, payload any) ([]byte, error) {
	switch cmdType {
	case CmdNoOp:
		return nil, nil
	case CmdCreateBucket:
		return encodeCreateBucketCmd(payload.(CreateBucketCmd))
	case CmdDeleteBucket:
		return encodeDeleteBucketCmd(payload.(DeleteBucketCmd))
	case CmdPutObjectMeta:
		return encodePutObjectMetaCmd(payload.(PutObjectMetaCmd))
	case CmdDeleteObject:
		return encodeDeleteObjectCmd(payload.(DeleteObjectCmd))
	case CmdCreateMultipartUpload:
		return encodeCreateMultipartUploadCmd(payload.(CreateMultipartUploadCmd))
	case CmdCompleteMultipart:
		return encodeCompleteMultipartCmd(payload.(CompleteMultipartCmd))
	case CmdAbortMultipart:
		return encodeAbortMultipartCmd(payload.(AbortMultipartCmd))
	case CmdSetBucketPolicy:
		return encodeSetBucketPolicyCmd(payload.(SetBucketPolicyCmd))
	case CmdDeleteBucketPolicy:
		return encodeDeleteBucketPolicyCmd(payload.(DeleteBucketPolicyCmd))
	case CmdMigrateShard:
		return encodeMigrateShardCmd(payload.(MigrateShardFSMCmd))
	case CmdMigrationDone:
		return encodeMigrationDoneCmd(payload.(MigrationDoneFSMCmd))
	case CmdPutShardPlacement:
		return encodePutShardPlacementCmd(payload.(PutShardPlacementCmd))
	case CmdDeleteShardPlacement:
		return encodeDeleteShardPlacementCmd(payload.(DeleteShardPlacementCmd))
	case CmdDeleteObjectVersion:
		return encodeDeleteObjectVersionCmd(payload.(DeleteObjectVersionCmd))
	case CmdSetBucketVersioning:
		return encodeSetBucketVersioningCmd(payload.(SetBucketVersioningCmd))
	case CmdSetObjectACL:
		return encodeSetObjectACLCmd(payload.(SetObjectACLCmd))
	case CmdSetObjectTags:
		return encodeSetObjectTagsCmd(payload.(SetObjectTagsCmd))
	case CmdAppendObject:
		return encodeAppendObjectCmd(payload.(AppendObjectCmd))
	case CmdCoalesceSegments:
		return encodeCoalesceSegmentsCmd(payload.(CoalesceSegmentsCmd))
	case CmdSetRing:
		return encodeSetRingCmd(payload.(SetRingCmd))
	case CmdPutObjectQuarantine:
		return encodePutObjectQuarantineCmd(payload.(PutObjectQuarantineCmd))
	default:
		return nil, fmt.Errorf("unknown command type: %d", cmdType)
	}
}

func encodePutObjectQuarantineCmd(c PutObjectQuarantineCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	vidOff := b.CreateString(c.VersionID)
	causeOff := b.CreateString(c.Cause)
	reasonOff := b.CreateString(c.Reason)
	clusterpb.PutObjectQuarantineCmdStart(b)
	clusterpb.PutObjectQuarantineCmdAddBucket(b, bucketOff)
	clusterpb.PutObjectQuarantineCmdAddKey(b, keyOff)
	clusterpb.PutObjectQuarantineCmdAddVersionId(b, vidOff)
	clusterpb.PutObjectQuarantineCmdAddCause(b, causeOff)
	clusterpb.PutObjectQuarantineCmdAddReason(b, reasonOff)
	return fbFinish(b, clusterpb.PutObjectQuarantineCmdEnd(b)), nil
}

func decodePutObjectQuarantineCmd(data []byte) (PutObjectQuarantineCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.PutObjectQuarantineCmd {
		return clusterpb.GetRootAsPutObjectQuarantineCmd(d, 0)
	})
	if err != nil {
		return PutObjectQuarantineCmd{}, err
	}
	return PutObjectQuarantineCmd{
		Bucket:    string(t.Bucket()),
		Key:       string(t.Key()),
		VersionID: string(t.VersionId()),
		Cause:     string(t.Cause()),
		Reason:    string(t.Reason()),
	}, nil
}

// decodePutObjectQuarantineCmdStorage is the storage-safe variant of
// decodePutObjectQuarantineCmd. Unlike the RPC version, it wraps BOTH
// GetRootAs AND all field access in defer-recover so a malformed FB blob
// produces a typed error instead of panicking through callers. (Existing
// fbSafe only wraps the GetRootAs call.)
//
// Used by FSM apply paths that read raft-derived badger values.
func decodePutObjectQuarantineCmdStorage(data []byte) (cmd PutObjectQuarantineCmd, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode quarantine storage: malformed FB: %v", r)
		}
	}()
	t := clusterpb.GetRootAsPutObjectQuarantineCmd(data, 0)
	cmd = PutObjectQuarantineCmd{
		Bucket:    string(t.Bucket()),
		Key:       string(t.Key()),
		VersionID: string(t.VersionId()),
		Cause:     string(t.Cause()),
		Reason:    string(t.Reason()),
	}
	return cmd, nil
}

func encodePutShardPlacementCmd(c PutShardPlacementCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)

	nodeOffs := make([]flatbuffers.UOffsetT, len(c.NodeIDs))
	for i, n := range c.NodeIDs {
		nodeOffs[i] = b.CreateString(n)
	}
	clusterpb.PutShardPlacementCmdStartNodeIdsVector(b, len(c.NodeIDs))
	for i := len(c.NodeIDs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(nodeOffs[i])
	}
	nodesVec := b.EndVector(len(c.NodeIDs))

	clusterpb.PutShardPlacementCmdStart(b)
	clusterpb.PutShardPlacementCmdAddBucket(b, bucketOff)
	clusterpb.PutShardPlacementCmdAddKey(b, keyOff)
	clusterpb.PutShardPlacementCmdAddNodeIds(b, nodesVec)
	clusterpb.PutShardPlacementCmdAddK(b, int32(c.K))
	clusterpb.PutShardPlacementCmdAddM(b, int32(c.M))
	return fbFinish(b, clusterpb.PutShardPlacementCmdEnd(b)), nil
}

//nolint:unused // package tests pin command wire compatibility.
func decodePutShardPlacementCmd(data []byte) (PutShardPlacementCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.PutShardPlacementCmd {
		return clusterpb.GetRootAsPutShardPlacementCmd(d, 0)
	})
	if err != nil {
		return PutShardPlacementCmd{}, err
	}
	n := t.NodeIdsLength()
	nodes := make([]string, n)
	for i := 0; i < n; i++ {
		nodes[i] = string(t.NodeIds(i))
	}
	return PutShardPlacementCmd{
		Bucket:  string(t.Bucket()),
		Key:     string(t.Key()),
		NodeIDs: nodes,
		K:       int(t.K()),
		M:       int(t.M()),
	}, nil
}

func encodeDeleteShardPlacementCmd(c DeleteShardPlacementCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	clusterpb.DeleteShardPlacementCmdStart(b)
	clusterpb.DeleteShardPlacementCmdAddBucket(b, bucketOff)
	clusterpb.DeleteShardPlacementCmdAddKey(b, keyOff)
	return fbFinish(b, clusterpb.DeleteShardPlacementCmdEnd(b)), nil
}

//nolint:unused // package tests pin command wire compatibility.
func decodeDeleteShardPlacementCmd(data []byte) (DeleteShardPlacementCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.DeleteShardPlacementCmd {
		return clusterpb.GetRootAsDeleteShardPlacementCmd(d, 0)
	})
	if err != nil {
		return DeleteShardPlacementCmd{}, err
	}
	return DeleteShardPlacementCmd{
		Bucket: string(t.Bucket()),
		Key:    string(t.Key()),
	}, nil
}
