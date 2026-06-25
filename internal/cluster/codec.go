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
	StripeBytes  uint32   // 0 = contiguous/legacy, >0 = stripe-interleaved chunk size
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
	// MetaSeq is the lowest-priority quorum-meta LWW tiebreak (see
	// PutObjectMetaCmd.MetaSeq). Carried on the FSM-stored objectMeta so a
	// re-derived quorum-meta blob cannot be tied by a stale one. Default 0.
	MetaSeq uint64
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
	StripeBytes uint32
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

// decodeCreateBucketCmd is test-only (see codec_bucket_retired_test.go).

func encodeDeleteBucketCmd(c DeleteBucketCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	clusterpb.DeleteBucketCmdStart(b)
	clusterpb.DeleteBucketCmdAddBucket(b, bucketOff)
	return fbFinish(b, clusterpb.DeleteBucketCmdEnd(b)), nil
}

// decodeDeleteBucketCmd is test-only (see codec_bucket_retired_test.go).

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
	// Slice 2: quarantine cause string — must be created before PutObjectMetaCmdStart.
	var quarantineCauseOff flatbuffers.UOffsetT
	if c.QuarantineCause != "" {
		quarantineCauseOff = b.CreateString(c.QuarantineCause)
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
			clusterpb.SegmentMetaEntryAddStripeBytes(b, s.StripeBytes)
			segOffs[i] = clusterpb.SegmentMetaEntryEnd(b)
		}
		clusterpb.PutObjectMetaCmdStartSegmentsVector(b, len(segOffs))
		for i := len(segOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(segOffs[i])
		}
		segmentsOff = b.EndVector(len(segOffs))
	}
	// parts — build child MultipartPartEntry tables BEFORE PutObjectMetaCmdStart.
	partsOff := buildPartsVector(b, c.Parts, clusterpb.PutObjectMetaCmdStartPartsVector)
	tagsVec := buildTagsVector(b, c.Tags, clusterpb.PutObjectMetaCmdStartTagsVector)
	// coalesced — build child CoalescedShardRef tables BEFORE PutObjectMetaCmdStart.
	coalescedOff := buildCoalescedVector(b, c.Coalesced, clusterpb.PutObjectMetaCmdStartCoalescedVector)
	// append_call_md5s — build child BytesValue tables BEFORE PutObjectMetaCmdStart.
	var appendMD5sOff flatbuffers.UOffsetT
	if len(c.AppendCallMD5s) > 0 {
		md5Offs := make([]flatbuffers.UOffsetT, len(c.AppendCallMD5s))
		for i, d := range c.AppendCallMD5s {
			vOff := b.CreateByteVector(d)
			clusterpb.BytesValueStart(b)
			clusterpb.BytesValueAddVBytes(b, vOff)
			md5Offs[i] = clusterpb.BytesValueEnd(b)
		}
		clusterpb.PutObjectMetaCmdStartAppendCallMd5sVector(b, len(md5Offs))
		for i := len(md5Offs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(md5Offs[i])
		}
		appendMD5sOff = b.EndVector(len(md5Offs))
	}
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
	clusterpb.PutObjectMetaCmdAddStripeBytes(b, c.StripeBytes)
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
	if c.ACL != 0 {
		clusterpb.PutObjectMetaCmdAddAcl(b, c.ACL)
	}
	if c.MetaSeq != 0 {
		clusterpb.PutObjectMetaCmdAddMetaSeq(b, c.MetaSeq)
	}
	if c.IsHardDeleted {
		clusterpb.PutObjectMetaCmdAddIsHardDeleted(b, true)
	}
	if coalescedOff != 0 {
		clusterpb.PutObjectMetaCmdAddCoalesced(b, coalescedOff)
	}
	if c.IsAppendable {
		clusterpb.PutObjectMetaCmdAddIsAppendable(b, true)
	}
	if c.MetaSeqCAS {
		clusterpb.PutObjectMetaCmdAddMetaSeqCas(b, true)
	}
	if c.IsQuarantined {
		clusterpb.PutObjectMetaCmdAddIsQuarantined(b, true)
	}
	if quarantineCauseOff != 0 {
		clusterpb.PutObjectMetaCmdAddQuarantineCause(b, quarantineCauseOff)
	}
	if appendMD5sOff != 0 {
		clusterpb.PutObjectMetaCmdAddAppendCallMd5s(b, appendMD5sOff)
	}
	return fbFinish(b, clusterpb.PutObjectMetaCmdEnd(b)), nil
}

// encodeQuorumMetaBlob serializes an object's quorum-meta record for the off-raft
// quorum-meta tree (and the per-version / multipart-manifest blobs that reuse it).
// It is the bare PutObjectMetaCmd FlatBuffer — NOT wrapped in a clusterpb.Command
// envelope — so the data plane no longer depends on the raft command codec/enum.
// Every quorum-meta blob is a PutObjectMetaCmd, so no command-type discriminator is
// needed. WIRE FORMAT: bare PutObjectMetaCmd FB (greenfield; see CHANGELOG).
func encodeQuorumMetaBlob(c PutObjectMetaCmd) ([]byte, error) {
	return encodePutObjectMetaCmd(c)
}

// decodeQuorumMetaBlob is the inverse of encodeQuorumMetaBlob.
func decodeQuorumMetaBlob(data []byte) (PutObjectMetaCmd, error) {
	return decodePutObjectMetaCmd(data)
}

func decodePutObjectMetaCmd(data []byte) (out PutObjectMetaCmd, err error) {
	// A corrupt blob can pass GetRootAsPutObjectMetaCmd (which only reads the root
	// vtable offset) yet panic later when a field accessor (e.g. NodeIdsLength)
	// indexes past the buffer. fbSafe below covers only the root read, so the
	// whole field-decode body must also be panic-safe to stay fail-closed: a
	// malformed on-disk quorum-meta blob must surface as an error, never a panic.
	defer func() {
		if r := recover(); r != nil {
			out = PutObjectMetaCmd{}
			err = fmt.Errorf("invalid flatbuffer: %v", r)
		}
	}()
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
				StripeBytes:      se.StripeBytes(),
			}
		}
	}
	var coalesced []CoalescedShardRef
	if n := t.CoalescedLength(); n > 0 {
		coalesced = make([]CoalescedShardRef, n)
		var c clusterpb.CoalescedShardRef
		for i := 0; i < n; i++ {
			if !t.Coalesced(&c, i) {
				return PutObjectMetaCmd{}, fmt.Errorf("decode coalesced[%d]", i)
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
				StripeBytes: c.StripeBytes(),
				NodeIDs:     nodeIDs,
			}
		}
	}
	var appendCallMD5s [][]byte
	if n := t.AppendCallMd5sLength(); n > 0 {
		appendCallMD5s = make([][]byte, n)
		var bv clusterpb.BytesValue
		for i := 0; i < n; i++ {
			if !t.AppendCallMd5s(&bv, i) {
				return PutObjectMetaCmd{}, fmt.Errorf("decode append_call_md5s[%d]", i)
			}
			if v := bv.VBytesBytes(); len(v) > 0 {
				appendCallMD5s[i] = append([]byte(nil), v...)
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
		StripeBytes:      t.StripeBytes(),
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
		ACL:              t.Acl(),
		MetaSeq:          t.MetaSeq(),
		IsHardDeleted:    t.IsHardDeleted(),
		Coalesced:        coalesced,
		IsAppendable:     t.IsAppendable(),
		MetaSeqCAS:       t.MetaSeqCas(),
		IsQuarantined:    t.IsQuarantined(),
		QuarantineCause:  string(t.QuarantineCause()),
		AppendCallMD5s:   appendCallMD5s,
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

// decodeSetBucketPolicyCmd is test-only (see codec_bucket_retired_test.go).

func encodeDeleteBucketPolicyCmd(c DeleteBucketPolicyCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	clusterpb.DeleteBucketPolicyCmdStart(b)
	clusterpb.DeleteBucketPolicyCmdAddBucket(b, bucketOff)
	return fbFinish(b, clusterpb.DeleteBucketPolicyCmdEnd(b)), nil
}

// decodeDeleteBucketPolicyCmd is test-only (see codec_bucket_retired_test.go).

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

// buildCoalescedVector encodes []CoalescedShardRef as a CoalescedShardRef
// FlatBuffers vector using the provided parent-table startVector func (e.g.
// clusterpb.PutObjectMetaCmdStartCoalescedVector /
// clusterpb.ObjectMetaStartCoalescedVector). Returns 0 when len==0 so callers
// can guard the Add call. Each ref's node_ids vector and child strings MUST be
// built BEFORE that ref's CoalescedShardRefStart, and the whole vector BEFORE
// the parent table's Start (flatbuffers nested-vector rule).
//
// EcData/EcParity/StripeBytes are written unconditionally to preserve
// byte-identity with the inline loops this replaced (do NOT add a 0-default
// omit — semantically similar but not byte-identical).
func buildCoalescedVector(b *flatbuffers.Builder, refs []CoalescedShardRef, startVec func(*flatbuffers.Builder, int) flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	if len(refs) == 0 {
		return 0
	}
	cOffs := make([]flatbuffers.UOffsetT, len(refs))
	for i, c := range refs {
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
		clusterpb.CoalescedShardRefAddStripeBytes(b, c.StripeBytes)
		if nodesOff != 0 {
			clusterpb.CoalescedShardRefAddNodeIds(b, nodesOff)
		}
		cOffs[i] = clusterpb.CoalescedShardRefEnd(b)
	}
	startVec(b, len(cOffs))
	for i := len(cOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(cOffs[i])
	}
	return b.EndVector(len(cOffs))
}

// buildPartsVector encodes []storage.MultipartPartEntry as a MultipartPartEntry
// FlatBuffers vector using the provided parent-table startVector func (e.g.
// clusterpb.PutObjectMetaCmdStartPartsVector /
// clusterpb.ObjectMetaStartPartsVector). Returns 0 when len==0. Each entry's
// etag string MUST be created BEFORE its MultipartPartEntryStart, and the whole
// vector BEFORE the parent table's Start.
//
// Deliberately narrow to []storage.MultipartPartEntry (NOT storage.Part /
// forward PartRef) so the int32(PartNumber) cast and Add order stay
// byte-identical with the inline loops this replaced.
func buildPartsVector(b *flatbuffers.Builder, parts []storage.MultipartPartEntry, startVec func(*flatbuffers.Builder, int) flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	if len(parts) == 0 {
		return 0
	}
	partOffs := make([]flatbuffers.UOffsetT, len(parts))
	for i, p := range parts {
		etOff := b.CreateString(p.ETag)
		clusterpb.MultipartPartEntryStart(b)
		clusterpb.MultipartPartEntryAddPartNumber(b, int32(p.PartNumber))
		clusterpb.MultipartPartEntryAddSize(b, p.Size)
		clusterpb.MultipartPartEntryAddEtag(b, etOff)
		partOffs[i] = clusterpb.MultipartPartEntryEnd(b)
	}
	startVec(b, len(partOffs))
	for i := len(partOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(partOffs[i])
	}
	return b.EndVector(len(partOffs))
}

// --- ObjectMeta codec ---

func marshalObjectMeta(m objectMeta) ([]byte, error) { //nolint:unused // referenced by codec_test.go and multiple *_test.go helpers
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
			if s.StripeBytes != 0 {
				clusterpb.SegmentRefAddStripeBytes(b, s.StripeBytes)
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
	coalescedOff := buildCoalescedVector(b, m.Coalesced, clusterpb.ObjectMetaStartCoalescedVector)
	// parts — build child MultipartPartEntry tables BEFORE ObjectMetaStart.
	partsOff := buildPartsVector(b, m.Parts, clusterpb.ObjectMetaStartPartsVector)
	// tags — build child Tag tables BEFORE ObjectMetaStart.
	tagsVec := buildTagsVector(b, m.Tags, clusterpb.ObjectMetaStartTagsVector)
	clusterpb.ObjectMetaStart(b)
	clusterpb.ObjectMetaAddKey(b, keyOff)
	clusterpb.ObjectMetaAddSize(b, m.Size)
	clusterpb.ObjectMetaAddContentType(b, ctOff)
	clusterpb.ObjectMetaAddEtag(b, etagOff)
	clusterpb.ObjectMetaAddLastModified(b, m.LastModified)
	clusterpb.ObjectMetaAddAcl(b, m.ACL)
	clusterpb.ObjectMetaAddEcData(b, m.ECData)
	clusterpb.ObjectMetaAddEcParity(b, m.ECParity)
	clusterpb.ObjectMetaAddStripeBytes(b, m.StripeBytes)
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
	if m.MetaSeq != 0 {
		clusterpb.ObjectMetaAddMetaSeq(b, m.MetaSeq)
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
				StripeBytes:      seg.StripeBytes(),
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
				StripeBytes: c.StripeBytes(),
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
		StripeBytes:      t.StripeBytes(),
		NodeIDs:          nodeIDs,
		PlacementGroupID: string(t.PlacementGroupId()),
		UserMetadata:     readKeyValueProperties(t.UserMetadataLength(), t.UserMetadata),
		SSEAlgorithm:     string(t.SseAlgorithm()),
		Segments:         segments,
		Coalesced:        coalesced,
		IsAppendable:     t.IsAppendable(),
		Parts:            parts,
		Tags:             tags,
		MetaSeq:          t.MetaSeq(),
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

func encodeSetBucketVersioningCmd(c SetBucketVersioningCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(c.Bucket)
	stateOff := b.CreateString(c.State)
	clusterpb.SetBucketVersioningCmdStart(b)
	clusterpb.SetBucketVersioningCmdAddBucket(b, bucketOff)
	clusterpb.SetBucketVersioningCmdAddState(b, stateOff)
	return fbFinish(b, clusterpb.SetBucketVersioningCmdEnd(b)), nil
}

// decodeSetBucketVersioningCmd is test-only (see codec_bucket_retired_test.go).

// --- Payload encoding dispatch ---

func encodePayload(cmdType CommandType, payload any) ([]byte, error) {
	switch cmdType {
	case CmdNoOp:
		return nil, nil
	case CmdCreateBucket:
		return encodeCreateBucketCmd(payload.(CreateBucketCmd))
	case CmdDeleteBucket:
		return encodeDeleteBucketCmd(payload.(DeleteBucketCmd))
	case CmdSetBucketPolicy:
		return encodeSetBucketPolicyCmd(payload.(SetBucketPolicyCmd))
	case CmdDeleteBucketPolicy:
		return encodeDeleteBucketPolicyCmd(payload.(DeleteBucketPolicyCmd))
	case CmdSetBucketVersioning:
		return encodeSetBucketVersioningCmd(payload.(SetBucketVersioningCmd))
	case CmdResealFSMValues:
		return encodeResealFSMValuesCmd(payload.(ResealFSMValuesCmd))
	case CmdFSMValueResealDone:
		return encodeFSMValueResealDoneCmd(payload.(FSMValueResealDoneCmd))
	default:
		// Unknown / retired command type. The per-object/multipart/append/placement
		// commands moved off-raft and have no production proposer; the quorum-meta
		// blob is encoded via encodeQuorumMetaBlob, not through this raft payload path.
		return nil, fmt.Errorf("unknown command type: %d", cmdType)
	}
}

func encodeResealFSMValuesCmd(c ResealFSMValuesCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	var keysOff flatbuffers.UOffsetT
	if len(c.Keys) > 0 {
		keysOff = buildStringVector(b, c.Keys, clusterpb.ResealFSMValuesCmdStartKeysVector)
	}
	clusterpb.ResealFSMValuesCmdStart(b)
	if len(c.Keys) > 0 {
		clusterpb.ResealFSMValuesCmdAddKeys(b, keysOff)
	}
	clusterpb.ResealFSMValuesCmdAddActiveGen(b, c.ActiveGen)
	return fbFinish(b, clusterpb.ResealFSMValuesCmdEnd(b)), nil
}

func decodeResealFSMValuesCmd(data []byte) (ResealFSMValuesCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.ResealFSMValuesCmd {
		return clusterpb.GetRootAsResealFSMValuesCmd(d, 0)
	})
	if err != nil {
		return ResealFSMValuesCmd{}, err
	}
	n := t.KeysLength()
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = string(t.Keys(i))
	}
	return ResealFSMValuesCmd{Keys: keys, ActiveGen: t.ActiveGen()}, nil
}

func encodeFSMValueResealDoneCmd(c FSMValueResealDoneCmd) ([]byte, error) {
	b := clusterBuilderPool.Get()
	clusterpb.FSMValueResealDoneCmdStart(b)
	clusterpb.FSMValueResealDoneCmdAddGen(b, c.Gen)
	return fbFinish(b, clusterpb.FSMValueResealDoneCmdEnd(b)), nil
}

//nolint:unused // referenced by codec_test.go (TestFSMValueResealDoneCmd_RoundTrip).
func decodeFSMValueResealDoneCmd(data []byte) (FSMValueResealDoneCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.FSMValueResealDoneCmd {
		return clusterpb.GetRootAsFSMValueResealDoneCmd(d, 0)
	})
	if err != nil {
		return FSMValueResealDoneCmd{}, err
	}
	return FSMValueResealDoneCmd{Gen: t.Gen()}, nil
}
