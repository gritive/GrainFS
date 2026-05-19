package cluster

import (
	"errors"
	"io"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
)

// errInternalReply is returned when a forwarded reply parses but is missing
// expected fields (Object/Upload/Part) — indicates a server-side encoder bug
// or schema drift, not a normal error path.
var errInternalReply = errors.New("forward: internal reply error")

// forward_codec.go: FBS args + reply build/parse helpers shared by
// ClusterCoordinator (request-side) and ForwardReceiver (response-side, T8).
// Centralizing these in one file means a schema bump touches only 10 callers
// here rather than scattered routing methods.
//
// Convention: legacy body-bearing args keep body bytes inside the FBS payload.
// Production serve wiring streams PutObject/UploadPart bodies separately on
// StreamGroupForwardBody.

// --- Args builders (request side) ---

func buildPutObjectArgs(bucket, key, contentType string, body []byte) []byte {
	b := flatbuffers.NewBuilder(putObjectArgsBuilderSize(bucket, key, contentType, len(body)))
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	ct := b.CreateString(contentType)
	bodyOff := b.CreateByteVector(body)
	raftpb.PutObjectArgsStart(b)
	raftpb.PutObjectArgsAddBucket(b, bk)
	raftpb.PutObjectArgsAddKey(b, k)
	raftpb.PutObjectArgsAddContentType(b, ct)
	raftpb.PutObjectArgsAddBody(b, bodyOff)
	b.Finish(raftpb.PutObjectArgsEnd(b))
	return b.FinishedBytes()
}

func putObjectArgsBuilderSize(bucket, key, contentType string, bodyLen int) int {
	const tableOverhead = 128
	n := bodyLen + len(bucket) + len(key) + len(contentType) + tableOverhead
	if bodyLen > 0 {
		n += bodyLen
	}
	return n
}

func buildGetObjectArgs(bucket, key string) []byte {
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	raftpb.GetObjectArgsStart(b)
	raftpb.GetObjectArgsAddBucket(b, bk)
	raftpb.GetObjectArgsAddKey(b, k)
	b.Finish(raftpb.GetObjectArgsEnd(b))
	return b.FinishedBytes()
}

func buildReadAtArgs(bucket, key string, offset, length int64) []byte {
	b := flatbuffers.NewBuilder(80)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	raftpb.ReadAtArgsStart(b)
	raftpb.ReadAtArgsAddBucket(b, bk)
	raftpb.ReadAtArgsAddKey(b, k)
	raftpb.ReadAtArgsAddOffset(b, offset)
	raftpb.ReadAtArgsAddLength(b, length)
	b.Finish(raftpb.ReadAtArgsEnd(b))
	return b.FinishedBytes()
}

func buildGetObjectVersionArgs(bucket, key, versionID string) []byte {
	b := flatbuffers.NewBuilder(96)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	vid := b.CreateString(versionID)
	raftpb.GetObjectVersionArgsStart(b)
	raftpb.GetObjectVersionArgsAddBucket(b, bk)
	raftpb.GetObjectVersionArgsAddKey(b, k)
	raftpb.GetObjectVersionArgsAddVersionId(b, vid)
	b.Finish(raftpb.GetObjectVersionArgsEnd(b))
	return b.FinishedBytes()
}

func buildHeadObjectVersionArgs(bucket, key, versionID string) []byte {
	b := flatbuffers.NewBuilder(96)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	vid := b.CreateString(versionID)
	raftpb.HeadObjectVersionArgsStart(b)
	raftpb.HeadObjectVersionArgsAddBucket(b, bk)
	raftpb.HeadObjectVersionArgsAddKey(b, k)
	raftpb.HeadObjectVersionArgsAddVersionId(b, vid)
	b.Finish(raftpb.HeadObjectVersionArgsEnd(b))
	return b.FinishedBytes()
}

func buildHeadObjectArgs(bucket, key string) []byte {
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	raftpb.HeadObjectArgsStart(b)
	raftpb.HeadObjectArgsAddBucket(b, bk)
	raftpb.HeadObjectArgsAddKey(b, k)
	b.Finish(raftpb.HeadObjectArgsEnd(b))
	return b.FinishedBytes()
}

func buildDeleteObjectArgs(bucket, key string) []byte {
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	raftpb.DeleteObjectArgsStart(b)
	raftpb.DeleteObjectArgsAddBucket(b, bk)
	raftpb.DeleteObjectArgsAddKey(b, k)
	b.Finish(raftpb.DeleteObjectArgsEnd(b))
	return b.FinishedBytes()
}

func buildSetObjectACLArgs(bucket, key string, acl uint8) []byte {
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	raftpb.SetObjectACLArgsStart(b)
	raftpb.SetObjectACLArgsAddBucket(b, bk)
	raftpb.SetObjectACLArgsAddKey(b, k)
	raftpb.SetObjectACLArgsAddAcl(b, acl)
	b.Finish(raftpb.SetObjectACLArgsEnd(b))
	return b.FinishedBytes()
}

func buildSetObjectTagsArgs(bucket, key, versionID string, tags []storage.Tag) []byte {
	b := flatbuffers.NewBuilder(128)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	vid := b.CreateString(versionID)

	var tagsVec flatbuffers.UOffsetT
	if len(tags) > 0 {
		offs := make([]flatbuffers.UOffsetT, len(tags))
		for i, t := range tags {
			kOff := b.CreateString(t.Key)
			vOff := b.CreateString(t.Value)
			raftpb.TagStart(b)
			raftpb.TagAddKey(b, kOff)
			raftpb.TagAddValue(b, vOff)
			offs[i] = raftpb.TagEnd(b)
		}
		raftpb.SetObjectTagsArgsStartTagsVector(b, len(offs))
		for i := len(offs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(offs[i])
		}
		tagsVec = b.EndVector(len(offs))
	}

	raftpb.SetObjectTagsArgsStart(b)
	raftpb.SetObjectTagsArgsAddBucket(b, bk)
	raftpb.SetObjectTagsArgsAddKey(b, k)
	raftpb.SetObjectTagsArgsAddVersionId(b, vid)
	if tagsVec != 0 {
		raftpb.SetObjectTagsArgsAddTags(b, tagsVec)
	}
	b.Finish(raftpb.SetObjectTagsArgsEnd(b))
	return b.FinishedBytes()
}

func buildGetObjectTagsArgs(bucket, key, versionID string) []byte {
	b := flatbuffers.NewBuilder(96)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	vid := b.CreateString(versionID)
	raftpb.GetObjectTagsArgsStart(b)
	raftpb.GetObjectTagsArgsAddBucket(b, bk)
	raftpb.GetObjectTagsArgsAddKey(b, k)
	raftpb.GetObjectTagsArgsAddVersionId(b, vid)
	b.Finish(raftpb.GetObjectTagsArgsEnd(b))
	return b.FinishedBytes()
}

// buildGetObjectTagsReply packs []storage.Tag into ForwardReply.tags. Empty
// tag set is encoded as a zero-length vector via the absent tags field;
// tagsFromReply returns nil for both, preserving the GetObjectTags contract.
func buildGetObjectTagsReply(tags []storage.Tag) []byte {
	b := flatbuffers.NewBuilder(64 + len(tags)*32)
	tagsOff := appendForwardTagsVector(b, tags, raftpb.ForwardReplyStartTagsVector)
	raftpb.ForwardReplyStart(b)
	raftpb.ForwardReplyAddStatus(b, raftpb.ForwardStatusOK)
	if tagsOff != 0 {
		raftpb.ForwardReplyAddTags(b, tagsOff)
	}
	b.Finish(raftpb.ForwardReplyEnd(b))
	return b.FinishedBytes()
}

func buildDeleteObjectVersionArgs(bucket, key, versionID string) []byte {
	b := flatbuffers.NewBuilder(96)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	vid := b.CreateString(versionID)
	raftpb.DeleteObjectVersionArgsStart(b)
	raftpb.DeleteObjectVersionArgsAddBucket(b, bk)
	raftpb.DeleteObjectVersionArgsAddKey(b, k)
	raftpb.DeleteObjectVersionArgsAddVersionId(b, vid)
	b.Finish(raftpb.DeleteObjectVersionArgsEnd(b))
	return b.FinishedBytes()
}

func buildListObjectsArgs(bucket, prefix, marker string, maxKeys int32) []byte {
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	pf := b.CreateString(prefix)
	var mk flatbuffers.UOffsetT
	if marker != "" {
		mk = b.CreateString(marker)
	}
	raftpb.ListObjectsArgsStart(b)
	raftpb.ListObjectsArgsAddBucket(b, bk)
	raftpb.ListObjectsArgsAddPrefix(b, pf)
	raftpb.ListObjectsArgsAddMaxKeys(b, maxKeys)
	if mk != 0 {
		raftpb.ListObjectsArgsAddMarker(b, mk)
	}
	b.Finish(raftpb.ListObjectsArgsEnd(b))
	return b.FinishedBytes()
}

func buildListObjectVersionsArgs(bucket, prefix string, maxKeys int32) []byte {
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	pf := b.CreateString(prefix)
	raftpb.ListObjectVersionsArgsStart(b)
	raftpb.ListObjectVersionsArgsAddBucket(b, bk)
	raftpb.ListObjectVersionsArgsAddPrefix(b, pf)
	raftpb.ListObjectVersionsArgsAddMaxKeys(b, maxKeys)
	b.Finish(raftpb.ListObjectVersionsArgsEnd(b))
	return b.FinishedBytes()
}

func buildWalkObjectsArgs(bucket, prefix string) []byte {
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	pf := b.CreateString(prefix)
	raftpb.WalkObjectsArgsStart(b)
	raftpb.WalkObjectsArgsAddBucket(b, bk)
	raftpb.WalkObjectsArgsAddPrefix(b, pf)
	b.Finish(raftpb.WalkObjectsArgsEnd(b))
	return b.FinishedBytes()
}

// buildCreateMultipartUploadArgs encodes the forward args for both
// CreateMultipartUpload (tags == nil) and CreateMultipartUploadWithTags
// (tags non-empty). An empty/nil tags slice is encoded as an absent vector
// so the receiver's TagsLength() check cleanly distinguishes the two paths
// — mirrors the SetObjectTagsArgs builder convention.
func buildCreateMultipartUploadArgs(bucket, key, contentType string, tags []storage.Tag) []byte {
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	ct := b.CreateString(contentType)

	var tagsVec flatbuffers.UOffsetT
	if len(tags) > 0 {
		offs := make([]flatbuffers.UOffsetT, len(tags))
		for i, t := range tags {
			kOff := b.CreateString(t.Key)
			vOff := b.CreateString(t.Value)
			raftpb.TagStart(b)
			raftpb.TagAddKey(b, kOff)
			raftpb.TagAddValue(b, vOff)
			offs[i] = raftpb.TagEnd(b)
		}
		raftpb.CreateMultipartUploadArgsStartTagsVector(b, len(offs))
		for i := len(offs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(offs[i])
		}
		tagsVec = b.EndVector(len(offs))
	}

	raftpb.CreateMultipartUploadArgsStart(b)
	raftpb.CreateMultipartUploadArgsAddBucket(b, bk)
	raftpb.CreateMultipartUploadArgsAddKey(b, k)
	raftpb.CreateMultipartUploadArgsAddContentType(b, ct)
	if tagsVec != 0 {
		raftpb.CreateMultipartUploadArgsAddTags(b, tagsVec)
	}
	b.Finish(raftpb.CreateMultipartUploadArgsEnd(b))
	return b.FinishedBytes()
}

func buildUploadPartArgs(bucket, key, uploadID string, partNumber int32, body []byte) []byte {
	b := flatbuffers.NewBuilder(64 + len(body))
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	u := b.CreateString(uploadID)
	bodyOff := b.CreateByteVector(body)
	raftpb.UploadPartArgsStart(b)
	raftpb.UploadPartArgsAddBucket(b, bk)
	raftpb.UploadPartArgsAddKey(b, k)
	raftpb.UploadPartArgsAddUploadId(b, u)
	raftpb.UploadPartArgsAddPartNumber(b, partNumber)
	raftpb.UploadPartArgsAddBody(b, bodyOff)
	b.Finish(raftpb.UploadPartArgsEnd(b))
	return b.FinishedBytes()
}

func buildCompleteMultipartUploadArgs(bucket, key, uploadID string, parts []storage.Part) []byte {
	b := flatbuffers.NewBuilder(128)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	u := b.CreateString(uploadID)
	// Build each PartRef table first (order matters for offsets).
	partRefs := make([]flatbuffers.UOffsetT, len(parts))
	for i, p := range parts {
		etag := b.CreateString(p.ETag)
		raftpb.PartRefStart(b)
		raftpb.PartRefAddPartNumber(b, int32(p.PartNumber))
		raftpb.PartRefAddEtag(b, etag)
		partRefs[i] = raftpb.PartRefEnd(b)
	}
	raftpb.CompleteMultipartUploadArgsStartPartsVector(b, len(parts))
	for i := len(partRefs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(partRefs[i])
	}
	partsVec := b.EndVector(len(parts))
	raftpb.CompleteMultipartUploadArgsStart(b)
	raftpb.CompleteMultipartUploadArgsAddBucket(b, bk)
	raftpb.CompleteMultipartUploadArgsAddKey(b, k)
	raftpb.CompleteMultipartUploadArgsAddUploadId(b, u)
	raftpb.CompleteMultipartUploadArgsAddParts(b, partsVec)
	b.Finish(raftpb.CompleteMultipartUploadArgsEnd(b))
	return b.FinishedBytes()
}

func buildAbortMultipartUploadArgs(bucket, key, uploadID string) []byte {
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	u := b.CreateString(uploadID)
	raftpb.AbortMultipartUploadArgsStart(b)
	raftpb.AbortMultipartUploadArgsAddBucket(b, bk)
	raftpb.AbortMultipartUploadArgsAddKey(b, k)
	raftpb.AbortMultipartUploadArgsAddUploadId(b, u)
	b.Finish(raftpb.AbortMultipartUploadArgsEnd(b))
	return b.FinishedBytes()
}

func buildListPartsArgs(bucket, key, uploadID string, maxParts int32) []byte {
	b := flatbuffers.NewBuilder(96)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	u := b.CreateString(uploadID)
	raftpb.ListPartsArgsStart(b)
	raftpb.ListPartsArgsAddBucket(b, bk)
	raftpb.ListPartsArgsAddKey(b, k)
	raftpb.ListPartsArgsAddUploadId(b, u)
	raftpb.ListPartsArgsAddMaxParts(b, maxParts)
	b.Finish(raftpb.ListPartsArgsEnd(b))
	return b.FinishedBytes()
}

// buildAppendObjectForwardArgs builds the header for an AppendObject forward.
// Body bytes are streamed separately on the same QUIC stream (forwardBodyStream).
func buildAppendObjectForwardArgs(bucket, key string, expectedOffset int64) []byte {
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	raftpb.AppendObjectForwardArgsStart(b)
	raftpb.AppendObjectForwardArgsAddBucket(b, bk)
	raftpb.AppendObjectForwardArgsAddKey(b, k)
	raftpb.AppendObjectForwardArgsAddExpectedOffset(b, expectedOffset)
	b.Finish(raftpb.AppendObjectForwardArgsEnd(b))
	return b.FinishedBytes()
}

func buildListMultipartUploadsArgs(bucket, prefix string, maxUploads int32) []byte {
	b := flatbuffers.NewBuilder(80)
	bk := b.CreateString(bucket)
	pf := b.CreateString(prefix)
	raftpb.ListMultipartUploadsArgsStart(b)
	raftpb.ListMultipartUploadsArgsAddBucket(b, bk)
	raftpb.ListMultipartUploadsArgsAddPrefix(b, pf)
	raftpb.ListMultipartUploadsArgsAddMaxUploads(b, maxUploads)
	b.Finish(raftpb.ListMultipartUploadsArgsEnd(b))
	return b.FinishedBytes()
}

// --- Reply builders (response side — used by ForwardReceiver in T8 + tests) ---

// appendPartsVector builds the per-part FBS tables for storage.Object.Parts
// and returns the parent vector offset. Returns 0 when there is nothing to
// encode so callers can skip the AddParts call. MUST be invoked BEFORE
// ForwardObjectMetaStart on the same builder (flatbuffers nested-vector
// rule: child tables/vector must finish before parent Start).
func appendPartsVector(b *flatbuffers.Builder, parts []storage.MultipartPartEntry) flatbuffers.UOffsetT {
	if len(parts) == 0 {
		return 0
	}
	offs := make([]flatbuffers.UOffsetT, len(parts))
	for i, p := range parts {
		etOff := b.CreateString(p.ETag)
		raftpb.ForwardPartMetaStart(b)
		raftpb.ForwardPartMetaAddPartNumber(b, int32(p.PartNumber))
		raftpb.ForwardPartMetaAddEtag(b, etOff)
		raftpb.ForwardPartMetaAddSize(b, p.Size)
		offs[i] = raftpb.ForwardPartMetaEnd(b)
	}
	raftpb.ForwardObjectMetaStartPartsVector(b, len(offs))
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(offs))
}

// appendForwardTagsVector encodes []storage.Tag as a Tag FlatBuffers vector
// using the provided parent-table startVector func (one of
// ForwardObjectMetaStartTagsVector / ForwardObjectVersionMetaStartTagsVector /
// ForwardReplyStartTagsVector). Mirrors appendPartsVector and the codec.go
// buildTagsVector helper — note the FBS Tag tables here come from raftpb
// (forward path), not clusterpb. MUST be invoked BEFORE the parent table's
// Start on the same builder.
func appendForwardTagsVector(b *flatbuffers.Builder, tags []storage.Tag, startVec func(*flatbuffers.Builder, int) flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	if len(tags) == 0 {
		return 0
	}
	offs := make([]flatbuffers.UOffsetT, len(tags))
	for i, t := range tags {
		kOff := b.CreateString(t.Key)
		vOff := b.CreateString(t.Value)
		raftpb.TagStart(b)
		raftpb.TagAddKey(b, kOff)
		raftpb.TagAddValue(b, vOff)
		offs[i] = raftpb.TagEnd(b)
	}
	startVec(b, len(offs))
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(offs))
}

// readForwardTagsVector decodes a raftpb.Tag vector via the accessor
// (length, element-by-mutating-receiver). Returns nil when length==0 so
// callers preserve a nil-clean storage.Object.Tags for untagged objects.
// string(t.Key()) copies out of the FBS buffer so the returned slice is
// safe to keep after the reply bytes go out of scope.
func readForwardTagsVector(length int, get func(*raftpb.Tag, int) bool) []storage.Tag {
	if length == 0 {
		return nil
	}
	out := make([]storage.Tag, 0, length)
	var t raftpb.Tag
	for i := 0; i < length; i++ {
		if !get(&t, i) {
			continue
		}
		out = append(out, storage.Tag{
			Key:   string(t.Key()),
			Value: string(t.Value()),
		})
	}
	return out
}

// readPartsVector inverse of appendPartsVector — decodes ForwardObjectMeta.parts
// into storage.Object.Parts. Returns nil for empty/missing vectors so the
// caller's Object.Parts stays nil-clean for non-multipart objects.
func readPartsVector(meta *raftpb.ForwardObjectMeta) []storage.MultipartPartEntry {
	n := meta.PartsLength()
	if n == 0 {
		return nil
	}
	out := make([]storage.MultipartPartEntry, n)
	var pe raftpb.ForwardPartMeta
	for i := 0; i < n; i++ {
		if !meta.Parts(&pe, i) {
			continue
		}
		out[i] = storage.MultipartPartEntry{
			PartNumber: int(pe.PartNumber()),
			Size:       pe.Size(),
			ETag:       string(pe.Etag()),
		}
	}
	return out
}

// buildObjectReply builds an OK reply with a populated ForwardObjectMeta —
// used by PutObject and HeadObject (no body).
func buildObjectReply(obj *storage.Object, bucket string) []byte {
	b := flatbuffers.NewBuilder(128)
	bk := b.CreateString(bucket)
	k := b.CreateString(obj.Key)
	etag := b.CreateString(obj.ETag)
	ct := b.CreateString(obj.ContentType)
	vid := b.CreateString(obj.VersionID)
	partsOff := appendPartsVector(b, obj.Parts)
	tagsOff := appendForwardTagsVector(b, obj.Tags, raftpb.ForwardObjectMetaStartTagsVector)
	raftpb.ForwardObjectMetaStart(b)
	raftpb.ForwardObjectMetaAddBucket(b, bk)
	raftpb.ForwardObjectMetaAddKey(b, k)
	raftpb.ForwardObjectMetaAddSize(b, obj.Size)
	raftpb.ForwardObjectMetaAddEtag(b, etag)
	raftpb.ForwardObjectMetaAddContentType(b, ct)
	raftpb.ForwardObjectMetaAddModifiedUnixMs(b, obj.LastModified)
	raftpb.ForwardObjectMetaAddVersionId(b, vid)
	if partsOff != 0 {
		raftpb.ForwardObjectMetaAddParts(b, partsOff)
	}
	if tagsOff != 0 {
		raftpb.ForwardObjectMetaAddTags(b, tagsOff)
	}
	metaOff := raftpb.ForwardObjectMetaEnd(b)

	raftpb.ForwardReplyStart(b)
	raftpb.ForwardReplyAddStatus(b, raftpb.ForwardStatusOK)
	raftpb.ForwardReplyAddObject(b, metaOff)
	b.Finish(raftpb.ForwardReplyEnd(b))
	return b.FinishedBytes()
}

// buildGetObjectReply embeds the read body inside ForwardReply.read_body.
func buildGetObjectReply(obj *storage.Object, bucket string, body []byte) []byte {
	b := flatbuffers.NewBuilder(128 + len(body))
	bk := b.CreateString(bucket)
	k := b.CreateString(obj.Key)
	etag := b.CreateString(obj.ETag)
	ct := b.CreateString(obj.ContentType)
	vid := b.CreateString(obj.VersionID)
	bodyOff := b.CreateByteVector(body)
	partsOff := appendPartsVector(b, obj.Parts)
	tagsOff := appendForwardTagsVector(b, obj.Tags, raftpb.ForwardObjectMetaStartTagsVector)

	raftpb.ForwardObjectMetaStart(b)
	raftpb.ForwardObjectMetaAddBucket(b, bk)
	raftpb.ForwardObjectMetaAddKey(b, k)
	raftpb.ForwardObjectMetaAddSize(b, obj.Size)
	raftpb.ForwardObjectMetaAddEtag(b, etag)
	raftpb.ForwardObjectMetaAddContentType(b, ct)
	raftpb.ForwardObjectMetaAddModifiedUnixMs(b, obj.LastModified)
	raftpb.ForwardObjectMetaAddVersionId(b, vid)
	if partsOff != 0 {
		raftpb.ForwardObjectMetaAddParts(b, partsOff)
	}
	if tagsOff != 0 {
		raftpb.ForwardObjectMetaAddTags(b, tagsOff)
	}
	metaOff := raftpb.ForwardObjectMetaEnd(b)

	raftpb.ForwardReplyStart(b)
	raftpb.ForwardReplyAddStatus(b, raftpb.ForwardStatusOK)
	raftpb.ForwardReplyAddObject(b, metaOff)
	raftpb.ForwardReplyAddReadBody(b, bodyOff)
	b.Finish(raftpb.ForwardReplyEnd(b))
	return b.FinishedBytes()
}

func buildReadAtReply(body []byte) []byte {
	b := flatbuffers.NewBuilder(64 + len(body))
	bodyOff := b.CreateByteVector(body)
	raftpb.ForwardReplyStart(b)
	raftpb.ForwardReplyAddStatus(b, raftpb.ForwardStatusOK)
	raftpb.ForwardReplyAddReadBody(b, bodyOff)
	b.Finish(raftpb.ForwardReplyEnd(b))
	return b.FinishedBytes()
}

// buildObjectsReply packs a list of *storage.Object into ForwardReply.objects
// — used by ListObjects and WalkObjects.
func buildObjectsReply(bucket string, objs []*storage.Object) []byte {
	b := flatbuffers.NewBuilder(64 + len(objs)*64)

	metas := make([]flatbuffers.UOffsetT, len(objs))
	for i, o := range objs {
		bk := b.CreateString(bucket)
		k := b.CreateString(o.Key)
		etag := b.CreateString(o.ETag)
		ct := b.CreateString(o.ContentType)
		vid := b.CreateString(o.VersionID)
		// Tags vector MUST be built before the parent ForwardObjectMetaStart
		// (FBS nested-vector rule). Per-element inside the loop is required.
		tagsOff := appendForwardTagsVector(b, o.Tags, raftpb.ForwardObjectMetaStartTagsVector)
		raftpb.ForwardObjectMetaStart(b)
		raftpb.ForwardObjectMetaAddBucket(b, bk)
		raftpb.ForwardObjectMetaAddKey(b, k)
		raftpb.ForwardObjectMetaAddSize(b, o.Size)
		raftpb.ForwardObjectMetaAddEtag(b, etag)
		raftpb.ForwardObjectMetaAddContentType(b, ct)
		raftpb.ForwardObjectMetaAddModifiedUnixMs(b, o.LastModified)
		raftpb.ForwardObjectMetaAddVersionId(b, vid)
		if tagsOff != 0 {
			raftpb.ForwardObjectMetaAddTags(b, tagsOff)
		}
		metas[i] = raftpb.ForwardObjectMetaEnd(b)
	}
	raftpb.ForwardReplyStartObjectsVector(b, len(objs))
	for i := len(metas) - 1; i >= 0; i-- {
		b.PrependUOffsetT(metas[i])
	}
	objsVec := b.EndVector(len(objs))

	raftpb.ForwardReplyStart(b)
	raftpb.ForwardReplyAddStatus(b, raftpb.ForwardStatusOK)
	raftpb.ForwardReplyAddObjects(b, objsVec)
	b.Finish(raftpb.ForwardReplyEnd(b))
	return b.FinishedBytes()
}

func buildObjectVersionsReply(versions []*storage.ObjectVersion) []byte {
	b := flatbuffers.NewBuilder(64 + len(versions)*64)

	metas := make([]flatbuffers.UOffsetT, len(versions))
	for i, v := range versions {
		k := b.CreateString(v.Key)
		vid := b.CreateString(v.VersionID)
		etag := b.CreateString(v.ETag)
		// Tags vector MUST be built before the parent
		// ForwardObjectVersionMetaStart (FBS nested-vector rule).
		tagsOff := appendForwardTagsVector(b, v.Tags, raftpb.ForwardObjectVersionMetaStartTagsVector)
		raftpb.ForwardObjectVersionMetaStart(b)
		raftpb.ForwardObjectVersionMetaAddKey(b, k)
		raftpb.ForwardObjectVersionMetaAddVersionId(b, vid)
		raftpb.ForwardObjectVersionMetaAddIsLatest(b, v.IsLatest)
		raftpb.ForwardObjectVersionMetaAddIsDeleteMarker(b, v.IsDeleteMarker)
		raftpb.ForwardObjectVersionMetaAddEtag(b, etag)
		raftpb.ForwardObjectVersionMetaAddSize(b, v.Size)
		raftpb.ForwardObjectVersionMetaAddModifiedUnixMs(b, v.LastModified)
		if tagsOff != 0 {
			raftpb.ForwardObjectVersionMetaAddTags(b, tagsOff)
		}
		metas[i] = raftpb.ForwardObjectVersionMetaEnd(b)
	}
	raftpb.ForwardReplyStartVersionsVector(b, len(versions))
	for i := len(metas) - 1; i >= 0; i-- {
		b.PrependUOffsetT(metas[i])
	}
	versionsVec := b.EndVector(len(versions))

	raftpb.ForwardReplyStart(b)
	raftpb.ForwardReplyAddStatus(b, raftpb.ForwardStatusOK)
	raftpb.ForwardReplyAddVersions(b, versionsVec)
	b.Finish(raftpb.ForwardReplyEnd(b))
	return b.FinishedBytes()
}

// buildUploadReply for CreateMultipartUpload.
func buildUploadReply(bucket, key, uploadID string) []byte {
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	u := b.CreateString(uploadID)
	raftpb.ForwardMultipartUploadMetaStart(b)
	raftpb.ForwardMultipartUploadMetaAddBucket(b, bk)
	raftpb.ForwardMultipartUploadMetaAddKey(b, k)
	raftpb.ForwardMultipartUploadMetaAddUploadId(b, u)
	upOff := raftpb.ForwardMultipartUploadMetaEnd(b)

	raftpb.ForwardReplyStart(b)
	raftpb.ForwardReplyAddStatus(b, raftpb.ForwardStatusOK)
	raftpb.ForwardReplyAddUpload(b, upOff)
	b.Finish(raftpb.ForwardReplyEnd(b))
	return b.FinishedBytes()
}

func buildMultipartUploadsReply(uploads []*storage.MultipartUpload) []byte {
	b := flatbuffers.NewBuilder(64 + len(uploads)*64)
	metas := make([]flatbuffers.UOffsetT, len(uploads))
	for i, upload := range uploads {
		bk := b.CreateString(upload.Bucket)
		k := b.CreateString(upload.Key)
		u := b.CreateString(upload.UploadID)
		ct := b.CreateString(upload.ContentType)
		raftpb.ForwardMultipartUploadMetaStart(b)
		raftpb.ForwardMultipartUploadMetaAddBucket(b, bk)
		raftpb.ForwardMultipartUploadMetaAddKey(b, k)
		raftpb.ForwardMultipartUploadMetaAddUploadId(b, u)
		raftpb.ForwardMultipartUploadMetaAddContentType(b, ct)
		raftpb.ForwardMultipartUploadMetaAddCreatedAt(b, upload.CreatedAt)
		metas[i] = raftpb.ForwardMultipartUploadMetaEnd(b)
	}
	raftpb.ForwardReplyStartUploadsVector(b, len(uploads))
	for i := len(metas) - 1; i >= 0; i-- {
		b.PrependUOffsetT(metas[i])
	}
	uploadsVec := b.EndVector(len(uploads))
	raftpb.ForwardReplyStart(b)
	raftpb.ForwardReplyAddStatus(b, raftpb.ForwardStatusOK)
	raftpb.ForwardReplyAddUploads(b, uploadsVec)
	b.Finish(raftpb.ForwardReplyEnd(b))
	return b.FinishedBytes()
}

// buildPartReply for UploadPart.
func buildPartReply(part *storage.Part) []byte {
	b := flatbuffers.NewBuilder(64)
	etag := b.CreateString(part.ETag)
	raftpb.ForwardPartMetaStart(b)
	raftpb.ForwardPartMetaAddPartNumber(b, int32(part.PartNumber))
	raftpb.ForwardPartMetaAddEtag(b, etag)
	raftpb.ForwardPartMetaAddSize(b, part.Size)
	pOff := raftpb.ForwardPartMetaEnd(b)

	raftpb.ForwardReplyStart(b)
	raftpb.ForwardReplyAddStatus(b, raftpb.ForwardStatusOK)
	raftpb.ForwardReplyAddPart(b, pOff)
	b.Finish(raftpb.ForwardReplyEnd(b))
	return b.FinishedBytes()
}

func buildPartsReply(parts []storage.Part) []byte {
	b := flatbuffers.NewBuilder(64 + len(parts)*64)
	metas := make([]flatbuffers.UOffsetT, len(parts))
	for i, part := range parts {
		etag := b.CreateString(part.ETag)
		raftpb.ForwardPartMetaStart(b)
		raftpb.ForwardPartMetaAddPartNumber(b, int32(part.PartNumber))
		raftpb.ForwardPartMetaAddEtag(b, etag)
		raftpb.ForwardPartMetaAddSize(b, part.Size)
		metas[i] = raftpb.ForwardPartMetaEnd(b)
	}
	raftpb.ForwardReplyStartPartsVector(b, len(parts))
	for i := len(metas) - 1; i >= 0; i-- {
		b.PrependUOffsetT(metas[i])
	}
	partsVec := b.EndVector(len(parts))
	raftpb.ForwardReplyStart(b)
	raftpb.ForwardReplyAddStatus(b, raftpb.ForwardStatusOK)
	raftpb.ForwardReplyAddParts(b, partsVec)
	b.Finish(raftpb.ForwardReplyEnd(b))
	return b.FinishedBytes()
}

// buildOKReply is a status-only OK reply — DeleteObject, AbortMultipartUpload.
func buildOKReply() []byte { return buildSimpleReply(raftpb.ForwardStatusOK, "") }

// --- Reply parsers (request side) ---

// parseStatus returns the reply's status and converts non-OK statuses to a
// canonical error type that the storage layer understands. Returns nil on OK.
func parseReplyStatus(reply []byte) error {
	if len(reply) == 0 {
		return ErrNoReachablePeer
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	switch fr.Status() {
	case raftpb.ForwardStatusOK:
		return nil
	case raftpb.ForwardStatusNoSuchBucket:
		return storage.ErrNoSuchBucket
	case raftpb.ForwardStatusNoSuchKey:
		return storage.ErrObjectNotFound
	case raftpb.ForwardStatusMethodNotAllowed:
		return storage.ErrMethodNotAllowed
	case raftpb.ForwardStatusNoSuchUpload:
		return storage.ErrUploadNotFound
	case raftpb.ForwardStatusEntityTooLarge:
		return storage.ErrEntityTooLarge
	case raftpb.ForwardStatusAppendOffsetMismatch:
		return storage.ErrAppendOffsetMismatch
	case raftpb.ForwardStatusAppendNotSupported:
		return storage.ErrAppendNotSupported
	case raftpb.ForwardStatusAppendCapExceeded:
		return storage.ErrAppendCapExceeded
	case raftpb.ForwardStatusAppendObjectTooLarge:
		return storage.ErrAppendObjectTooLarge
	case raftpb.ForwardStatusInsufficientPlacementTargets:
		return &ErrInsufficientPlacementTargets{
			Operation:     "forwarded_write",
			FailureReason: "remote group reported insufficient placement targets",
		}
	case raftpb.ForwardStatusNotLeader:
		// Should not reach caller — ForwardSender retries on hint.
		return ErrNoReachablePeer
	case raftpb.ForwardStatusNotVoter:
		return ErrUnknownGroup
	default:
		return errInternalReply
	}
}

func readAtReplyInto(reply []byte, dst []byte) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			n = 0
			err = errInternalReply
		}
	}()
	if err := parseReplyStatus(reply); err != nil {
		return 0, err
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	body := fr.ReadBodyBytes()
	n = copy(dst, body)
	if n != len(body) {
		return n, ErrForwardBodySizeMismatch
	}
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}

// objectFromReply builds a *storage.Object from a ForwardReply with a populated
// ForwardObjectMeta — used by PutObject, GetObject, HeadObject.
func objectFromReply(reply []byte) (*storage.Object, error) {
	if err := parseReplyStatus(reply); err != nil {
		return nil, err
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	meta := fr.Object(nil)
	if meta == nil {
		return nil, errInternalReply
	}
	return &storage.Object{
		Key:          string(meta.Key()),
		Size:         meta.Size(),
		ETag:         string(meta.Etag()),
		ContentType:  string(meta.ContentType()),
		LastModified: meta.ModifiedUnixMs(),
		VersionID:    string(meta.VersionId()),
		Parts:        readPartsVector(meta),
		Tags:         readForwardTagsVector(meta.TagsLength(), meta.Tags),
	}, nil
}

// objectsFromReply unpacks ForwardReply.objects into []*storage.Object — used
// by ListObjects. WalkObjects callers iterate the same slice.
func objectsFromReply(reply []byte) ([]*storage.Object, error) {
	if err := parseReplyStatus(reply); err != nil {
		return nil, err
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	n := fr.ObjectsLength()
	out := make([]*storage.Object, 0, n)
	var meta raftpb.ForwardObjectMeta
	for i := 0; i < n; i++ {
		if !fr.Objects(&meta, i) {
			continue
		}
		out = append(out, &storage.Object{
			Key:          string(meta.Key()),
			Size:         meta.Size(),
			ETag:         string(meta.Etag()),
			ContentType:  string(meta.ContentType()),
			LastModified: meta.ModifiedUnixMs(),
			VersionID:    string(meta.VersionId()),
			Parts:        readPartsVector(&meta),
			Tags:         readForwardTagsVector(meta.TagsLength(), meta.Tags),
		})
	}
	return out, nil
}

func objectVersionsFromReply(reply []byte) ([]*storage.ObjectVersion, error) {
	if err := parseReplyStatus(reply); err != nil {
		return nil, err
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	n := fr.VersionsLength()
	out := make([]*storage.ObjectVersion, 0, n)
	var meta raftpb.ForwardObjectVersionMeta
	for i := 0; i < n; i++ {
		if !fr.Versions(&meta, i) {
			continue
		}
		out = append(out, &storage.ObjectVersion{
			Key:            string(meta.Key()),
			VersionID:      string(meta.VersionId()),
			IsLatest:       meta.IsLatest(),
			IsDeleteMarker: meta.IsDeleteMarker(),
			ETag:           string(meta.Etag()),
			Size:           meta.Size(),
			LastModified:   meta.ModifiedUnixMs(),
			Tags:           readForwardTagsVector(meta.TagsLength(), meta.Tags),
		})
	}
	return out, nil
}

// uploadFromReply for CreateMultipartUpload.
func uploadFromReply(reply []byte) (*storage.MultipartUpload, error) {
	if err := parseReplyStatus(reply); err != nil {
		return nil, err
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	up := fr.Upload(nil)
	if up == nil {
		return nil, errInternalReply
	}
	return &storage.MultipartUpload{
		UploadID: string(up.UploadId()),
		Bucket:   string(up.Bucket()),
		Key:      string(up.Key()),
	}, nil
}

// partFromReply for UploadPart.
func partFromReply(reply []byte) (*storage.Part, error) {
	if err := parseReplyStatus(reply); err != nil {
		return nil, err
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	p := fr.Part(nil)
	if p == nil {
		return nil, errInternalReply
	}
	return &storage.Part{
		PartNumber: int(p.PartNumber()),
		ETag:       string(p.Etag()),
		Size:       p.Size(),
	}, nil
}

// tagsFromReply unpacks ForwardReply.tags for GetObjectTags. Returns nil
// when the tags vector is absent or zero-length so callers preserve the
// "no tags" / "empty tag set" convention.
func tagsFromReply(reply []byte) ([]storage.Tag, error) {
	if err := parseReplyStatus(reply); err != nil {
		return nil, err
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	return readForwardTagsVector(fr.TagsLength(), fr.Tags), nil
}

func partsFromReply(reply []byte) ([]storage.Part, error) {
	if err := parseReplyStatus(reply); err != nil {
		return nil, err
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	n := fr.PartsLength()
	out := make([]storage.Part, 0, n)
	var p raftpb.ForwardPartMeta
	for i := 0; i < n; i++ {
		if !fr.Parts(&p, i) {
			continue
		}
		out = append(out, storage.Part{
			PartNumber: int(p.PartNumber()),
			ETag:       string(p.Etag()),
			Size:       p.Size(),
		})
	}
	return out, nil
}

func multipartUploadsFromReply(reply []byte) ([]*storage.MultipartUpload, error) {
	if err := parseReplyStatus(reply); err != nil {
		return nil, err
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	n := fr.UploadsLength()
	out := make([]*storage.MultipartUpload, 0, n)
	var upload raftpb.ForwardMultipartUploadMeta
	for i := 0; i < n; i++ {
		if !fr.Uploads(&upload, i) {
			continue
		}
		out = append(out, &storage.MultipartUpload{
			Bucket:      string(upload.Bucket()),
			Key:         string(upload.Key()),
			UploadID:    string(upload.UploadId()),
			ContentType: string(upload.ContentType()),
			CreatedAt:   upload.CreatedAt(),
		})
	}
	return out, nil
}
