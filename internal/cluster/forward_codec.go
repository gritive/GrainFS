package cluster

import (
	"errors"

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
	b := flatbuffers.NewBuilder(64 + len(body))
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

func buildListObjectsArgs(bucket, prefix string, maxKeys int32) []byte {
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	pf := b.CreateString(prefix)
	raftpb.ListObjectsArgsStart(b)
	raftpb.ListObjectsArgsAddBucket(b, bk)
	raftpb.ListObjectsArgsAddPrefix(b, pf)
	raftpb.ListObjectsArgsAddMaxKeys(b, maxKeys)
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

func buildCreateMultipartUploadArgs(bucket, key, contentType string) []byte {
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	ct := b.CreateString(contentType)
	raftpb.CreateMultipartUploadArgsStart(b)
	raftpb.CreateMultipartUploadArgsAddBucket(b, bk)
	raftpb.CreateMultipartUploadArgsAddKey(b, k)
	raftpb.CreateMultipartUploadArgsAddContentType(b, ct)
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

// --- Reply builders (response side — used by ForwardReceiver in T8 + tests) ---

// buildObjectReply builds an OK reply with a populated ForwardObjectMeta —
// used by PutObject and HeadObject (no body).
func buildObjectReply(obj *storage.Object, bucket string) []byte {
	b := flatbuffers.NewBuilder(128)
	bk := b.CreateString(bucket)
	k := b.CreateString(obj.Key)
	etag := b.CreateString(obj.ETag)
	ct := b.CreateString(obj.ContentType)
	vid := b.CreateString(obj.VersionID)
	raftpb.ForwardObjectMetaStart(b)
	raftpb.ForwardObjectMetaAddBucket(b, bk)
	raftpb.ForwardObjectMetaAddKey(b, k)
	raftpb.ForwardObjectMetaAddSize(b, obj.Size)
	raftpb.ForwardObjectMetaAddEtag(b, etag)
	raftpb.ForwardObjectMetaAddContentType(b, ct)
	raftpb.ForwardObjectMetaAddModifiedUnixMs(b, obj.LastModified)
	raftpb.ForwardObjectMetaAddVersionId(b, vid)
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

	raftpb.ForwardObjectMetaStart(b)
	raftpb.ForwardObjectMetaAddBucket(b, bk)
	raftpb.ForwardObjectMetaAddKey(b, k)
	raftpb.ForwardObjectMetaAddSize(b, obj.Size)
	raftpb.ForwardObjectMetaAddEtag(b, etag)
	raftpb.ForwardObjectMetaAddContentType(b, ct)
	raftpb.ForwardObjectMetaAddModifiedUnixMs(b, obj.LastModified)
	raftpb.ForwardObjectMetaAddVersionId(b, vid)
	metaOff := raftpb.ForwardObjectMetaEnd(b)

	raftpb.ForwardReplyStart(b)
	raftpb.ForwardReplyAddStatus(b, raftpb.ForwardStatusOK)
	raftpb.ForwardReplyAddObject(b, metaOff)
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
		raftpb.ForwardObjectMetaStart(b)
		raftpb.ForwardObjectMetaAddBucket(b, bk)
		raftpb.ForwardObjectMetaAddKey(b, k)
		raftpb.ForwardObjectMetaAddSize(b, o.Size)
		raftpb.ForwardObjectMetaAddEtag(b, etag)
		raftpb.ForwardObjectMetaAddContentType(b, ct)
		raftpb.ForwardObjectMetaAddModifiedUnixMs(b, o.LastModified)
		raftpb.ForwardObjectMetaAddVersionId(b, vid)
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
		raftpb.ForwardObjectVersionMetaStart(b)
		raftpb.ForwardObjectVersionMetaAddKey(b, k)
		raftpb.ForwardObjectVersionMetaAddVersionId(b, vid)
		raftpb.ForwardObjectVersionMetaAddIsLatest(b, v.IsLatest)
		raftpb.ForwardObjectVersionMetaAddIsDeleteMarker(b, v.IsDeleteMarker)
		raftpb.ForwardObjectVersionMetaAddEtag(b, etag)
		raftpb.ForwardObjectVersionMetaAddSize(b, v.Size)
		raftpb.ForwardObjectVersionMetaAddModifiedUnixMs(b, v.LastModified)
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
	case raftpb.ForwardStatusEntityTooLarge:
		return storage.ErrEntityTooLarge
	case raftpb.ForwardStatusNotLeader:
		// Should not reach caller — ForwardSender retries on hint.
		return ErrNoReachablePeer
	case raftpb.ForwardStatusNotVoter:
		return ErrUnknownGroup
	default:
		return errInternalReply
	}
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
