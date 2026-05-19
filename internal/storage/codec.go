package storage

import (
	"fmt"
	"sort"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/storage/storagepb"
)

var storageBuilderPool = pool.New(func() *flatbuffers.Builder { return flatbuffers.NewBuilder(256) })

func marshalObject(obj *Object) ([]byte, error) {
	b := storageBuilderPool.Get()
	keyOff := b.CreateString(obj.Key)
	ctOff := b.CreateString(obj.ContentType)
	etagOff := b.CreateString(obj.ETag)
	metadataOff := buildUserMetadataVector(b, obj.UserMetadata)
	var sseOff flatbuffers.UOffsetT
	if obj.SSEAlgorithm != "" {
		sseOff = b.CreateString(obj.SSEAlgorithm)
	}
	var segmentsOff flatbuffers.UOffsetT
	if len(obj.Segments) > 0 {
		segOffs := make([]flatbuffers.UOffsetT, len(obj.Segments))
		for i := len(obj.Segments) - 1; i >= 0; i-- {
			s := obj.Segments[i]
			blobOff := b.CreateString(s.BlobID)
			var checksumOff flatbuffers.UOffsetT
			if len(s.Checksum) > 0 {
				checksumOff = b.CreateByteVector(s.Checksum)
			}
			var pgOff flatbuffers.UOffsetT
			if s.PlacementGroupID != "" {
				pgOff = b.CreateString(s.PlacementGroupID)
			}
			storagepb.SegmentRefStart(b)
			storagepb.SegmentRefAddBlobId(b, blobOff)
			storagepb.SegmentRefAddSize(b, s.Size)
			if checksumOff != 0 {
				storagepb.SegmentRefAddChecksum(b, checksumOff)
			}
			if pgOff != 0 {
				storagepb.SegmentRefAddPlacementGroupId(b, pgOff)
			}
			if s.ShardSize != 0 {
				storagepb.SegmentRefAddShardSize(b, s.ShardSize)
			}
			segOffs[i] = storagepb.SegmentRefEnd(b)
		}
		storagepb.ObjectStartSegmentsVector(b, len(segOffs))
		for i := len(segOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(segOffs[i])
		}
		segmentsOff = b.EndVector(len(segOffs))
	}
	var partsOff flatbuffers.UOffsetT
	if len(obj.Parts) > 0 {
		partOffs := make([]flatbuffers.UOffsetT, len(obj.Parts))
		for i := len(obj.Parts) - 1; i >= 0; i-- {
			p := obj.Parts[i]
			etOff := b.CreateString(p.ETag)
			storagepb.MultipartPartEntryStart(b)
			storagepb.MultipartPartEntryAddPartNumber(b, int32(p.PartNumber))
			storagepb.MultipartPartEntryAddSize(b, p.Size)
			storagepb.MultipartPartEntryAddEtag(b, etOff)
			partOffs[i] = storagepb.MultipartPartEntryEnd(b)
		}
		storagepb.ObjectStartPartsVector(b, len(partOffs))
		for i := len(partOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(partOffs[i])
		}
		partsOff = b.EndVector(len(partOffs))
	}
	var appendMD5sOff flatbuffers.UOffsetT
	if len(obj.AppendCallMD5s) > 0 {
		md5Offs := make([]flatbuffers.UOffsetT, len(obj.AppendCallMD5s))
		for i := len(obj.AppendCallMD5s) - 1; i >= 0; i-- {
			vOff := b.CreateByteVector(obj.AppendCallMD5s[i])
			storagepb.BytesValueStart(b)
			storagepb.BytesValueAddV(b, vOff)
			md5Offs[i] = storagepb.BytesValueEnd(b)
		}
		storagepb.ObjectStartAppendCallMd5sVector(b, len(md5Offs))
		for i := len(md5Offs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(md5Offs[i])
		}
		appendMD5sOff = b.EndVector(len(md5Offs))
	}
	var tagsOff flatbuffers.UOffsetT
	if len(obj.Tags) > 0 {
		tagOffs := make([]flatbuffers.UOffsetT, len(obj.Tags))
		for i, t := range obj.Tags {
			kOff := b.CreateString(t.Key)
			vOff := b.CreateString(t.Value)
			storagepb.TagStart(b)
			storagepb.TagAddKey(b, kOff)
			storagepb.TagAddValue(b, vOff)
			tagOffs[i] = storagepb.TagEnd(b)
		}
		storagepb.ObjectStartTagsVector(b, len(tagOffs))
		for i := len(tagOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(tagOffs[i])
		}
		tagsOff = b.EndVector(len(tagOffs))
	}
	storagepb.ObjectStart(b)
	storagepb.ObjectAddKey(b, keyOff)
	storagepb.ObjectAddSize(b, obj.Size)
	storagepb.ObjectAddContentType(b, ctOff)
	storagepb.ObjectAddEtag(b, etagOff)
	storagepb.ObjectAddLastModified(b, obj.LastModified)
	storagepb.ObjectAddAcl(b, obj.ACL)
	if metadataOff != 0 {
		storagepb.ObjectAddUserMetadata(b, metadataOff)
	}
	if sseOff != 0 {
		storagepb.ObjectAddSseAlgorithm(b, sseOff)
	}
	if segmentsOff != 0 {
		storagepb.ObjectAddSegments(b, segmentsOff)
	}
	if obj.IsAppendable {
		storagepb.ObjectAddIsAppendable(b, true)
	}
	if partsOff != 0 {
		storagepb.ObjectAddParts(b, partsOff)
	}
	if appendMD5sOff != 0 {
		storagepb.ObjectAddAppendCallMd5s(b, appendMD5sOff)
	}
	if tagsOff != 0 {
		storagepb.ObjectAddTags(b, tagsOff)
	}
	root := storagepb.ObjectEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	b.Reset()
	storageBuilderPool.Put(b)
	return out, nil
}

// unmarshalObjectInto decodes data directly into dst, growing nothing.
// Hot read paths (HeadObject, WalkObjects, ListObjects) already keep an
// Object on the heap because they return it; pointing them at the
// destination directly avoids one extra allocation per decode.
func unmarshalObjectInto(data []byte, dst *Object) (err error) {
	if len(data) == 0 {
		return fmt.Errorf("unmarshal object: empty data")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unmarshal object: invalid flatbuffer: %v", r)
		}
	}()
	t := storagepb.GetRootAsObject(data, 0)
	*dst = Object{
		Key:          string(t.Key()),
		Size:         t.Size(),
		ContentType:  string(t.ContentType()),
		ETag:         string(t.Etag()),
		LastModified: t.LastModified(),
		ACL:          t.Acl(),
		UserMetadata: readUserMetadata(t.UserMetadataLength(), t.UserMetadata),
		SSEAlgorithm: string(t.SseAlgorithm()),
	}
	if n := t.SegmentsLength(); n > 0 {
		segs := make([]SegmentRef, n)
		var seg storagepb.SegmentRef
		for i := 0; i < n; i++ {
			if !t.Segments(&seg, i) {
				continue
			}
			var checksum []byte
			if cb := seg.ChecksumBytes(); len(cb) > 0 {
				checksum = append([]byte(nil), cb...)
			}
			segs[i] = SegmentRef{
				BlobID:           string(seg.BlobId()),
				Size:             seg.Size(),
				Checksum:         checksum,
				PlacementGroupID: string(seg.PlacementGroupId()),
				ShardSize:        seg.ShardSize(),
			}
		}
		dst.Segments = segs
	}
	if n := t.PartsLength(); n > 0 {
		parts := make([]MultipartPartEntry, n)
		var p storagepb.MultipartPartEntry
		for i := 0; i < n; i++ {
			if !t.Parts(&p, i) {
				continue
			}
			parts[i] = MultipartPartEntry{
				PartNumber: int(p.PartNumber()),
				Size:       p.Size(),
				ETag:       string(p.Etag()),
			}
		}
		dst.Parts = parts
	}
	dst.IsAppendable = t.IsAppendable()
	if n := t.AppendCallMd5sLength(); n > 0 {
		md5s := make([][]byte, n)
		var bv storagepb.BytesValue
		for i := 0; i < n; i++ {
			if !t.AppendCallMd5s(&bv, i) {
				continue
			}
			if v := bv.VBytes(); len(v) > 0 {
				md5s[i] = append([]byte(nil), v...)
			}
		}
		dst.AppendCallMD5s = md5s
	}
	if n := t.TagsLength(); n > 0 {
		tags := make([]Tag, n)
		var tag storagepb.Tag
		for i := 0; i < n; i++ {
			if t.Tags(&tag, i) {
				tags[i] = Tag{Key: string(tag.Key()), Value: string(tag.Value())}
			}
		}
		dst.Tags = tags
	}
	return nil
}

func buildUserMetadataVector(b *flatbuffers.Builder, metadata map[string]string) flatbuffers.UOffsetT {
	if len(metadata) == 0 {
		return 0
	}
	keys := make([]string, 0, len(metadata))
	for k := range metadata {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	offsets := make([]flatbuffers.UOffsetT, len(keys))
	for i, key := range keys {
		keyOff := b.CreateString(key)
		valueOff := b.CreateString(metadata[key])
		storagepb.UserMetadataStart(b)
		storagepb.UserMetadataAddKey(b, keyOff)
		storagepb.UserMetadataAddValue(b, valueOff)
		offsets[i] = storagepb.UserMetadataEnd(b)
	}
	storagepb.ObjectStartUserMetadataVector(b, len(offsets))
	for i := len(offsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(len(offsets))
}

func readUserMetadata(n int, at func(*storagepb.UserMetadata, int) bool) map[string]string {
	if n == 0 {
		return nil
	}
	out := make(map[string]string, n)
	var kv storagepb.UserMetadata
	for i := 0; i < n; i++ {
		if !at(&kv, i) {
			continue
		}
		out[string(kv.Key())] = string(kv.Value())
	}
	return out
}

func marshalMultipartMeta(m *multipartMeta) ([]byte, error) {
	b := storageBuilderPool.Get()
	uidOff := b.CreateString(m.UploadID)
	bucketOff := b.CreateString(m.Bucket)
	keyOff := b.CreateString(m.Key)
	ctOff := b.CreateString(m.ContentType)
	storagepb.MultipartMetaStart(b)
	storagepb.MultipartMetaAddUploadId(b, uidOff)
	storagepb.MultipartMetaAddBucket(b, bucketOff)
	storagepb.MultipartMetaAddKey(b, keyOff)
	storagepb.MultipartMetaAddContentType(b, ctOff)
	storagepb.MultipartMetaAddCreatedAt(b, m.CreatedAt)
	root := storagepb.MultipartMetaEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	b.Reset()
	storageBuilderPool.Put(b)
	return out, nil
}

func unmarshalMultipartMeta(data []byte) (m *multipartMeta, err error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("unmarshal multipart meta: empty data")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unmarshal multipart meta: invalid flatbuffer: %v", r)
		}
	}()
	t := storagepb.GetRootAsMultipartMeta(data, 0)
	return &multipartMeta{
		UploadID:    string(t.UploadId()),
		Bucket:      string(t.Bucket()),
		Key:         string(t.Key()),
		ContentType: string(t.ContentType()),
		CreatedAt:   t.CreatedAt(),
	}, nil
}
