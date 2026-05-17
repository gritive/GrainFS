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
	root := storagepb.ObjectEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	b.Reset()
	storageBuilderPool.Put(b)
	return out, nil
}

func unmarshalObject(data []byte) (*Object, error) {
	obj := new(Object)
	if err := unmarshalObjectInto(data, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// unmarshalObjectInto decodes data directly into dst, skipping the inner
// `&Object{...}` allocation that unmarshalObject performs. Hot read paths
// (HeadObject, WalkObjects, ListObjects) already keep an Object on the
// heap because they return it; pointing them at the destination directly
// avoids one extra allocation per decode. Same recover/error semantics as
// unmarshalObject.
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
