package storage

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/storage/storagepb"
)

func marshalObject(obj *Object) ([]byte, error) {
	b := flatbuffers.NewBuilder(128)
	keyOff := b.CreateString(obj.Key)
	ctOff := b.CreateString(obj.ContentType)
	etagOff := b.CreateString(obj.ETag)
	storagepb.ObjectStart(b)
	storagepb.ObjectAddKey(b, keyOff)
	storagepb.ObjectAddSize(b, obj.Size)
	storagepb.ObjectAddContentType(b, ctOff)
	storagepb.ObjectAddEtag(b, etagOff)
	storagepb.ObjectAddLastModified(b, obj.LastModified)
	storagepb.ObjectAddAcl(b, obj.ACL)
	root := storagepb.ObjectEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

func unmarshalObject(data []byte) (obj *Object, err error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("unmarshal object: empty data")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unmarshal object: invalid flatbuffer: %v", r)
		}
	}()
	t := storagepb.GetRootAsObject(data, 0)
	return &Object{
		Key:          string(t.Key()),
		Size:         t.Size(),
		ContentType:  string(t.ContentType()),
		ETag:         string(t.Etag()),
		LastModified: t.LastModified(),
		ACL:          t.Acl(),
	}, nil
}

func marshalMultipartMeta(m *multipartMeta) ([]byte, error) {
	b := flatbuffers.NewBuilder(128)
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
