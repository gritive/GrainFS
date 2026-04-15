package storage

import (
	pb "github.com/gritive/GrainFS/internal/storage/storagepb"
	"google.golang.org/protobuf/proto"
)

func marshalObject(obj *Object) ([]byte, error) {
	return proto.Marshal(&pb.Object{
		Key:          obj.Key,
		Size:         obj.Size,
		ContentType:  obj.ContentType,
		Etag:         obj.ETag,
		LastModified: obj.LastModified,
	})
}

func unmarshalObject(data []byte) (*Object, error) {
	var p pb.Object
	if err := proto.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &Object{
		Key:          p.Key,
		Size:         p.Size,
		ContentType:  p.ContentType,
		ETag:         p.Etag,
		LastModified: p.LastModified,
	}, nil
}

func marshalMultipartMeta(m *multipartMeta) ([]byte, error) {
	return proto.Marshal(&pb.MultipartMeta{
		UploadId:    m.UploadID,
		Bucket:      m.Bucket,
		Key:         m.Key,
		ContentType: m.ContentType,
		CreatedAt:   m.CreatedAt,
	})
}

func unmarshalMultipartMeta(data []byte) (*multipartMeta, error) {
	var p pb.MultipartMeta
	if err := proto.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &multipartMeta{
		UploadID:    p.UploadId,
		Bucket:      p.Bucket,
		Key:         p.Key,
		ContentType: p.ContentType,
		CreatedAt:   p.CreatedAt,
	}, nil
}
