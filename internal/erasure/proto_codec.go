package erasure

import (
	pb "github.com/gritive/GrainFS/internal/erasure/erasurepb"
	"google.golang.org/protobuf/proto"
)

// ecMultipartMeta holds multipart upload metadata for EC backend.
type ecMultipartMeta struct {
	UploadID    string
	Bucket      string
	Key         string
	ContentType string
	CreatedAt   int64
}

func marshalBucketMeta(m *bucketMeta) ([]byte, error) {
	return proto.Marshal(&pb.BucketMeta{
		EcEnabled:       m.ECEnabled,
		VersioningState: m.VersioningState,
	})
}

func unmarshalBucketMeta(data []byte) (*bucketMeta, error) {
	var p pb.BucketMeta
	if err := proto.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	state := p.VersioningState
	if state == "" {
		state = "Unversioned"
	}
	return &bucketMeta{ECEnabled: p.EcEnabled, VersioningState: state}, nil
}

func marshalECObjectMeta(m *ecObjectMeta) ([]byte, error) {
	return proto.Marshal(&pb.ECObjectMeta{
		Key:            m.Key,
		Size:           m.Size,
		ContentType:    m.ContentType,
		Etag:           m.ETag,
		LastModified:   m.LastModified,
		DataShards:     int32(m.DataShards),
		ParityShards:   int32(m.ParityShards),
		ShardSize:      int32(m.ShardSize),
		VersionId:      m.VersionID,
		IsDeleteMarker: m.IsDeleteMarker,
	})
}

func unmarshalECObjectMeta(data []byte) (*ecObjectMeta, error) {
	var p pb.ECObjectMeta
	if err := proto.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &ecObjectMeta{
		Key:            p.Key,
		Size:           p.Size,
		ContentType:    p.ContentType,
		ETag:           p.Etag,
		LastModified:   p.LastModified,
		DataShards:     int(p.DataShards),
		ParityShards:   int(p.ParityShards),
		ShardSize:      int(p.ShardSize),
		VersionID:      p.VersionId,
		IsDeleteMarker: p.IsDeleteMarker,
	}, nil
}

func marshalECMultipartMeta(m *ecMultipartMeta) ([]byte, error) {
	return proto.Marshal(&pb.MultipartUploadMeta{
		UploadId:    m.UploadID,
		Bucket:      m.Bucket,
		Key:         m.Key,
		ContentType: m.ContentType,
		CreatedAt:   m.CreatedAt,
	})
}

func unmarshalECMultipartMeta(data []byte) (*ecMultipartMeta, error) {
	var p pb.MultipartUploadMeta
	if err := proto.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &ecMultipartMeta{
		UploadID:    p.UploadId,
		Bucket:      p.Bucket,
		Key:         p.Key,
		ContentType: p.ContentType,
		CreatedAt:   p.CreatedAt,
	}, nil
}
