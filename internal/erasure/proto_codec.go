package erasure

import (
	"github.com/gritive/GrainFS/internal/s3auth"
	pb "github.com/gritive/GrainFS/internal/erasure/erasurepb"
	"google.golang.org/protobuf/proto"
)

// fmtFlatBuffers is the 1-byte prefix that marks a FlatBuffers-encoded value.
//
// 0x01 cannot be the first byte of any valid proto3-encoded message:
// proto3 field tags encode as varint (field_number << 3 | wire_type).
// 0x01 = field 0, wire type 1 — illegal in proto3. Safe as a format sentinel.
const fmtFlatBuffers byte = 0x01

func isFlatBuffers(data []byte) bool {
	return len(data) > 0 && data[0] == fmtFlatBuffers
}

// marshalWithFBPrefix prepends fmtFlatBuffers to a FlatBuffers payload.
func marshalWithFBPrefix(fb []byte) []byte {
	out := make([]byte, 1+len(fb))
	out[0] = fmtFlatBuffers
	copy(out[1:], fb)
	return out
}

// stripFBPrefix removes the format prefix byte for FlatBuffers data,
// or returns data unchanged for legacy Protobuf data (no prefix).
func stripFBPrefix(data []byte) []byte {
	if isFlatBuffers(data) {
		return data[1:]
	}
	return data
}

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
	if isFlatBuffers(data) {
		return unmarshalBucketMetaFB(stripFBPrefix(data))
	}
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
		CreatedNano:    m.CreatedNano,
		Acl:            uint32(m.ACL),
	})
}

func unmarshalECObjectMeta(data []byte) (*ecObjectMeta, error) {
	if isFlatBuffers(data) {
		return unmarshalECObjectMetaFB(stripFBPrefix(data))
	}
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
		CreatedNano:    p.CreatedNano,
		ACL:            s3auth.ACLGrant(p.Acl), // proto3: missing field → 0 → ACLPrivate
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
	if isFlatBuffers(data) {
		return unmarshalECMultipartMetaFB(stripFBPrefix(data))
	}
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
