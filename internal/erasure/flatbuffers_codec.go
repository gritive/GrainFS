package erasure

import (
	"fmt"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/s3auth"
	fb "github.com/gritive/GrainFS/internal/erasure/erasurepb"
)

// ecMultipartMeta holds multipart upload metadata for EC backend.
type ecMultipartMeta struct {
	UploadID    string
	Bucket      string
	Key         string
	ContentType string
	CreatedAt   int64
}

func fbRecover(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("invalid flatbuffer: %v", r)
	}
}

// builderPool reuses FlatBuffers builders across marshal calls to reduce allocations.
var builderPool = sync.Pool{
	New: func() any { return flatbuffers.NewBuilder(256) },
}

func marshalECObjectMeta(m *ecObjectMeta) ([]byte, error) {
	b := builderPool.Get().(*flatbuffers.Builder)
	defer func() {
		b.Reset()
		builderPool.Put(b)
	}()

	keyOff := b.CreateString(m.Key)
	ctOff := b.CreateString(m.ContentType)
	etagOff := b.CreateString(m.ETag)
	vidOff := b.CreateString(m.VersionID)

	fb.ECObjectMetaStart(b)
	fb.ECObjectMetaAddKey(b, keyOff)
	fb.ECObjectMetaAddSize(b, m.Size)
	fb.ECObjectMetaAddContentType(b, ctOff)
	fb.ECObjectMetaAddEtag(b, etagOff)
	fb.ECObjectMetaAddLastModified(b, m.LastModified)
	fb.ECObjectMetaAddDataShards(b, int32(m.DataShards))
	fb.ECObjectMetaAddParityShards(b, int32(m.ParityShards))
	fb.ECObjectMetaAddShardSize(b, int32(m.ShardSize))
	fb.ECObjectMetaAddVersionId(b, vidOff)
	fb.ECObjectMetaAddIsDeleteMarker(b, m.IsDeleteMarker)
	fb.ECObjectMetaAddCreatedNano(b, m.CreatedNano)
	fb.ECObjectMetaAddAcl(b, uint32(m.ACL))
	root := fb.ECObjectMetaEnd(b)
	b.Finish(root)

	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

func unmarshalECObjectMeta(data []byte) (result *ecObjectMeta, err error) {
	defer fbRecover(&err)
	t := fb.GetRootAsECObjectMeta(data, 0)
	return &ecObjectMeta{
		Key:            string(t.Key()),
		Size:           t.Size(),
		ContentType:    string(t.ContentType()),
		ETag:           string(t.Etag()),
		LastModified:   t.LastModified(),
		DataShards:     int(t.DataShards()),
		ParityShards:   int(t.ParityShards()),
		ShardSize:      int(t.ShardSize()),
		VersionID:      string(t.VersionId()),
		IsDeleteMarker: t.IsDeleteMarker(),
		CreatedNano:    t.CreatedNano(),
		ACL:            s3auth.ACLGrant(t.Acl()),
	}, nil
}

func marshalBucketMeta(m *bucketMeta) ([]byte, error) {
	b := builderPool.Get().(*flatbuffers.Builder)
	defer func() {
		b.Reset()
		builderPool.Put(b)
	}()

	vsOff := b.CreateString(m.VersioningState)

	fb.BucketMetaStart(b)
	fb.BucketMetaAddEcEnabled(b, m.ECEnabled)
	fb.BucketMetaAddVersioningState(b, vsOff)
	root := fb.BucketMetaEnd(b)
	b.Finish(root)

	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

func unmarshalBucketMeta(data []byte) (result *bucketMeta, err error) {
	defer fbRecover(&err)
	t := fb.GetRootAsBucketMeta(data, 0)
	state := string(t.VersioningState())
	if state == "" {
		state = "Unversioned"
	}
	return &bucketMeta{ECEnabled: t.EcEnabled(), VersioningState: state}, nil
}

func marshalECMultipartMeta(m *ecMultipartMeta) ([]byte, error) {
	b := builderPool.Get().(*flatbuffers.Builder)
	defer func() {
		b.Reset()
		builderPool.Put(b)
	}()

	uploadIDOff := b.CreateString(m.UploadID)
	bucketOff := b.CreateString(m.Bucket)
	keyOff := b.CreateString(m.Key)
	ctOff := b.CreateString(m.ContentType)

	fb.MultipartUploadMetaStart(b)
	fb.MultipartUploadMetaAddUploadId(b, uploadIDOff)
	fb.MultipartUploadMetaAddBucket(b, bucketOff)
	fb.MultipartUploadMetaAddKey(b, keyOff)
	fb.MultipartUploadMetaAddContentType(b, ctOff)
	fb.MultipartUploadMetaAddCreatedAt(b, m.CreatedAt)
	root := fb.MultipartUploadMetaEnd(b)
	b.Finish(root)

	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

func unmarshalECMultipartMeta(data []byte) (result *ecMultipartMeta, err error) {
	defer fbRecover(&err)
	t := fb.GetRootAsMultipartUploadMeta(data, 0)
	return &ecMultipartMeta{
		UploadID:    string(t.UploadId()),
		Bucket:      string(t.Bucket()),
		Key:         string(t.Key()),
		ContentType: string(t.ContentType()),
		CreatedAt:   t.CreatedAt(),
	}, nil
}
