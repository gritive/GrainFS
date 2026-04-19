package erasure

import (
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/s3auth"
	fb "github.com/gritive/GrainFS/internal/erasure/erasurepb"
)

// builderPool reuses FlatBuffers builders across marshal calls to reduce allocations.
var builderPool = sync.Pool{
	New: func() any { return flatbuffers.NewBuilder(256) },
}

// marshalECObjectMetaFB serializes m to FlatBuffers format.
// Borrows a Builder from the pool; the returned slice is a fresh copy safe for storage.
func marshalECObjectMetaFB(m *ecObjectMeta) []byte {
	b := builderPool.Get().(*flatbuffers.Builder)
	defer func() {
		b.Reset()
		builderPool.Put(b)
	}()

	// Strings must be written before StartObject.
	keyOff := b.CreateString(m.Key)
	ctOff := b.CreateString(m.ContentType)
	etagOff := b.CreateString(m.ETag)
	vidOff := b.CreateString(m.VersionID)

	fb.ECObjectMetaFBStart(b)
	fb.ECObjectMetaFBAddKey(b, keyOff)
	fb.ECObjectMetaFBAddSize(b, m.Size)
	fb.ECObjectMetaFBAddContentType(b, ctOff)
	fb.ECObjectMetaFBAddEtag(b, etagOff)
	fb.ECObjectMetaFBAddLastModified(b, m.LastModified)
	fb.ECObjectMetaFBAddDataShards(b, int32(m.DataShards))
	fb.ECObjectMetaFBAddParityShards(b, int32(m.ParityShards))
	fb.ECObjectMetaFBAddShardSize(b, int32(m.ShardSize))
	fb.ECObjectMetaFBAddVersionId(b, vidOff)
	fb.ECObjectMetaFBAddIsDeleteMarker(b, m.IsDeleteMarker)
	fb.ECObjectMetaFBAddCreatedNano(b, m.CreatedNano)
	fb.ECObjectMetaFBAddAcl(b, uint32(m.ACL))
	root := fb.ECObjectMetaFBEnd(b)
	b.Finish(root)

	// FinishedBytes points into the builder's internal buffer (reset on pool return).
	// Copy to a stable slice before returning.
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

// unmarshalECObjectMetaFB deserializes a FlatBuffers-encoded ECObjectMeta.
// String fields are converted from the zero-copy []byte to Go strings here,
// keeping the call signature identical to unmarshalECObjectMeta.
func unmarshalECObjectMetaFB(data []byte) (*ecObjectMeta, error) {
	t := fb.GetRootAsECObjectMetaFB(data, 0)
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

func unmarshalBucketMetaFB(data []byte) (*bucketMeta, error) {
	t := fb.GetRootAsBucketMetaFB(data, 0)
	state := string(t.VersioningState())
	if state == "" {
		state = "Unversioned"
	}
	return &bucketMeta{ECEnabled: t.EcEnabled(), VersioningState: state}, nil
}

func unmarshalECMultipartMetaFB(data []byte) (*ecMultipartMeta, error) {
	t := fb.GetRootAsMultipartUploadMetaFB(data, 0)
	return &ecMultipartMeta{
		UploadID:    string(t.UploadId()),
		Bucket:      string(t.Bucket()),
		Key:         string(t.Key()),
		ContentType: string(t.ContentType()),
		CreatedAt:   t.CreatedAt(),
	}, nil
}
