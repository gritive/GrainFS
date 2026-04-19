// Hand-written FlatBuffers accessor for ECObjectMeta.
// Mirrors what `flatc --go ECObjectMeta.fbs` would generate.
// Regenerate once flatc is available: make generate-fbs

package erasurepb

import flatbuffers "github.com/google/flatbuffers/go"

// ECObjectMetaFB is the FlatBuffers accessor for ECObjectMeta.
// Zero-copy: field reads return slices or primitives backed by the input buffer.
type ECObjectMetaFB struct {
	_tab flatbuffers.Table
}

func GetRootAsECObjectMetaFB(buf []byte, offset flatbuffers.UOffsetT) *ECObjectMetaFB {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &ECObjectMetaFB{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *ECObjectMetaFB) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

// Key returns the key bytes without allocation (zero-copy).
func (rcv *ECObjectMetaFB) Key() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *ECObjectMetaFB) Size() int64 {
	return rcv._tab.GetInt64Slot(6, 0)
}

func (rcv *ECObjectMetaFB) ContentType() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *ECObjectMetaFB) Etag() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *ECObjectMetaFB) LastModified() int64 {
	return rcv._tab.GetInt64Slot(12, 0)
}

func (rcv *ECObjectMetaFB) DataShards() int32 {
	return rcv._tab.GetInt32Slot(14, 0)
}

func (rcv *ECObjectMetaFB) ParityShards() int32 {
	return rcv._tab.GetInt32Slot(16, 0)
}

func (rcv *ECObjectMetaFB) ShardSize() int32 {
	return rcv._tab.GetInt32Slot(18, 0)
}

func (rcv *ECObjectMetaFB) VersionId() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(20))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *ECObjectMetaFB) IsDeleteMarker() bool {
	return rcv._tab.GetBoolSlot(22, false)
}

func (rcv *ECObjectMetaFB) CreatedNano() int64 {
	return rcv._tab.GetInt64Slot(24, 0)
}

func (rcv *ECObjectMetaFB) Acl() uint32 {
	return rcv._tab.GetUint32Slot(26, 0)
}

// Builder helpers (field ordering matches .fbs field indices).

func ECObjectMetaFBStart(b *flatbuffers.Builder)                                { b.StartObject(12) }
func ECObjectMetaFBAddKey(b *flatbuffers.Builder, key flatbuffers.UOffsetT)     { b.PrependUOffsetTSlot(0, key, 0) }
func ECObjectMetaFBAddSize(b *flatbuffers.Builder, size int64)                  { b.PrependInt64Slot(1, size, 0) }
func ECObjectMetaFBAddContentType(b *flatbuffers.Builder, ct flatbuffers.UOffsetT) { b.PrependUOffsetTSlot(2, ct, 0) }
func ECObjectMetaFBAddEtag(b *flatbuffers.Builder, etag flatbuffers.UOffsetT)   { b.PrependUOffsetTSlot(3, etag, 0) }
func ECObjectMetaFBAddLastModified(b *flatbuffers.Builder, v int64)             { b.PrependInt64Slot(4, v, 0) }
func ECObjectMetaFBAddDataShards(b *flatbuffers.Builder, v int32)               { b.PrependInt32Slot(5, v, 0) }
func ECObjectMetaFBAddParityShards(b *flatbuffers.Builder, v int32)             { b.PrependInt32Slot(6, v, 0) }
func ECObjectMetaFBAddShardSize(b *flatbuffers.Builder, v int32)                { b.PrependInt32Slot(7, v, 0) }
func ECObjectMetaFBAddVersionId(b *flatbuffers.Builder, v flatbuffers.UOffsetT) { b.PrependUOffsetTSlot(8, v, 0) }
func ECObjectMetaFBAddIsDeleteMarker(b *flatbuffers.Builder, v bool)            { b.PrependBoolSlot(9, v, false) }
func ECObjectMetaFBAddCreatedNano(b *flatbuffers.Builder, v int64)              { b.PrependInt64Slot(10, v, 0) }
func ECObjectMetaFBAddAcl(b *flatbuffers.Builder, v uint32)                     { b.PrependUint32Slot(11, v, 0) }
func ECObjectMetaFBEnd(b *flatbuffers.Builder) flatbuffers.UOffsetT             { return b.EndObject() }

// BucketMetaFB is the FlatBuffers accessor for BucketMeta.
type BucketMetaFB struct {
	_tab flatbuffers.Table
}

func GetRootAsBucketMetaFB(buf []byte, offset flatbuffers.UOffsetT) *BucketMetaFB {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &BucketMetaFB{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *BucketMetaFB) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *BucketMetaFB) EcEnabled() bool {
	return rcv._tab.GetBoolSlot(4, false)
}

func (rcv *BucketMetaFB) VersioningState() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func BucketMetaFBStart(b *flatbuffers.Builder)                                         { b.StartObject(2) }
func BucketMetaFBAddEcEnabled(b *flatbuffers.Builder, v bool)                          { b.PrependBoolSlot(0, v, false) }
func BucketMetaFBAddVersioningState(b *flatbuffers.Builder, v flatbuffers.UOffsetT)    { b.PrependUOffsetTSlot(1, v, 0) }
func BucketMetaFBEnd(b *flatbuffers.Builder) flatbuffers.UOffsetT                      { return b.EndObject() }

// MultipartUploadMetaFB is the FlatBuffers accessor for MultipartUploadMeta.
type MultipartUploadMetaFB struct {
	_tab flatbuffers.Table
}

func GetRootAsMultipartUploadMetaFB(buf []byte, offset flatbuffers.UOffsetT) *MultipartUploadMetaFB {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &MultipartUploadMetaFB{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *MultipartUploadMetaFB) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *MultipartUploadMetaFB) UploadId() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *MultipartUploadMetaFB) Bucket() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *MultipartUploadMetaFB) Key() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *MultipartUploadMetaFB) ContentType() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *MultipartUploadMetaFB) CreatedAt() int64 {
	return rcv._tab.GetInt64Slot(12, 0)
}

func MultipartUploadMetaFBStart(b *flatbuffers.Builder)                                       { b.StartObject(5) }
func MultipartUploadMetaFBAddUploadId(b *flatbuffers.Builder, v flatbuffers.UOffsetT)         { b.PrependUOffsetTSlot(0, v, 0) }
func MultipartUploadMetaFBAddBucket(b *flatbuffers.Builder, v flatbuffers.UOffsetT)           { b.PrependUOffsetTSlot(1, v, 0) }
func MultipartUploadMetaFBAddKey(b *flatbuffers.Builder, v flatbuffers.UOffsetT)              { b.PrependUOffsetTSlot(2, v, 0) }
func MultipartUploadMetaFBAddContentType(b *flatbuffers.Builder, v flatbuffers.UOffsetT)      { b.PrependUOffsetTSlot(3, v, 0) }
func MultipartUploadMetaFBAddCreatedAt(b *flatbuffers.Builder, v int64)                       { b.PrependInt64Slot(4, v, 0) }
func MultipartUploadMetaFBEnd(b *flatbuffers.Builder) flatbuffers.UOffsetT                    { return b.EndObject() }
