// Hand-written FlatBuffers accessors for RPCMessage and ShardRequest.
// Mirrors what `flatc --go shard.fbs` would generate.

package raftpb

import flatbuffers "github.com/google/flatbuffers/go"

// RPCMessageFB is the FlatBuffers accessor for RPCMessage.
// Fields: type:string (0), data:[ubyte] (1)
type RPCMessageFB struct {
	_tab flatbuffers.Table
}

func GetRootAsRPCMessageFB(buf []byte, offset flatbuffers.UOffsetT) *RPCMessageFB {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &RPCMessageFB{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *RPCMessageFB) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *RPCMessageFB) Type() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *RPCMessageFB) Data() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func RPCMessageFBStart(b *flatbuffers.Builder)                                    { b.StartObject(2) }
func RPCMessageFBAddType(b *flatbuffers.Builder, v flatbuffers.UOffsetT)          { b.PrependUOffsetTSlot(0, v, 0) }
func RPCMessageFBAddData(b *flatbuffers.Builder, v flatbuffers.UOffsetT)          { b.PrependUOffsetTSlot(1, v, 0) }
func RPCMessageFBEnd(b *flatbuffers.Builder) flatbuffers.UOffsetT                 { return b.EndObject() }

// ShardRequestFB is the FlatBuffers accessor for ShardRequest.
// Fields: bucket:string (0), key:string (1), shard_idx:int32 (2), data:[ubyte] (3)
type ShardRequestFB struct {
	_tab flatbuffers.Table
}

func GetRootAsShardRequestFB(buf []byte, offset flatbuffers.UOffsetT) *ShardRequestFB {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &ShardRequestFB{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *ShardRequestFB) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *ShardRequestFB) Bucket() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *ShardRequestFB) Key() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *ShardRequestFB) ShardIdx() int32 {
	return rcv._tab.GetInt32Slot(8, 0)
}

func (rcv *ShardRequestFB) Data() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func ShardRequestFBStart(b *flatbuffers.Builder)                                   { b.StartObject(4) }
func ShardRequestFBAddBucket(b *flatbuffers.Builder, v flatbuffers.UOffsetT)       { b.PrependUOffsetTSlot(0, v, 0) }
func ShardRequestFBAddKey(b *flatbuffers.Builder, v flatbuffers.UOffsetT)          { b.PrependUOffsetTSlot(1, v, 0) }
func ShardRequestFBAddShardIdx(b *flatbuffers.Builder, v int32)                    { b.PrependInt32Slot(2, v, 0) }
func ShardRequestFBAddData(b *flatbuffers.Builder, v flatbuffers.UOffsetT)         { b.PrependUOffsetTSlot(3, v, 0) }
func ShardRequestFBEnd(b *flatbuffers.Builder) flatbuffers.UOffsetT                { return b.EndObject() }
