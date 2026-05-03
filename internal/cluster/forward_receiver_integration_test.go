package cluster

import (
	"bytes"
	"io"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestForwardReceiver_PutObject_DispatchesToBackend verifies PutObject operation
// is properly decoded and routed through the ForwardReceiver.
//
// NOTE: This test uses WrapDistributedBackend with a nil raft.Node.
// The nil node causes Handle() to return ForwardStatusNotLeader.
// Full end-to-end tests with actual leader simulation would require
// in-memory BadgerDB and Raft nodes (see backend_test.go for patterns).
func TestForwardReceiver_PutObject_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1", "node2"}, gb))

	// Encode PutObject args using FlatBuffers
	builder := flatbuffers.NewBuilder(0)
	bucketStr := builder.CreateString("test-bucket")
	keyStr := builder.CreateString("test-key")
	ctStr := builder.CreateString("text/plain")
	bodyBytes := builder.CreateByteVector([]byte("test-body"))

	raftpb.PutObjectArgsStart(builder)
	raftpb.PutObjectArgsAddBucket(builder, bucketStr)
	raftpb.PutObjectArgsAddKey(builder, keyStr)
	raftpb.PutObjectArgsAddContentType(builder, ctStr)
	raftpb.PutObjectArgsAddBody(builder, bodyBytes)
	paOffset := raftpb.PutObjectArgsEnd(builder)
	builder.Finish(paOffset)

	payload := encodeForwardPayload("g1", raftpb.ForwardOpPutObject, builder.FinishedBytes())
	msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

	reply := rcv.Handle(msg)
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	// With nil node, expect NotLeader (group exists, backend exists, but no RaftNode)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"PutObject should decode and attempt dispatch, returning NotLeader for nil RaftNode")
}

// TestForwardReceiver_GetObject_DispatchesToBackend verifies GetObject operation.
func TestForwardReceiver_GetObject_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	builder := flatbuffers.NewBuilder(0)
	bucketStr := builder.CreateString("get-bucket")
	keyStr := builder.CreateString("get-key")

	raftpb.GetObjectArgsStart(builder)
	raftpb.GetObjectArgsAddBucket(builder, bucketStr)
	raftpb.GetObjectArgsAddKey(builder, keyStr)
	gaOffset := raftpb.GetObjectArgsEnd(builder)
	builder.Finish(gaOffset)

	payload := encodeForwardPayload("g1", raftpb.ForwardOpGetObject, builder.FinishedBytes())
	msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

	reply := rcv.Handle(msg)
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"GetObject should decode and attempt dispatch, returning NotLeader for nil RaftNode")
}

func TestForwardReceiver_GetObjectVersion_TooLargeReturnsEntityTooLarge(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")
	gb := newTestGroupBackend(t, "g1")
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	obj, err := gb.PutObject("bk", "large", bytes.NewReader(bytes.Repeat([]byte("x"), int(DefaultMaxForwardReplyBytes)+1)), "application/octet-stream")
	require.NoError(t, err)

	payload := encodeForwardPayload("g1", raftpb.ForwardOpGetObjectVersion, buildGetObjectVersionArgs("bk", "large", obj.VersionID))
	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusEntityTooLarge, fr.Status())
}

func TestForwardReceiver_ListObjectVersions_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")
	gb := newTestGroupBackend(t, "g1")
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	first, err := gb.PutObject("bk", "k", bytes.NewReader([]byte("v1")), "text/plain")
	require.NoError(t, err)
	_, err = gb.PutObject("bk", "k", bytes.NewReader([]byte("v2")), "text/plain")
	require.NoError(t, err)
	require.NoError(t, gb.DeleteObject("bk", "k"))

	payload := encodeForwardPayload("g1", raftpb.ForwardOpListObjectVersions, buildListObjectVersionsArgs("bk", "k", 100))
	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)

	versions, err := objectVersionsFromReply(reply.Payload)
	require.NoError(t, err)
	require.Len(t, versions, 3)
	require.True(t, versions[0].IsLatest)
	require.True(t, versions[0].IsDeleteMarker)
	require.Equal(t, first.VersionID, versions[2].VersionID)
}

func TestForwardReceiver_DeleteObjectVersion_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")
	gb := newTestGroupBackend(t, "g1")
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	obj, err := gb.PutObject("bk", "k", bytes.NewReader([]byte("v1")), "text/plain")
	require.NoError(t, err)

	payload := encodeForwardPayload("g1", raftpb.ForwardOpDeleteObjectVersion, buildDeleteObjectVersionArgs("bk", "k", obj.VersionID))
	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)
	require.NoError(t, parseReplyStatus(reply.Payload))

	_, _, err = gb.GetObjectVersion("bk", "k", obj.VersionID)
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestForwardReceiver_GetObjectVersion_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")
	gb := newTestGroupBackend(t, "g1")
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	obj, err := gb.PutObject("bk", "k", bytes.NewReader([]byte("v1")), "text/plain")
	require.NoError(t, err)

	payload := encodeForwardPayload("g1", raftpb.ForwardOpGetObjectVersion, buildGetObjectVersionArgs("bk", "k", obj.VersionID))
	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)

	got, err := objectFromReply(reply.Payload)
	require.NoError(t, err)
	require.Equal(t, obj.VersionID, got.VersionID)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	body, err := io.ReadAll(bytes.NewReader(fr.ReadBodyBytes()))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), body)
}

// TestForwardReceiver_HeadObject_DispatchesToBackend verifies HeadObject operation.
func TestForwardReceiver_HeadObject_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	builder := flatbuffers.NewBuilder(0)
	bucketStr := builder.CreateString("head-bucket")
	keyStr := builder.CreateString("head-key")

	raftpb.HeadObjectArgsStart(builder)
	raftpb.HeadObjectArgsAddBucket(builder, bucketStr)
	raftpb.HeadObjectArgsAddKey(builder, keyStr)
	haOffset := raftpb.HeadObjectArgsEnd(builder)
	builder.Finish(haOffset)

	payload := encodeForwardPayload("g1", raftpb.ForwardOpHeadObject, builder.FinishedBytes())
	msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

	reply := rcv.Handle(msg)
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"HeadObject should decode and attempt dispatch, returning NotLeader for nil RaftNode")
}

// TestForwardReceiver_DeleteObject_DispatchesToBackend verifies DeleteObject operation.
func TestForwardReceiver_DeleteObject_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	builder := flatbuffers.NewBuilder(0)
	bucketStr := builder.CreateString("del-bucket")
	keyStr := builder.CreateString("del-key")

	raftpb.DeleteObjectArgsStart(builder)
	raftpb.DeleteObjectArgsAddBucket(builder, bucketStr)
	raftpb.DeleteObjectArgsAddKey(builder, keyStr)
	daOffset := raftpb.DeleteObjectArgsEnd(builder)
	builder.Finish(daOffset)

	payload := encodeForwardPayload("g1", raftpb.ForwardOpDeleteObject, builder.FinishedBytes())
	msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

	reply := rcv.Handle(msg)
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"DeleteObject should decode and attempt dispatch, returning NotLeader for nil RaftNode")
}

// TestForwardReceiver_ListObjects_DispatchesToBackend verifies ListObjects operation.
func TestForwardReceiver_ListObjects_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	builder := flatbuffers.NewBuilder(0)
	bucketStr := builder.CreateString("list-bucket")
	prefixStr := builder.CreateString("prefix/")

	raftpb.ListObjectsArgsStart(builder)
	raftpb.ListObjectsArgsAddBucket(builder, bucketStr)
	raftpb.ListObjectsArgsAddPrefix(builder, prefixStr)
	raftpb.ListObjectsArgsAddMaxKeys(builder, 100)
	laOffset := raftpb.ListObjectsArgsEnd(builder)
	builder.Finish(laOffset)

	payload := encodeForwardPayload("g1", raftpb.ForwardOpListObjects, builder.FinishedBytes())
	msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

	reply := rcv.Handle(msg)
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"ListObjects should decode and attempt dispatch, returning NotLeader for nil RaftNode")
}

// TestForwardReceiver_WalkObjects_DispatchesToBackend verifies WalkObjects operation.
func TestForwardReceiver_WalkObjects_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	builder := flatbuffers.NewBuilder(0)
	bucketStr := builder.CreateString("walk-bucket")
	prefixStr := builder.CreateString("walk-prefix/")

	raftpb.WalkObjectsArgsStart(builder)
	raftpb.WalkObjectsArgsAddBucket(builder, bucketStr)
	raftpb.WalkObjectsArgsAddPrefix(builder, prefixStr)
	waOffset := raftpb.WalkObjectsArgsEnd(builder)
	builder.Finish(waOffset)

	payload := encodeForwardPayload("g1", raftpb.ForwardOpWalkObjects, builder.FinishedBytes())
	msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

	reply := rcv.Handle(msg)
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"WalkObjects should decode and attempt dispatch, returning NotLeader for nil RaftNode")
}

// TestForwardReceiver_CreateMultipartUpload_DispatchesToBackend verifies CreateMultipartUpload operation.
func TestForwardReceiver_CreateMultipartUpload_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	builder := flatbuffers.NewBuilder(0)
	bucketStr := builder.CreateString("mpu-bucket")
	keyStr := builder.CreateString("mpu-key")
	ctStr := builder.CreateString("application/octet-stream")

	raftpb.CreateMultipartUploadArgsStart(builder)
	raftpb.CreateMultipartUploadArgsAddBucket(builder, bucketStr)
	raftpb.CreateMultipartUploadArgsAddKey(builder, keyStr)
	raftpb.CreateMultipartUploadArgsAddContentType(builder, ctStr)
	cmaOffset := raftpb.CreateMultipartUploadArgsEnd(builder)
	builder.Finish(cmaOffset)

	payload := encodeForwardPayload("g1", raftpb.ForwardOpCreateMultipartUpload, builder.FinishedBytes())
	msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

	reply := rcv.Handle(msg)
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"CreateMultipartUpload should decode and attempt dispatch, returning NotLeader for nil RaftNode")
}

// TestForwardReceiver_UploadPart_DispatchesToBackend verifies UploadPart operation.
func TestForwardReceiver_UploadPart_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	builder := flatbuffers.NewBuilder(0)
	bucketStr := builder.CreateString("upload-bucket")
	keyStr := builder.CreateString("upload-key")
	uploadIDStr := builder.CreateString("test-upload-id")
	partData := builder.CreateByteVector([]byte("part-data-body"))

	raftpb.UploadPartArgsStart(builder)
	raftpb.UploadPartArgsAddBucket(builder, bucketStr)
	raftpb.UploadPartArgsAddKey(builder, keyStr)
	raftpb.UploadPartArgsAddUploadId(builder, uploadIDStr)
	raftpb.UploadPartArgsAddPartNumber(builder, 1)
	raftpb.UploadPartArgsAddBody(builder, partData)
	upaOffset := raftpb.UploadPartArgsEnd(builder)
	builder.Finish(upaOffset)

	payload := encodeForwardPayload("g1", raftpb.ForwardOpUploadPart, builder.FinishedBytes())
	msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

	reply := rcv.Handle(msg)
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"UploadPart should decode and attempt dispatch, returning NotLeader for nil RaftNode")
}

// TestForwardReceiver_CompleteMultipartUpload_DispatchesToBackend verifies CompleteMultipartUpload operation.
func TestForwardReceiver_CompleteMultipartUpload_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	builder := flatbuffers.NewBuilder(0)
	bucketStr := builder.CreateString("complete-bucket")
	keyStr := builder.CreateString("complete-key")
	uploadIDStr := builder.CreateString("test-upload-id")

	// Build PartRef vector
	etag1Str := builder.CreateString("etag-1")
	raftpb.PartRefStart(builder)
	raftpb.PartRefAddPartNumber(builder, 1)
	raftpb.PartRefAddEtag(builder, etag1Str)
	part1Offset := raftpb.PartRefEnd(builder)

	etag2Str := builder.CreateString("etag-2")
	raftpb.PartRefStart(builder)
	raftpb.PartRefAddPartNumber(builder, 2)
	raftpb.PartRefAddEtag(builder, etag2Str)
	part2Offset := raftpb.PartRefEnd(builder)

	// Create vector
	raftpb.CompleteMultipartUploadArgsStartPartsVector(builder, 2)
	builder.PrependUOffsetT(part2Offset)
	builder.PrependUOffsetT(part1Offset)
	partsVector := builder.EndVector(2)

	raftpb.CompleteMultipartUploadArgsStart(builder)
	raftpb.CompleteMultipartUploadArgsAddBucket(builder, bucketStr)
	raftpb.CompleteMultipartUploadArgsAddKey(builder, keyStr)
	raftpb.CompleteMultipartUploadArgsAddUploadId(builder, uploadIDStr)
	raftpb.CompleteMultipartUploadArgsAddParts(builder, partsVector)
	cmaOffset := raftpb.CompleteMultipartUploadArgsEnd(builder)
	builder.Finish(cmaOffset)

	payload := encodeForwardPayload("g1", raftpb.ForwardOpCompleteMultipartUpload, builder.FinishedBytes())
	msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

	reply := rcv.Handle(msg)
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"CompleteMultipartUpload should decode and attempt dispatch, returning NotLeader for nil RaftNode")
}

// TestForwardReceiver_AbortMultipartUpload_DispatchesToBackend verifies AbortMultipartUpload operation.
func TestForwardReceiver_AbortMultipartUpload_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	builder := flatbuffers.NewBuilder(0)
	bucketStr := builder.CreateString("abort-bucket")
	keyStr := builder.CreateString("abort-key")
	uploadIDStr := builder.CreateString("test-upload-id")

	raftpb.AbortMultipartUploadArgsStart(builder)
	raftpb.AbortMultipartUploadArgsAddBucket(builder, bucketStr)
	raftpb.AbortMultipartUploadArgsAddKey(builder, keyStr)
	raftpb.AbortMultipartUploadArgsAddUploadId(builder, uploadIDStr)
	amaOffset := raftpb.AbortMultipartUploadArgsEnd(builder)
	builder.Finish(amaOffset)

	payload := encodeForwardPayload("g1", raftpb.ForwardOpAbortMultipartUpload, builder.FinishedBytes())
	msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

	reply := rcv.Handle(msg)
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"AbortMultipartUpload should decode and attempt dispatch, returning NotLeader for nil RaftNode")
}
