package cluster

import (
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/raft/raftpb"
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
