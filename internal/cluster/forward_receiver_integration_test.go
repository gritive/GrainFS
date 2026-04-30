package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// TestForwardReceiver_PutObject_DispatchesToBackend verifies PutObject operation
// flows through FlatBuffers encoding and dispatches to the backend.
//
// NOTE: These tests use WrapDistributedBackend with a stub DistributedBackend.
// The stub has a nil raft.Node, so Handle() returns ForwardStatusNotLeader.
// Full end-to-end tests with actual leader simulation require a real Raft node
// (see backend_test.go for patterns with in-memory BadgerDB and Raft nodes).
func TestForwardReceiver_PutObject_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	// Create a minimal GroupBackend with wrapped DistributedBackend
	// The wrapped backend has a nil raft.Node (NotLeader status)
	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)

	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1", "node2"}, gb))

	// Encode FlatBuffers args
	body := []byte("hello world")
	args := buildPutObjectArgs("bucket-a", "key-1", "text/plain", body)
	payload := encodeForwardPayload("g1", raftpb.ForwardOpPutObject, args)

	// Call Handle
	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	// With nil node, expect NotLeader (group exists, backend exists, but no RaftNode)
	// This confirms the ForwardReceiver found the group, checked Backend(), then checked leadership
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"PutObject should dispatch to backend and return NotLeader for nil RaftNode")
}

// TestForwardReceiver_GetObject_DispatchesToBackend verifies GetObject operation.
func TestForwardReceiver_GetObject_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	args := buildGetObjectArgs("bucket-b", "key-2")
	payload := encodeForwardPayload("g1", raftpb.ForwardOpGetObject, args)

	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"GetObject should dispatch to backend and return NotLeader for nil RaftNode")
}

// TestForwardReceiver_HeadObject_DispatchesToBackend verifies HeadObject operation.
func TestForwardReceiver_HeadObject_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	args := buildHeadObjectArgs("bucket-c", "key-3")
	payload := encodeForwardPayload("g1", raftpb.ForwardOpHeadObject, args)

	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"HeadObject should dispatch to backend and return NotLeader for nil RaftNode")
}

// TestForwardReceiver_DeleteObject_DispatchesToBackend verifies DeleteObject operation.
func TestForwardReceiver_DeleteObject_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	args := buildDeleteObjectArgs("bucket-d", "key-4")
	payload := encodeForwardPayload("g1", raftpb.ForwardOpDeleteObject, args)

	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"DeleteObject should dispatch to backend and return NotLeader for nil RaftNode")
}

// TestForwardReceiver_ListObjects_DispatchesToBackend verifies ListObjects operation.
func TestForwardReceiver_ListObjects_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	args := buildListObjectsArgs("bucket-e", "prefix/", 100)
	payload := encodeForwardPayload("g1", raftpb.ForwardOpListObjects, args)

	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"ListObjects should dispatch to backend and return NotLeader for nil RaftNode")
}

// TestForwardReceiver_WalkObjects_DispatchesToBackend verifies WalkObjects operation.
func TestForwardReceiver_WalkObjects_DispatchesToBackend(t *testing.T) {
	rcv, mgr := setupReceiver(t, "node1")

	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"node1"}, gb))

	args := buildWalkObjectsArgs("bucket-f", "walk-prefix/")
	payload := encodeForwardPayload("g1", raftpb.ForwardOpWalkObjects, args)

	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)

	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status(),
		"WalkObjects should dispatch to backend and return NotLeader for nil RaftNode")
}
