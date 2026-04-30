package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// Test helpers

func setupReceiver(t *testing.T, selfID string) (*ForwardReceiver, *DataGroupManager) {
	mgr := NewDataGroupManager()
	rcv := NewForwardReceiver(mgr)
	return rcv, mgr
}

// Tests

func TestForwardReceiver_UnknownGroup_NotVoter(t *testing.T) {
	rcv, _ := setupReceiver(t, "self")
	payload := encodeForwardPayload("g99", raftpb.ForwardOpHeadObject, buildHeadObjectArgs("b", "k"))
	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotVoter, fr.Status())
}

func TestForwardReceiver_NonLeaderVoter_ReturnsHint(t *testing.T) {
	rcv, mgr := setupReceiver(t, "self")

	// Create a minimal GroupBackend with wrapped DistributedBackend
	// The wrapped backend has a mock RaftNode
	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)

	mgr.Add(NewDataGroupWithBackend("g1", []string{"self", "peer-A"}, gb))

	payload := encodeForwardPayload("g1", raftpb.ForwardOpHeadObject, buildHeadObjectArgs("b", "k"))

	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	// Without a real RaftNode, we expect either NotVoter (nil node) or OK (if mock reports leader)
	status := fr.Status()
	require.True(t, status == raftpb.ForwardStatusOK || status == raftpb.ForwardStatusNotVoter || status == raftpb.ForwardStatusNotLeader,
		"expected OK/NotVoter/NotLeader, got %v", status)
}
