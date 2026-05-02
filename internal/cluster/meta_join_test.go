package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/transport"
)

func TestJoinStatus_EncodeDecodeTyped(t *testing.T) {
	in := JoinReply{Accepted: false, Status: JoinStatusNotLeader, LeaderID: "n1", LeaderAddr: "10.0.0.1:7001"}

	data, err := encodeJoinReply(in)
	require.NoError(t, err)
	out, err := decodeJoinReply(data)
	require.NoError(t, err)

	require.Equal(t, JoinStatusNotLeader, out.Status)
	require.Equal(t, "10.0.0.1:7001", out.LeaderAddr)
}

func TestMetaJoin_StreamTypeReserved(t *testing.T) {
	require.Equal(t, transport.StreamType(0x0E), transport.StreamMetaJoin)
}

func TestMetaJoinSender_NotLeaderRetriesLeaderHint(t *testing.T) {
	calls := make([]string, 0, 2)
	s := NewMetaJoinSender(func(peer string, payload []byte) ([]byte, error) {
		calls = append(calls, peer)
		req, err := decodeJoinRequest(payload)
		require.NoError(t, err)
		require.Equal(t, "node-2", req.NodeID)
		if peer == "follower:7001" {
			reply, err := encodeJoinReply(JoinReply{
				Accepted:   false,
				Status:     JoinStatusNotLeader,
				LeaderID:   "node-1",
				LeaderAddr: "leader:7001",
			})
			require.NoError(t, err)
			return reply, nil
		}
		reply, err := encodeJoinReply(JoinReply{Accepted: true, Status: JoinStatusOK})
		require.NoError(t, err)
		return reply, nil
	})

	reply, err := s.SendJoin(context.Background(), []string{"follower:7001"}, JoinRequest{
		NodeID:  "node-2",
		Address: "node-2:7001",
	})
	require.NoError(t, err)
	require.True(t, reply.Accepted)
	require.Equal(t, []string{"follower:7001", "leader:7001"}, calls)
}

func TestMetaJoinReceiver_NotLeaderReturnsResolvedHint(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.1:7001", 0)))
	receiver := NewMetaJoinReceiver(&fakeJoinCoordinator{
		leaderID: "node-1",
		fsm:      f,
	})
	payload, err := encodeJoinRequest(JoinRequest{NodeID: "node-2", Address: "10.0.0.2:7001"})
	require.NoError(t, err)

	resp := receiver.Handle(&transport.Message{Type: transport.StreamMetaJoin, Payload: payload})
	reply, err := decodeJoinReply(resp.Payload)
	require.NoError(t, err)

	require.False(t, reply.Accepted)
	require.Equal(t, JoinStatusNotLeader, reply.Status)
	require.Equal(t, "node-1", reply.LeaderID)
	require.Equal(t, "10.0.0.1:7001", reply.LeaderAddr)
}

type fakeJoinCoordinator struct {
	leader   bool
	leaderID string
	fsm      *MetaFSM
}

func (f *fakeJoinCoordinator) IsLeader() bool { return f.leader }
func (f *fakeJoinCoordinator) LeaderID() string {
	return f.leaderID
}
func (f *fakeJoinCoordinator) Join(ctx context.Context, id, addr string) error {
	return nil
}
func (f *fakeJoinCoordinator) Nodes() []MetaNodeEntry {
	return f.fsm.Nodes()
}
