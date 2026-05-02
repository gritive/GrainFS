package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

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

func TestMetaJoinReceiver_SerializesSameNodeIDJoin(t *testing.T) {
	f := NewMetaFSM()
	started := make(chan struct{})
	release := make(chan struct{})
	coord := &fakeJoinCoordinator{
		leader:      true,
		fsm:         f,
		joinStarted: started,
		releaseJoin: release,
		onJoin: func(id, addr string) {
			require.NoError(t, f.applyCmd(makeAddNodeCmd(t, id, addr, 0)))
		},
	}
	receiver := NewMetaJoinReceiver(coord)
	payloadA, err := encodeJoinRequest(JoinRequest{NodeID: "node-2", Address: "10.0.0.2:7001"})
	require.NoError(t, err)
	payloadB, err := encodeJoinRequest(JoinRequest{NodeID: "node-2", Address: "10.0.0.22:7001"})
	require.NoError(t, err)

	replyA := make(chan *JoinReply, 1)
	replyB := make(chan *JoinReply, 1)
	go func() {
		resp := receiver.Handle(&transport.Message{Type: transport.StreamMetaJoin, Payload: payloadA})
		reply, err := decodeJoinReply(resp.Payload)
		require.NoError(t, err)
		replyA <- reply
	}()
	<-started
	go func() {
		resp := receiver.Handle(&transport.Message{Type: transport.StreamMetaJoin, Payload: payloadB})
		reply, err := decodeJoinReply(resp.Payload)
		require.NoError(t, err)
		replyB <- reply
	}()

	select {
	case reply := <-replyB:
		t.Fatalf("second join completed before first join registered membership: %+v", reply)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	require.Equal(t, JoinStatusOK, (<-replyA).Status)
	require.Equal(t, JoinStatusAddrMismatch, (<-replyB).Status)
	require.Equal(t, 1, coord.JoinCalls())
}

type fakeJoinCoordinator struct {
	leader      bool
	leaderID    string
	fsm         *MetaFSM
	joinStarted chan struct{}
	releaseJoin chan struct{}
	onJoin      func(id, addr string)
	mu          sync.Mutex
	joinCalls   int
}

func (f *fakeJoinCoordinator) IsLeader() bool { return f.leader }
func (f *fakeJoinCoordinator) LeaderID() string {
	return f.leaderID
}
func (f *fakeJoinCoordinator) Join(ctx context.Context, id, addr string) error {
	f.mu.Lock()
	f.joinCalls++
	if f.joinStarted != nil && f.joinCalls == 1 {
		close(f.joinStarted)
	}
	f.mu.Unlock()
	if f.releaseJoin != nil {
		select {
		case <-f.releaseJoin:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.onJoin != nil {
		f.onJoin(id, addr)
	}
	return nil
}
func (f *fakeJoinCoordinator) Nodes() []MetaNodeEntry {
	return f.fsm.Nodes()
}

func (f *fakeJoinCoordinator) JoinCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.joinCalls
}
