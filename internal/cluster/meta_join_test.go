package cluster

import (
	"context"
	"crypto/ed25519"
	"reflect"
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
func (f *fakeJoinCoordinator) IsSPKIDenylisted(spki [32]byte) bool {
	return f.fsm.peers.isDenylisted(spki)
}
func (f *fakeJoinCoordinator) SPKIOwner(spki [32]byte) (string, bool) {
	return f.fsm.peers.spkiOwner(spki)
}
func (f *fakeJoinCoordinator) LookupInvite(id string, now time.Time) (ed25519.PublicKey, bool) {
	return f.fsm.invites.lookup(id, now)
}
func (f *fakeJoinCoordinator) AcceptSPKIBytes() [][]byte {
	return f.fsm.peers.acceptSPKIBytes()
}
func (f *fakeJoinCoordinator) JoinViaInvite(ctx context.Context, nodeID, addr string, spki [32]byte, inviteID string) error {
	return f.Join(ctx, nodeID, addr)
}

// Two-phase invite-join stubs (W7). These fakes exercise only the KEK/legacy
// paths, so the two-phase methods are unused no-ops.
func (f *fakeJoinCoordinator) ProposeInvitePending(context.Context, string, string, [32]byte, string) error {
	return nil
}
func (f *fakeJoinCoordinator) LookupPending(string, time.Time) (string, [32]byte, string, bool) {
	return "", [32]byte{}, "", false
}
func (f *fakeJoinCoordinator) ProposeInviteConsume(context.Context, string) error { return nil }
func (f *fakeJoinCoordinator) ProposeInviteConsumeAt(context.Context, string, time.Time) error {
	return nil
}
func (f *fakeJoinCoordinator) RemoveLearner(string, string) error { return nil }
func (f *fakeJoinCoordinator) PeerSPKIs() [][32]byte              { return nil }
func (f *fakeJoinCoordinator) ClusterKeyDropped() bool            { return false }

func (f *fakeJoinCoordinator) JoinCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.joinCalls
}

func TestMetaJoin_RoundTrip_AllStatuses(t *testing.T) {
	statuses := []JoinStatus{
		JoinStatusOK, JoinStatusAlreadyMember, JoinStatusNotLeader,
		JoinStatusAddrMismatch, JoinStatusClusterFull, JoinStatusMixedVersion,
		JoinStatusTimeout, JoinStatusError,
	}
	req := JoinRequest{NodeID: "node-1", Address: "10.0.0.1:9100"}
	reqBytes, err := encodeJoinRequest(req)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	gotReq, err := decodeJoinRequest(reqBytes)
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if !reflect.DeepEqual(gotReq, req) {
		t.Errorf("request roundtrip: got %+v want %+v", gotReq, req)
	}

	for _, st := range statuses {
		st := st
		t.Run(string(st), func(t *testing.T) {
			reply := JoinReply{
				Accepted:   st == JoinStatusOK,
				Status:     st,
				Message:    "msg for " + string(st),
				LeaderID:   "leader-1",
				LeaderAddr: "10.0.0.2:9100",
			}
			payload, err := encodeJoinReply(reply)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			out, err := decodeJoinReply(payload)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(*out, reply) {
				t.Errorf("reply roundtrip: got %+v want %+v", *out, reply)
			}
		})
	}
}

func TestMetaJoinRequest_MalformedFB(t *testing.T) {
	bad := append([]byte(nil), metaJoinRequestMagic...)
	bad = append(bad, 0xff, 0xff, 0xff, 0xff)
	_, err := decodeJoinRequest(bad)
	if err == nil {
		t.Fatal("expected malformed-FB error, got nil")
	}
}

func TestMetaJoinReply_MalformedFB(t *testing.T) {
	bad := []byte{0xff, 0xff, 0xff, 0xff}
	_, err := decodeJoinReply(bad)
	if err == nil {
		t.Fatal("expected malformed-FB error, got nil")
	}
}

func TestJoinStatus_DriftGuard(t *testing.T) {
	cases := []JoinStatus{
		JoinStatusOK, JoinStatusAlreadyMember, JoinStatusNotLeader,
		JoinStatusAddrMismatch, JoinStatusClusterFull, JoinStatusMixedVersion,
		JoinStatusTimeout, JoinStatusError, JoinStatusKEKMismatch,
	}
	for _, s := range cases {
		fb := joinStatusToFB(s)
		back := joinStatusFromFB(fb)
		if back != s {
			t.Errorf("drift: %q -> %v -> %q", s, fb, back)
		}
	}
}

func TestHandleJoinRequest_NotLeaderReturnsRedirect(t *testing.T) {
	fsm := NewMetaFSM()
	coord := &fakeJoinCoordinator{leader: false, leaderID: "leader-1", fsm: fsm}
	r := NewMetaJoinReceiver(coord)

	reqBytes, err := encodeJoinRequest(JoinRequest{NodeID: "n2", Address: "10.0.0.2:7001"})
	require.NoError(t, err)

	// HandleJoinRequest preserves the not-leader gate: a follower returns the
	// standard JoinStatusNotLeader reply (with leader hint) rather than running
	// the invite path.
	replyBytes, err := r.HandleJoinRequest(context.Background(), [32]byte{1}, make([]byte, transport.JoinBindingLen), reqBytes)
	require.NoError(t, err)
	reply, err := decodeJoinReply(replyBytes)
	require.NoError(t, err)
	require.False(t, reply.Accepted)
	require.Equal(t, JoinStatusNotLeader, reply.Status)
	require.Equal(t, "leader-1", reply.LeaderID)
}
