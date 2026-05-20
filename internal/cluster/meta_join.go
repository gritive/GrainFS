package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

type JoinStatus string

const (
	JoinStatusOK            JoinStatus = "ok"
	JoinStatusAlreadyMember JoinStatus = "already_member"
	JoinStatusNotLeader     JoinStatus = "not_leader"
	JoinStatusAddrMismatch  JoinStatus = "addr_mismatch"
	JoinStatusClusterFull   JoinStatus = "cluster_full"
	JoinStatusMixedVersion  JoinStatus = "mixed_version"
	JoinStatusTimeout       JoinStatus = "timeout"
	JoinStatusError         JoinStatus = "error"
	// JoinStatusKEKMismatch is returned when the joiner's HMAC-SHA256 response
	// does not match the leader's KEK (or the nonce is unknown / replayed /
	// expired). The joiner is refused admission before AddVoter is called.
	// §7 T55 (D#15, F#23).
	JoinStatusKEKMismatch JoinStatus = "kek_mismatch"
)

type JoinRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
	// HandshakeNonce/Response carry the KEK challenge-response proof. The
	// joiner first calls Challenge to obtain a fresh Nonce, computes
	// HMAC-SHA256(KEK, Nonce) as Response, and sends both alongside Join.
	// §7 T55 (D#15, F#23).
	HandshakeNonce    []byte `json:"handshake_nonce,omitempty"`
	HandshakeResponse []byte `json:"handshake_response,omitempty"`
}

type JoinReply struct {
	Accepted   bool       `json:"accepted"`
	Status     JoinStatus `json:"status"`
	Message    string     `json:"message,omitempty"`
	LeaderID   string     `json:"leader_id,omitempty"`
	LeaderAddr string     `json:"leader_addr,omitempty"`
}

type metaJoinDialer func(peer string, payload []byte) ([]byte, error)

type MetaJoinSender struct {
	dialer metaJoinDialer
}

func NewMetaJoinSender(d metaJoinDialer) *MetaJoinSender {
	return &MetaJoinSender{dialer: d}
}

func (s *MetaJoinSender) SendJoin(ctx context.Context, peers []string, req JoinRequest) (*JoinReply, error) {
	payload, err := encodeJoinRequest(req)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, peer := range peers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		replyBytes, err := s.dialer(peer, payload)
		if err != nil {
			lastErr = err
			continue
		}
		reply, err := decodeJoinReply(replyBytes)
		if err != nil {
			return nil, err
		}
		if reply.Status == JoinStatusNotLeader && reply.LeaderAddr != "" {
			replyBytes, err = s.dialer(reply.LeaderAddr, payload)
			if err != nil {
				lastErr = err
				continue
			}
			return decodeJoinReply(replyBytes)
		}
		return reply, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("meta_join: no peers")
}

type metaJoinCoordinator interface {
	IsLeader() bool
	LeaderID() string
	Join(ctx context.Context, id, addr string) error
	Nodes() []MetaNodeEntry
}

type MetaJoinReceiver struct {
	meta         metaJoinCoordinator
	joinMu       sync.Mutex
	postJoinHook func(context.Context, JoinRequest) error
	// verifier, when non-nil, gates admission on the KEK challenge-response
	// HMAC. T55 ships the plumbing; T57 will require verifier != nil at boot.
	// When nil (legacy callers, unit tests not exercising the handshake), the
	// HMAC gate is skipped so existing behavior is preserved.
	verifier *encrypt.HandshakeVerifier
}

func NewMetaJoinReceiver(meta metaJoinCoordinator) *MetaJoinReceiver {
	return &MetaJoinReceiver{meta: meta}
}

func (r *MetaJoinReceiver) WithPostJoinHook(fn func(context.Context, JoinRequest) error) *MetaJoinReceiver {
	r.postJoinHook = fn
	return r
}

// WithHandshakeVerifier installs the KEK handshake verifier used to gate
// admission. The SAME verifier instance must be wired into the paired
// MetaChallengeReceiver so the issued-nonce map is shared. §7 T55 (D#15, F#23).
func (r *MetaJoinReceiver) WithHandshakeVerifier(v *encrypt.HandshakeVerifier) *MetaJoinReceiver {
	r.verifier = v
	return r
}

func (r *MetaJoinReceiver) Handle(req *transport.Message) *transport.Message {
	joinReq, err := decodeJoinRequest(req.Payload)
	if err != nil {
		return joinMessage(JoinReply{Accepted: false, Status: JoinStatusError, Message: err.Error()})
	}
	if joinReq.NodeID == "" || joinReq.Address == "" {
		return joinMessage(JoinReply{Accepted: false, Status: JoinStatusError, Message: "node_id and address are required"})
	}
	if !r.meta.IsLeader() {
		leaderID := r.meta.LeaderID()
		leaderAddr := ""
		for _, n := range r.meta.Nodes() {
			if n.ID == leaderID || n.Address == leaderID {
				leaderAddr = n.Address
				break
			}
		}
		if leaderAddr == "" {
			leaderAddr = leaderID
		}
		return joinMessage(JoinReply{
			Accepted:   false,
			Status:     JoinStatusNotLeader,
			LeaderID:   leaderID,
			LeaderAddr: leaderAddr,
		})
	}
	// KEK handshake gate. Runs after the leader check so non-leaders return
	// JoinStatusNotLeader without consuming a nonce on the leader's verifier
	// (saves nonce churn on follower retries). §7 T55 (D#15, F#23).
	if r.verifier != nil {
		if err := r.verifier.VerifyResponse(joinReq.HandshakeNonce, joinReq.HandshakeResponse); err != nil {
			return joinMessage(JoinReply{
				Accepted: false,
				Status:   JoinStatusKEKMismatch,
				Message:  "KEK handshake failed: " + err.Error(),
			})
		}
	}
	r.joinMu.Lock()
	defer r.joinMu.Unlock()
	for _, n := range r.meta.Nodes() {
		if n.ID == joinReq.NodeID {
			if n.Address == joinReq.Address {
				return joinMessage(JoinReply{Accepted: true, Status: JoinStatusAlreadyMember})
			}
			return joinMessage(JoinReply{Accepted: false, Status: JoinStatusAddrMismatch, Message: "node ID already exists with different address"})
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := r.meta.Join(ctx, joinReq.NodeID, joinReq.Address); err != nil {
		return joinMessage(joinReplyFromError(err))
	}
	if r.postJoinHook != nil {
		if err := r.postJoinHook(ctx, joinReq); err != nil {
			return joinMessage(joinReplyFromError(err))
		}
	}
	return joinMessage(JoinReply{Accepted: true, Status: JoinStatusOK})
}

func joinReplyFromError(err error) JoinReply {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return JoinReply{Accepted: false, Status: JoinStatusNotLeader, Message: err.Error()}
	case errors.Is(err, raft.ErrMixedVersionNoMembershipChange):
		return JoinReply{Accepted: false, Status: JoinStatusMixedVersion, Message: err.Error()}
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return JoinReply{Accepted: false, Status: JoinStatusTimeout, Message: err.Error()}
	default:
		return JoinReply{Accepted: false, Status: JoinStatusError, Message: err.Error()}
	}
}

func joinMessage(reply JoinReply) *transport.Message {
	data, _ := encodeJoinReply(reply)
	return &transport.Message{Type: transport.StreamMetaJoin, Payload: data}
}

var metaJoinRequestMagic = []byte("GFSMJN2")

// 128-byte initial: request is two short strings; reply is one bool + one enum
// + three short strings. Typical payload fits in one slab.
var metaJoinBuilderPool = pool.New(func() *flatbuffers.Builder {
	return flatbuffers.NewBuilder(128)
})

func newMetaJoinBuilder() *flatbuffers.Builder {
	b := metaJoinBuilderPool.Get()
	b.Reset()
	return b
}

func releaseMetaJoinBuilder(b *flatbuffers.Builder) {
	metaJoinBuilderPool.Put(b)
}

func joinStatusToFB(s JoinStatus) clusterpb.JoinStatus {
	switch s {
	case JoinStatusOK:
		return clusterpb.JoinStatusOK
	case JoinStatusAlreadyMember:
		return clusterpb.JoinStatusAlreadyMember
	case JoinStatusNotLeader:
		return clusterpb.JoinStatusNotLeader
	case JoinStatusAddrMismatch:
		return clusterpb.JoinStatusAddrMismatch
	case JoinStatusClusterFull:
		return clusterpb.JoinStatusClusterFull
	case JoinStatusMixedVersion:
		return clusterpb.JoinStatusMixedVersion
	case JoinStatusTimeout:
		return clusterpb.JoinStatusTimeout
	case JoinStatusError:
		return clusterpb.JoinStatusError
	case JoinStatusKEKMismatch:
		return clusterpb.JoinStatusKEKMismatch
	default:
		return clusterpb.JoinStatusUnknown
	}
}

func joinStatusFromFB(s clusterpb.JoinStatus) JoinStatus {
	switch s {
	case clusterpb.JoinStatusOK:
		return JoinStatusOK
	case clusterpb.JoinStatusAlreadyMember:
		return JoinStatusAlreadyMember
	case clusterpb.JoinStatusNotLeader:
		return JoinStatusNotLeader
	case clusterpb.JoinStatusAddrMismatch:
		return JoinStatusAddrMismatch
	case clusterpb.JoinStatusClusterFull:
		return JoinStatusClusterFull
	case clusterpb.JoinStatusMixedVersion:
		return JoinStatusMixedVersion
	case clusterpb.JoinStatusTimeout:
		return JoinStatusTimeout
	case clusterpb.JoinStatusError:
		return JoinStatusError
	case clusterpb.JoinStatusKEKMismatch:
		return JoinStatusKEKMismatch
	default:
		return JoinStatus("")
	}
}

func encodeJoinRequest(req JoinRequest) ([]byte, error) {
	b := newMetaJoinBuilder()
	defer releaseMetaJoinBuilder(b)
	nodeOff := b.CreateString(req.NodeID)
	addrOff := b.CreateString(req.Address)
	nonceOff := b.CreateByteVector(req.HandshakeNonce)
	respOff := b.CreateByteVector(req.HandshakeResponse)
	clusterpb.JoinRequestStart(b)
	clusterpb.JoinRequestAddNodeId(b, nodeOff)
	clusterpb.JoinRequestAddAddress(b, addrOff)
	clusterpb.JoinRequestAddHandshakeNonce(b, nonceOff)
	clusterpb.JoinRequestAddHandshakeResponse(b, respOff)
	b.Finish(clusterpb.JoinRequestEnd(b))
	fb := b.FinishedBytes()
	out := make([]byte, 0, len(metaJoinRequestMagic)+len(fb))
	out = append(out, metaJoinRequestMagic...)
	out = append(out, fb...)
	return out, nil
}

func decodeJoinRequest(data []byte) (req JoinRequest, err error) {
	if !bytes.HasPrefix(data, metaJoinRequestMagic) {
		return JoinRequest{}, fmt.Errorf("meta_join: bad magic")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("meta_join: malformed JoinRequest: %v", r)
		}
	}()
	fb := clusterpb.GetRootAsJoinRequest(data[len(metaJoinRequestMagic):], 0)
	req.NodeID = string(fb.NodeId())
	req.Address = string(fb.Address())
	if n := fb.HandshakeNonceBytes(); len(n) > 0 {
		req.HandshakeNonce = append([]byte(nil), n...)
	}
	if r := fb.HandshakeResponseBytes(); len(r) > 0 {
		req.HandshakeResponse = append([]byte(nil), r...)
	}
	return req, nil
}

func encodeJoinReply(reply JoinReply) ([]byte, error) {
	b := newMetaJoinBuilder()
	defer releaseMetaJoinBuilder(b)
	msgOff := b.CreateString(reply.Message)
	leaderIDOff := b.CreateString(reply.LeaderID)
	leaderAddrOff := b.CreateString(reply.LeaderAddr)
	clusterpb.JoinReplyStart(b)
	clusterpb.JoinReplyAddAccepted(b, reply.Accepted)
	clusterpb.JoinReplyAddStatus(b, joinStatusToFB(reply.Status))
	clusterpb.JoinReplyAddMessage(b, msgOff)
	clusterpb.JoinReplyAddLeaderId(b, leaderIDOff)
	clusterpb.JoinReplyAddLeaderAddr(b, leaderAddrOff)
	b.Finish(clusterpb.JoinReplyEnd(b))
	return append([]byte(nil), b.FinishedBytes()...), nil
}

func decodeJoinReply(data []byte) (reply *JoinReply, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("meta_join: malformed JoinReply: %v", r)
		}
	}()
	fb := clusterpb.GetRootAsJoinReply(data, 0)
	return &JoinReply{
		Accepted:   fb.Accepted(),
		Status:     joinStatusFromFB(fb.Status()),
		Message:    string(fb.Message()),
		LeaderID:   string(fb.LeaderId()),
		LeaderAddr: string(fb.LeaderAddr()),
	}, nil
}
