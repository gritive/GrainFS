package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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
)

type JoinRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
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
	meta metaJoinCoordinator
}

func NewMetaJoinReceiver(meta metaJoinCoordinator) *MetaJoinReceiver {
	return &MetaJoinReceiver{meta: meta}
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
			if n.ID == leaderID {
				leaderAddr = n.Address
				break
			}
		}
		return joinMessage(JoinReply{
			Accepted:   false,
			Status:     JoinStatusNotLeader,
			LeaderID:   leaderID,
			LeaderAddr: leaderAddr,
		})
	}
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

func encodeJoinRequest(req JoinRequest) ([]byte, error) {
	return json.Marshal(req)
}

func decodeJoinRequest(data []byte) (JoinRequest, error) {
	var req JoinRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return JoinRequest{}, fmt.Errorf("meta_join: decode request: %w", err)
	}
	return req, nil
}

func encodeJoinReply(reply JoinReply) ([]byte, error) {
	return json.Marshal(reply)
}

func decodeJoinReply(data []byte) (*JoinReply, error) {
	var reply JoinReply
	if err := json.Unmarshal(data, &reply); err != nil {
		return nil, fmt.Errorf("meta_join: decode reply: %w", err)
	}
	return &reply, nil
}
