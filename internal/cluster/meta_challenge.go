// Package cluster — KEK handshake Challenge RPC.
//
// The Challenge RPC is the first half of the §7 cluster-join handshake
// (D#15, F#23). A joining node calls Challenge against the would-be leader
// to obtain a fresh 32-byte random nonce. The joiner then computes
// HMAC-SHA256(KEK, nonce) locally and sends it as JoinRequest.HandshakeResponse
// alongside the existing JoinRequest fields. The receiver verifies the
// response under its own KEK in MetaJoinReceiver.Handle before AddVoter runs.
//
// The Challenge and Join receivers MUST share the same *encrypt.HandshakeVerifier
// instance — the Challenge handler writes to the verifier's issued-nonce map
// and the Join handler reads from it.
package cluster

import (
	"bytes"
	"context"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// ChallengeRequest is sent by a joining node to obtain a fresh KEK-handshake
// nonce from a peer.
type ChallengeRequest struct {
	NodeID string `json:"node_id"`
}

// ChallengeReply carries the nonce (on success) or a JoinStatus describing
// why the challenge was refused.
type ChallengeReply struct {
	Nonce   []byte     `json:"nonce,omitempty"`
	Status  JoinStatus `json:"status"`
	Message string     `json:"message,omitempty"`
}

type metaChallengeDialer func(peer string, payload []byte) ([]byte, error)

type MetaChallengeSender struct {
	dialer metaChallengeDialer
}

func NewMetaChallengeSender(d metaChallengeDialer) *MetaChallengeSender {
	return &MetaChallengeSender{dialer: d}
}

// SendChallenge issues a Challenge RPC to peer and returns the reply.
func (s *MetaChallengeSender) SendChallenge(ctx context.Context, peer string, req ChallengeRequest) (*ChallengeReply, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	payload, err := encodeChallengeRequest(req)
	if err != nil {
		return nil, err
	}
	replyBytes, err := s.dialer(peer, payload)
	if err != nil {
		return nil, err
	}
	return decodeChallengeReply(replyBytes)
}

// MetaChallengeReceiver issues nonces via the shared HandshakeVerifier.
type MetaChallengeReceiver struct {
	verifier *encrypt.HandshakeVerifier
}

// NewMetaChallengeReceiver returns a receiver. The verifier MUST be the same
// instance as the one installed on the paired MetaJoinReceiver, otherwise
// VerifyResponse will see an unknown nonce and reject as KEK mismatch.
func NewMetaChallengeReceiver(v *encrypt.HandshakeVerifier) *MetaChallengeReceiver {
	return &MetaChallengeReceiver{verifier: v}
}

func (r *MetaChallengeReceiver) Handle(req *transport.Message) *transport.Message {
	if r.verifier == nil {
		return challengeMessage(ChallengeReply{Status: JoinStatusError, Message: "KEK handshake verifier not configured"})
	}
	if _, err := decodeChallengeRequest(req.Payload); err != nil {
		return challengeMessage(ChallengeReply{Status: JoinStatusError, Message: err.Error()})
	}
	nonce, err := r.verifier.IssueChallenge()
	if err != nil {
		return challengeMessage(ChallengeReply{Status: JoinStatusError, Message: err.Error()})
	}
	return challengeMessage(ChallengeReply{Nonce: nonce, Status: JoinStatusOK})
}

func challengeMessage(reply ChallengeReply) *transport.Message {
	data, _ := encodeChallengeReply(reply)
	return &transport.Message{Type: transport.StreamMetaJoinChallenge, Payload: data}
}

var metaChallengeRequestMagic = []byte("GFSMJC1")

func encodeChallengeRequest(req ChallengeRequest) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	nodeOff := b.CreateString(req.NodeID)
	clusterpb.ChallengeRequestStart(b)
	clusterpb.ChallengeRequestAddNodeId(b, nodeOff)
	b.Finish(clusterpb.ChallengeRequestEnd(b))
	fb := b.FinishedBytes()
	out := make([]byte, 0, len(metaChallengeRequestMagic)+len(fb))
	out = append(out, metaChallengeRequestMagic...)
	out = append(out, fb...)
	return out, nil
}

func decodeChallengeRequest(data []byte) (req ChallengeRequest, err error) {
	if !bytes.HasPrefix(data, metaChallengeRequestMagic) {
		return ChallengeRequest{}, fmt.Errorf("meta_challenge: bad magic")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("meta_challenge: malformed ChallengeRequest: %v", r)
		}
	}()
	fb := clusterpb.GetRootAsChallengeRequest(data[len(metaChallengeRequestMagic):], 0)
	req.NodeID = string(fb.NodeId())
	return req, nil
}

func encodeChallengeReply(reply ChallengeReply) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	nonceOff := b.CreateByteVector(reply.Nonce)
	msgOff := b.CreateString(reply.Message)
	clusterpb.ChallengeReplyStart(b)
	clusterpb.ChallengeReplyAddNonce(b, nonceOff)
	clusterpb.ChallengeReplyAddStatus(b, joinStatusToFB(reply.Status))
	clusterpb.ChallengeReplyAddMessage(b, msgOff)
	b.Finish(clusterpb.ChallengeReplyEnd(b))
	return append([]byte(nil), b.FinishedBytes()...), nil
}

func decodeChallengeReply(data []byte) (reply *ChallengeReply, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("meta_challenge: malformed ChallengeReply: %v", r)
		}
	}()
	fb := clusterpb.GetRootAsChallengeReply(data, 0)
	out := &ChallengeReply{
		Status:  joinStatusFromFB(fb.Status()),
		Message: string(fb.Message()),
	}
	if n := fb.NonceBytes(); len(n) > 0 {
		out.Nonce = append([]byte(nil), n...)
	}
	return out, nil
}
