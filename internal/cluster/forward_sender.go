package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/raft/raftpb"
)

// ForwardSender encodes a single transport.Message frame for the 0x08 stream
// type and delivers it via the supplied dialer. Single-message wire (no chunked
// streaming) — body bytes for PutObject/UploadPart sit inside the FBS args
// (5 MB hard cap enforced by ClusterCoordinator before encoding).
//
// Wire payload layout (single transport.Message of type=0x08):
//
//	[4B BE groupIDLen][groupID bytes][1B opcode][4B BE argsLen][FBS args]
//
// Reply payload is the FBS ForwardReply table.

var (
	ErrShortHeader     = errors.New("forward: short header")
	ErrTruncatedArgs   = errors.New("forward: truncated args")
	ErrNoReachablePeer = errors.New("forward: no reachable peer")
)

// forwardDialer abstracts the QUIC transport for testability. Production wires
// it to quicTransport.Send (type=StreamProposeGroupForward); tests pass a fake
// that returns canned replies.
type forwardDialer func(peer string, payload []byte) ([]byte, error)

// ForwardSender is the client side of the 0x08 wire. Stateless apart from the
// dialer + timeout — a single instance is shared across all coordinator calls.
type ForwardSender struct {
	dialer  forwardDialer
	timeout time.Duration
}

// NewForwardSender constructs a sender with default 10 s per-call timeout
// (header + body fits in one message; bench shows ≤2 s typical for 5 MB local
// loopback).
func NewForwardSender(d forwardDialer) *ForwardSender {
	return &ForwardSender{dialer: d, timeout: 10 * time.Second}
}

// encodeForwardPayload assembles the 0x08 wire payload.
func encodeForwardPayload(groupID string, op raftpb.ForwardOp, fbsArgs []byte) []byte {
	gid := []byte(groupID)
	out := make([]byte, 0, 4+len(gid)+1+4+len(fbsArgs))
	var be4 [4]byte
	binary.BigEndian.PutUint32(be4[:], uint32(len(gid)))
	out = append(out, be4[:]...)
	out = append(out, gid...)
	out = append(out, byte(op))
	binary.BigEndian.PutUint32(be4[:], uint32(len(fbsArgs)))
	out = append(out, be4[:]...)
	out = append(out, fbsArgs...)
	return out
}

// decodeForwardPayload is the receiver-side parser. fbsArgs aliases the input
// buffer — caller must copy if the buffer will be reused.
func decodeForwardPayload(buf []byte) (groupID string, op raftpb.ForwardOp, fbsArgs []byte, err error) {
	if len(buf) < 4 {
		err = ErrShortHeader
		return
	}
	gidLen := binary.BigEndian.Uint32(buf[0:4])
	if uint32(len(buf)) < 4+gidLen+1+4 {
		err = ErrShortHeader
		return
	}
	groupID = string(buf[4 : 4+gidLen])
	op = raftpb.ForwardOp(buf[4+gidLen])
	argsStart := 4 + gidLen + 1
	argsLen := binary.BigEndian.Uint32(buf[argsStart : argsStart+4])
	if uint32(len(buf)) < argsStart+4+argsLen {
		err = fmt.Errorf("%w: have %d need %d", ErrTruncatedArgs, len(buf), argsStart+4+argsLen)
		return
	}
	fbsArgs = buf[argsStart+4 : argsStart+4+argsLen]
	return
}

// Send delivers a single forward call. Tries peers in order; on dial error
// continues to the next peer. On NotLeader reply with a non-empty hint,
// redirects ONCE to the hinted peer (the hint may be a peer not in the
// supplied list). Returns the raw FBS ForwardReply bytes; caller decodes.
//
// Cold path semantics: 1 RTT loss accepted on first miss. The hint comes from
// raft.Node.LeaderID() on the receiver side, so the second hop is the actual
// leader 99% of the time.
func (s *ForwardSender) Send(
	ctx context.Context, peers []string, groupID string,
	op raftpb.ForwardOp, fbsArgs []byte,
) ([]byte, error) {
	payload := encodeForwardPayload(groupID, op, fbsArgs)

	var redirected bool
	var lastDialErr error
	for _, peer := range peers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reply, err := s.dialer(peer, payload)
		if err != nil {
			lastDialErr = err
			continue // try next peer
		}
		if isNotLeaderReply(reply) && !redirected {
			redirected = true
			if hint := extractLeaderHint(reply); hint != "" {
				r2, err2 := s.dialer(hint, payload)
				if err2 == nil {
					return r2, nil
				}
				// Hint dial failed; fall through to return original NotLeader
				// reply so caller can retry from a fresh node.
			}
		}
		return reply, nil
	}
	if lastDialErr != nil {
		return nil, fmt.Errorf("%w (last dial error: %v)", ErrNoReachablePeer, lastDialErr)
	}
	return nil, ErrNoReachablePeer
}

// isNotLeaderReply parses just enough of the FBS ForwardReply to read status.
func isNotLeaderReply(reply []byte) bool {
	if len(reply) == 0 {
		return false
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	return fr.Status() == raftpb.ForwardStatusNotLeader
}

// extractLeaderHint pulls the hint string from a NotLeader reply.
func extractLeaderHint(reply []byte) string {
	if len(reply) == 0 {
		return ""
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	return string(fr.LeaderHint())
}

// buildSimpleReply is a tiny helper to construct status-only replies (NotLeader
// hint, NotVoter, Internal). Receiver-side and tests share this so the wire
// layout stays in sync.
func buildSimpleReply(status raftpb.ForwardStatus, leaderHint string) []byte {
	b := flatbuffers.NewBuilder(64)
	hintOff := flatbuffers.UOffsetT(0)
	if leaderHint != "" {
		hintOff = b.CreateString(leaderHint)
	}
	raftpb.ForwardReplyStart(b)
	raftpb.ForwardReplyAddStatus(b, status)
	if leaderHint != "" {
		raftpb.ForwardReplyAddLeaderHint(b, hintOff)
	}
	off := raftpb.ForwardReplyEnd(b)
	b.Finish(off)
	return b.FinishedBytes()
}
