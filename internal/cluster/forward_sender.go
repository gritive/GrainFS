package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
)

// ForwardSender encodes a single transport.Message frame for the 0x08 stream
// type and delivers it via the supplied dialer. PutObject/UploadPart can use
// StreamGroupForwardBody when a stream dialer is configured; the legacy
// single-message path keeps body bytes inside the FBS args.
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
type forwardStreamDialer func(peer string, payload []byte, body io.Reader) ([]byte, error)

// ForwardSender is the client side of the 0x08 wire. Stateless apart from the
// dialer + timeout — a single instance is shared across all coordinator calls.
type ForwardSender struct {
	dialer       forwardDialer
	streamDialer forwardStreamDialer
	timeout      time.Duration
	streamSlots  chan struct{}
}

// NewForwardSender constructs a sender with default 10 s per-call timeout.
func NewForwardSender(d forwardDialer) *ForwardSender {
	return &ForwardSender{
		dialer:      d,
		timeout:     10 * time.Second,
		streamSlots: make(chan struct{}, 8),
	}
}

// WithStreamDialer enables streamed body forwarding for PutObject and
// UploadPart. The metadata payload is sent as the first frame; body is copied
// behind it on the same QUIC stream.
func (s *ForwardSender) WithStreamDialer(d forwardStreamDialer) *ForwardSender {
	s.streamDialer = d
	return s
}

// WithMaxForwardStreams overrides the concurrent streamed-body forward limit.
func (s *ForwardSender) WithMaxForwardStreams(n int) *ForwardSender {
	if n < 1 {
		n = 1
	}
	s.streamSlots = make(chan struct{}, n)
	return s
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
	gidLen := int(binary.BigEndian.Uint32(buf[0:4]))
	if gidLen > 256 || len(buf) < 4+gidLen+1+4 {
		err = ErrShortHeader
		return
	}
	groupID = string(buf[4 : 4+gidLen])
	op = raftpb.ForwardOp(buf[4+gidLen])
	argsStart := 4 + gidLen + 1
	argsLen := int(binary.BigEndian.Uint32(buf[argsStart : argsStart+4]))
	if len(buf) < argsStart+4+argsLen {
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
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
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

// ResolveLeaderPeers probes the candidate list with a safe HeadObject request
// and returns a peer order with the current leader first when it can be found.
// Any non-NotLeader reply means the contacted peer passed the receiver's leader
// gate; the object may still be missing, which is fine for PutObject preflight.
func (s *ForwardSender) ResolveLeaderPeers(ctx context.Context, peers []string, groupID, bucket, key string) []string {
	if s == nil || s.dialer == nil || len(peers) == 0 {
		return peers
	}
	payload := encodeForwardPayload(groupID, raftpb.ForwardOpHeadObject, buildHeadObjectArgs(bucket, key))
	for _, peer := range peers {
		if err := ctx.Err(); err != nil {
			return peers
		}
		reply, err := s.dialer(peer, payload)
		if err != nil {
			continue
		}
		if isNotLeaderReply(reply) {
			if hint := extractLeaderHint(reply); hint != "" {
				return preferForwardPeer(peers, hint)
			}
			continue
		}
		return preferForwardPeer(peers, peer)
	}
	return peers
}

func preferForwardPeer(peers []string, first string) []string {
	if first == "" {
		return peers
	}
	out := make([]string, 0, len(peers)+1)
	out = append(out, first)
	for _, peer := range peers {
		if peer != first {
			out = append(out, peer)
		}
	}
	return out
}

// SendStream delivers a forward call whose body is streamed after the metadata
// frame. Retry is supported when body implements io.Seeker; otherwise the first
// failed stream attempt returns because the body may have been consumed.
func (s *ForwardSender) SendStream(
	ctx context.Context, peers []string, groupID string,
	op raftpb.ForwardOp, fbsArgs []byte, body io.Reader,
) ([]byte, error) {
	if s.streamDialer == nil {
		return nil, ErrNoReachablePeer
	}
	select {
	case s.streamSlots <- struct{}{}:
		defer func() { <-s.streamSlots }()
	default:
		return nil, storage.ErrForwardBackpressure
	}

	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}

	payload := encodeForwardPayload(groupID, op, fbsArgs)
	var redirected bool
	var lastDialErr error
	for _, peer := range peers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := rewindForwardBody(body); err != nil {
			return nil, err
		}
		reply, err := s.streamDialer(peer, payload, body)
		if err != nil {
			lastDialErr = err
			if !canRewindForwardBody(body) {
				return nil, err
			}
			continue
		}
		if isNotLeaderReply(reply) && !redirected {
			redirected = true
			if !canRewindForwardBody(body) {
				return reply, nil
			}
			if hint := extractLeaderHint(reply); hint != "" {
				if err := rewindForwardBody(body); err != nil {
					return nil, err
				}
				r2, err2 := s.streamDialer(hint, payload, body)
				if err2 == nil {
					return r2, nil
				}
				if !canRewindForwardBody(body) {
					return reply, nil
				}
			}
		}
		return reply, nil
	}
	if lastDialErr != nil {
		return nil, fmt.Errorf("%w (last dial error: %v)", ErrNoReachablePeer, lastDialErr)
	}
	return nil, ErrNoReachablePeer
}

func canRewindForwardBody(r io.Reader) bool {
	_, ok := r.(io.Seeker)
	return ok
}

func rewindForwardBody(r io.Reader) error {
	if s, ok := r.(io.Seeker); ok {
		_, err := s.Seek(0, io.SeekStart)
		return err
	}
	return nil
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
