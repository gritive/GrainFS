// Package cluster: applied-index barrier RPC (§8b — PR-2a keystone).
//
// The leader uses this RPC to confirm every voter has applied raft entries up
// to a target index (typically the PreparePresentFlip commit index) BEFORE
// proposing BeginPresentFlip. By raft log order, a voter whose LastApplied
// >= target has applied all prior RegisterMember entries + Prepare — so its
// composer's accept-set contains every per-node SPKI. Only then is it safe
// for any node to flip its presented cert.
//
// Wire layout: magic-tagged binary, mirrors kek_lease_rpc.go framing style
// (no FlatBuffers — keeps the hot path lean and rolling-upgrade-detectable).
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Wire magic. The trailing byte is the rolling-upgrade guard: a new leader
// decoding an old response with the wrong guard byte fails the magic check
// → barrier refuses → flip refused until every node ships PR-2a.
var (
	appliedIndexReqMagic  = []byte("AIBREQ\x01")
	appliedIndexRespMagic = []byte("AIBREP\x02")
)

// AppliedIndexReq carries the index the leader is waiting on.
type AppliedIndexReq struct {
	TargetIndex uint64
}

// AppliedIndexResp carries one voter's currently-applied index + identity.
type AppliedIndexResp struct {
	NodeID      string
	LastApplied uint64
}

// encodeAppliedIndexReq: [magic 7B][target_index uint64 BE]
func encodeAppliedIndexReq(req AppliedIndexReq) []byte {
	out := make([]byte, 0, len(appliedIndexReqMagic)+8)
	out = append(out, appliedIndexReqMagic...)
	out = binary.BigEndian.AppendUint64(out, req.TargetIndex)
	return out
}

func decodeAppliedIndexReq(data []byte) (AppliedIndexReq, error) {
	mLen := len(appliedIndexReqMagic)
	if len(data) < mLen || string(data[:mLen]) != string(appliedIndexReqMagic) {
		return AppliedIndexReq{}, errors.New("applied_index_probe: bad request magic")
	}
	rest := data[mLen:]
	if len(rest) < 8 {
		return AppliedIndexReq{}, errors.New("applied_index_probe: request truncated")
	}
	return AppliedIndexReq{TargetIndex: binary.BigEndian.Uint64(rest)}, nil
}

// encodeAppliedIndexResp: [magic 7B][node_id_len uint16 BE][node_id][last_applied uint64 BE]
func encodeAppliedIndexResp(resp AppliedIndexResp) []byte {
	idLen := len(resp.NodeID)
	out := make([]byte, 0, len(appliedIndexRespMagic)+2+idLen+8)
	out = append(out, appliedIndexRespMagic...)
	out = binary.BigEndian.AppendUint16(out, uint16(idLen))
	out = append(out, resp.NodeID...)
	out = binary.BigEndian.AppendUint64(out, resp.LastApplied)
	return out
}

func decodeAppliedIndexResp(data []byte) (AppliedIndexResp, error) {
	mLen := len(appliedIndexRespMagic)
	if len(data) < mLen || string(data[:mLen]) != string(appliedIndexRespMagic) {
		return AppliedIndexResp{}, errors.New("applied_index_probe: bad/old response magic — upgrade all nodes")
	}
	rest := data[mLen:]
	if len(rest) < 2 {
		return AppliedIndexResp{}, errors.New("applied_index_probe: response truncated at id len")
	}
	idLen := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	if len(rest) < idLen+8 {
		return AppliedIndexResp{}, errors.New("applied_index_probe: response truncated at body")
	}
	id := string(rest[:idLen])
	rest = rest[idLen:]
	return AppliedIndexResp{NodeID: id, LastApplied: binary.BigEndian.Uint64(rest[:8])}, nil
}

// AppliedIndexDialer is the production-injected outbound. In production this
// wraps QUICTransport.Call with StreamAppliedIndexProbe; tests inject a fake.
type AppliedIndexDialer func(ctx context.Context, peer string, payload []byte) ([]byte, error)

// WaitVotersApplied blocks until every voter's LastApplied >= target, or the
// context expires. selfID short-circuits to the local LastAppliedFn. Returns
// an error naming the slowest voter on context cancel (fail-safe — the caller
// MUST NOT propose Begin on error).
//
// checkNodeID, if non-nil, is called with (voterAddr, resp.NodeID) for each
// remote response. Return a non-nil error to reject a response — use this to
// verify that the responder's identity matches the expected peer. Pass nil to
// skip verification (transport-level TLS auth alone suffices in most cases).
//
// pollInterval bounds re-request cadence per voter. 100ms is a sane default
// vs meta-raft heartbeat 150ms (well below).
func WaitVotersApplied(
	ctx context.Context,
	target uint64,
	voters []string,
	selfID string,
	localLastApplied func() uint64,
	dialer AppliedIndexDialer,
	checkNodeID func(voter, nodeID string) error,
	pollInterval time.Duration,
) error {
	if pollInterval <= 0 {
		pollInterval = 100 * time.Millisecond
	}
	pending := make(map[string]struct{}, len(voters))
	for _, v := range voters {
		pending[v] = struct{}{}
	}
	for len(pending) > 0 {
		for v := range pending {
			if ctx.Err() != nil {
				break
			}
			var la uint64
			var err error
			if v == selfID {
				la = localLastApplied()
			} else {
				resp, derr := dialer(ctx, v, encodeAppliedIndexReq(AppliedIndexReq{TargetIndex: target}))
				if derr != nil {
					err = derr
				} else {
					r, perr := decodeAppliedIndexResp(resp)
					if perr != nil {
						err = perr
					} else if checkNodeID != nil {
						if cerr := checkNodeID(v, r.NodeID); cerr != nil {
							// Identity mismatch: fail-fast, do not retry.
							return cerr
						} else {
							la = r.LastApplied
						}
					} else {
						la = r.LastApplied
					}
				}
			}
			if err == nil && la >= target {
				delete(pending, v)
				continue
			}
		}
		if len(pending) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			var names []string
			for v := range pending {
				names = append(names, v)
			}
			return fmt.Errorf("applied_index_probe: timeout waiting for voters [%s] to reach index %d: %w", strings.Join(names, ","), target, ctx.Err())
		case <-time.After(pollInterval):
		}
	}
	return nil
}

// HandleAppliedIndexProbe is the server-side handler. Returns the response
// payload (or an error) for a StreamAppliedIndexProbe inbound request.
//
// Wire it on QUICTransport at boot:
//
//	t.Handle(transport.StreamAppliedIndexProbe, func(req *Message) *Message {
//	    respPayload, err := HandleAppliedIndexProbe(req.Payload, selfID, mr.LastApplied)
//	    if err != nil { return nil }
//	    return &transport.Message{Type: transport.StreamAppliedIndexProbe, Payload: respPayload}
//	})
func HandleAppliedIndexProbe(reqPayload []byte, selfID string, localLastApplied func() uint64) ([]byte, error) {
	if _, err := decodeAppliedIndexReq(reqPayload); err != nil {
		return nil, err
	}
	return encodeAppliedIndexResp(AppliedIndexResp{NodeID: selfID, LastApplied: localLastApplied()}), nil
}
