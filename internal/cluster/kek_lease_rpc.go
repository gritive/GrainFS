// Package cluster: KEK in-flight lease snapshot probe RPC (Task 8).
//
// RPC mechanism: QUICTransport.Call / Handle with StreamKEKLeaseSnapshotProbe (0x19).
// The leader uses this to collect per-voter lease counts before proposing a
// MetaKEKPruneCmd. A version can only be pruned when every voter attests
// lease_count == 0 (no in-flight consumers hold a reference to K_old).
//
// Phase B has zero acquire sites — all voters return count=0 deterministically.
// Phase D wires real acquire sites (raft snapshot reader, InstallSnapshot
// receiver). The wire layout is established here so Task 9 (PruneCmd) can
// reference it without a protocol version bump.
//
// Wire layout: simple binary, magic-tagged, no FlatBuffers.
// Mirrors kek_diskspace_rpc.go framing style.
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// kekLeaseSnapshotReqMagic guards against misrouted payloads on StreamKEKLeaseSnapshotProbe.
var kekLeaseSnapshotReqMagic = []byte("KLSREQ\x01")

// kekLeaseSnapshotRespMagic guards against misrouted reply payloads.
var kekLeaseSnapshotRespMagic = []byte("KLSREP\x01")

// KEKLeaseSnapshotReq carries the KEK version to probe.
type KEKLeaseSnapshotReq struct {
	Version uint32
}

// KEKLeaseSnapshotResp carries the in-flight lease count for the requested
// version, the raft commit index observed at snapshot time, and the responding
// node's identity.
type KEKLeaseSnapshotResp struct {
	LeaseCount                uint64
	ObservedAtRaftCommitIndex uint64
	NodeID                    string
}

// kekLeaseSnapshotDialer abstracts the outbound transport.Call so tests can
// inject a fake without a real QUIC link. Production wires:
//
//	func(ctx, peer, payload) ([]byte, error) {
//	    resp, err := quic.Call(ctx, peer, &transport.Message{
//	        Type: transport.StreamKEKLeaseSnapshotProbe, Payload: payload})
//	    return resp.Payload, err
//	}
type kekLeaseSnapshotDialer func(ctx context.Context, peer string, payload []byte) ([]byte, error)

// encodeKEKLeaseSnapshotReq serialises a request.
//
// Layout:
//
//	[magic 7B "KLSREQ\x01"]
//	[version uint32 BE]
func encodeKEKLeaseSnapshotReq(req KEKLeaseSnapshotReq) []byte {
	out := make([]byte, 0, len(kekLeaseSnapshotReqMagic)+4)
	out = append(out, kekLeaseSnapshotReqMagic...)
	out = binary.BigEndian.AppendUint32(out, req.Version)
	return out
}

// decodeKEKLeaseSnapshotReq verifies the magic and decodes the request.
func decodeKEKLeaseSnapshotReq(data []byte) (KEKLeaseSnapshotReq, error) {
	magicLen := len(kekLeaseSnapshotReqMagic)
	if len(data) < magicLen || string(data[:magicLen]) != string(kekLeaseSnapshotReqMagic) {
		return KEKLeaseSnapshotReq{}, errors.New("kek_lease_snapshot_probe: bad request magic")
	}
	rest := data[magicLen:]
	if len(rest) < 4 {
		return KEKLeaseSnapshotReq{}, errors.New("kek_lease_snapshot_probe: request truncated")
	}
	return KEKLeaseSnapshotReq{Version: binary.BigEndian.Uint32(rest)}, nil
}

// encodeKEKLeaseSnapshotResp serialises a response.
//
// Layout:
//
//	[magic 7B "KLSREP\x01"]
//	[lease_count uint64 BE]
//	[observed_at_raft_commit_index uint64 BE]
//	[node_id_len uint16 BE][node_id bytes]
func encodeKEKLeaseSnapshotResp(resp KEKLeaseSnapshotResp) []byte {
	idLen := len(resp.NodeID)
	out := make([]byte, 0, len(kekLeaseSnapshotRespMagic)+8+8+2+idLen)
	out = append(out, kekLeaseSnapshotRespMagic...)
	out = binary.BigEndian.AppendUint64(out, resp.LeaseCount)
	out = binary.BigEndian.AppendUint64(out, resp.ObservedAtRaftCommitIndex)
	out = binary.BigEndian.AppendUint16(out, uint16(idLen))
	out = append(out, resp.NodeID...)
	return out
}

// decodeKEKLeaseSnapshotResp deserialises a wire payload produced by encodeKEKLeaseSnapshotResp.
func decodeKEKLeaseSnapshotResp(data []byte) (KEKLeaseSnapshotResp, error) {
	magicLen := len(kekLeaseSnapshotRespMagic)
	if len(data) < magicLen || string(data[:magicLen]) != string(kekLeaseSnapshotRespMagic) {
		return KEKLeaseSnapshotResp{}, errors.New("kek_lease_snapshot_probe: bad response magic")
	}
	rest := data[magicLen:]
	if len(rest) < 8+8+2 {
		return KEKLeaseSnapshotResp{}, errors.New("kek_lease_snapshot_probe: response truncated at header")
	}
	leaseCount := binary.BigEndian.Uint64(rest[:8])
	rest = rest[8:]
	commitIndex := binary.BigEndian.Uint64(rest[:8])
	rest = rest[8:]
	idLen := int(binary.BigEndian.Uint16(rest))
	rest = rest[2:]
	if len(rest) < idLen {
		return KEKLeaseSnapshotResp{}, fmt.Errorf("kek_lease_snapshot_probe: response truncated at node_id (need %d, have %d)", idLen, len(rest))
	}
	return KEKLeaseSnapshotResp{
		LeaseCount:                leaseCount,
		ObservedAtRaftCommitIndex: commitIndex,
		NodeID:                    string(rest[:idLen]),
	}, nil
}

// KEKLeaseSnapshotHandler is the server-side handler for StreamKEKLeaseSnapshotProbe.
// Register it with: quicTransport.Handle(transport.StreamKEKLeaseSnapshotProbe, h.Handle)
type KEKLeaseSnapshotHandler struct {
	nodeID        string
	tracker       *encrypt.KEKLeaseTracker
	commitIndexFn func() uint64 // returns the node's current raft applied/commit index
}

// NewKEKLeaseSnapshotHandler constructs a handler. commitIndexFn is called at
// handle time to snapshot the raft index alongside the lease count. Production
// wires metaRaft.lastApplied.Load (or equivalent); tests inject a fixed value.
func NewKEKLeaseSnapshotHandler(nodeID string, tracker *encrypt.KEKLeaseTracker, commitIndexFn func() uint64) *KEKLeaseSnapshotHandler {
	return &KEKLeaseSnapshotHandler{
		nodeID:        nodeID,
		tracker:       tracker,
		commitIndexFn: commitIndexFn,
	}
}

// Handle processes a StreamKEKLeaseSnapshotProbe request and returns a response.
func (h *KEKLeaseSnapshotHandler) Handle(req *transport.Message) *transport.Message {
	decoded, err := decodeKEKLeaseSnapshotReq(req.Payload)
	if err != nil {
		return transport.NewErrorResponse(req, transport.StatusError,
			fmt.Errorf("kek_lease_snapshot_probe: decode request: %w", err))
	}
	resp := KEKLeaseSnapshotResp{
		LeaseCount:                h.tracker.Count(decoded.Version),
		ObservedAtRaftCommitIndex: h.commitIndexFn(),
		NodeID:                    h.nodeID,
	}
	return transport.NewResponse(req, encodeKEKLeaseSnapshotResp(resp))
}

// GetKEKLeaseSnapshot sends a probe to a peer and returns its response.
func GetKEKLeaseSnapshot(ctx context.Context, peer string, version uint32, dialer kekLeaseSnapshotDialer) (KEKLeaseSnapshotResp, error) {
	reqBytes := encodeKEKLeaseSnapshotReq(KEKLeaseSnapshotReq{Version: version})
	respBytes, err := dialer(ctx, peer, reqBytes)
	if err != nil {
		return KEKLeaseSnapshotResp{}, fmt.Errorf("GetKEKLeaseSnapshot: transport: %w", err)
	}
	resp, err := decodeKEKLeaseSnapshotResp(respBytes)
	if err != nil {
		return KEKLeaseSnapshotResp{}, fmt.Errorf("GetKEKLeaseSnapshot: decode: %w", err)
	}
	return resp, nil
}
