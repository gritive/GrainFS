// Package cluster: KEK keystore-directory disk-space probe RPC (Task 5).
//
// RPC mechanism: QUICTransport.Call / Handle with StreamKEKDiskSpaceProbe (0x18).
// The leader uses this to verify every voter has at least MinKeystoreFreeBytes
// free in its keystore directory before proposing a MetaKEKRotateCmd. A node
// that runs out of disk between persist of K_new and the snapshot-atomic
// install would halt its apply loop (ErrFSMKEKFatal) and fork the cluster —
// we prefer to reject the proposal upstream.
//
// Wire layout: simple binary, magic-tagged, no FlatBuffers (no FBS schema for
// this single RPC). Mirrors capability_direct_rpc.go's framing style.
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"

	"github.com/gritive/GrainFS/internal/transport"
)

// kekDiskSpaceReqMagic guards against misrouted payloads on StreamKEKDiskSpaceProbe.
var kekDiskSpaceReqMagic = []byte("KDSREQ\x01")

// kekDiskSpaceRespMagic guards against misrouted reply payloads.
var kekDiskSpaceRespMagic = []byte("KDSREP\x01")

// KEKDiskSpaceReq is the (empty) request payload. The target keystore path is
// determined by the receiving server — the leader does not (and must not)
// inject a path.
type KEKDiskSpaceReq struct{}

// KEKDiskSpaceResp carries the free-bytes report for the responding node's
// keystore directory.
type KEKDiskSpaceResp struct {
	FreeBytes    uint64
	KeystorePath string
	NodeID       string
}

// kekDiskSpaceProbeDialer abstracts the outbound transport.Call so tests can
// inject a fake without a real QUIC link. Production wires:
//
//	func(ctx, peer, payload) ([]byte, error) {
//	    resp, err := quic.Call(ctx, peer, &transport.Message{
//	        Type: transport.StreamKEKDiskSpaceProbe, Payload: payload})
//	    return resp.Payload, err
//	}
type kekDiskSpaceProbeDialer func(ctx context.Context, peer string, payload []byte) ([]byte, error)

// encodeKEKDiskSpaceReq serialises an empty request.
func encodeKEKDiskSpaceReq(_ KEKDiskSpaceReq) []byte {
	return append([]byte(nil), kekDiskSpaceReqMagic...)
}

// decodeKEKDiskSpaceReq verifies the magic and returns a zero request value.
func decodeKEKDiskSpaceReq(data []byte) (KEKDiskSpaceReq, error) {
	if len(data) < len(kekDiskSpaceReqMagic) || string(data[:len(kekDiskSpaceReqMagic)]) != string(kekDiskSpaceReqMagic) {
		return KEKDiskSpaceReq{}, errors.New("kek_diskspace_probe: bad request magic")
	}
	return KEKDiskSpaceReq{}, nil
}

// encodeKEKDiskSpaceResp serialises a response.
//
// Layout:
//
//	[magic 7B "KDSREP\x01"]
//	[free_bytes uint64 BE]
//	[path_len uint16 BE][path bytes]
//	[node_id_len uint16 BE][node_id bytes]
func encodeKEKDiskSpaceResp(resp KEKDiskSpaceResp) []byte {
	pathLen := len(resp.KeystorePath)
	idLen := len(resp.NodeID)
	out := make([]byte, 0, len(kekDiskSpaceRespMagic)+8+2+pathLen+2+idLen)
	out = append(out, kekDiskSpaceRespMagic...)
	out = binary.BigEndian.AppendUint64(out, resp.FreeBytes)
	out = binary.BigEndian.AppendUint16(out, uint16(pathLen))
	out = append(out, resp.KeystorePath...)
	out = binary.BigEndian.AppendUint16(out, uint16(idLen))
	out = append(out, resp.NodeID...)
	return out
}

// decodeKEKDiskSpaceResp deserialises a wire payload produced by encodeKEKDiskSpaceResp.
func decodeKEKDiskSpaceResp(data []byte) (KEKDiskSpaceResp, error) {
	magicLen := len(kekDiskSpaceRespMagic)
	if len(data) < magicLen || string(data[:magicLen]) != string(kekDiskSpaceRespMagic) {
		return KEKDiskSpaceResp{}, errors.New("kek_diskspace_probe: bad response magic")
	}
	rest := data[magicLen:]
	if len(rest) < 8+2 {
		return KEKDiskSpaceResp{}, errors.New("kek_diskspace_probe: response truncated at header")
	}
	free := binary.BigEndian.Uint64(rest[:8])
	rest = rest[8:]
	pathLen := int(binary.BigEndian.Uint16(rest))
	rest = rest[2:]
	if len(rest) < pathLen+2 {
		return KEKDiskSpaceResp{}, fmt.Errorf("kek_diskspace_probe: response truncated at path (need %d, have %d)", pathLen, len(rest))
	}
	path := string(rest[:pathLen])
	rest = rest[pathLen:]
	idLen := int(binary.BigEndian.Uint16(rest))
	rest = rest[2:]
	if len(rest) < idLen {
		return KEKDiskSpaceResp{}, fmt.Errorf("kek_diskspace_probe: response truncated at node_id (need %d, have %d)", idLen, len(rest))
	}
	return KEKDiskSpaceResp{
		FreeBytes:    free,
		KeystorePath: path,
		NodeID:       string(rest[:idLen]),
	}, nil
}

// diskSpaceFunc returns the number of free bytes on the filesystem backing dir.
// Injectable for unit tests that need to simulate a low-free-bytes condition
// without manipulating real disk state.
type diskSpaceFunc func(dir string) (uint64, error)

// statfsDiskSpace is the production diskSpaceFunc — uses syscall.Statfs.
// Returns Bavail * Bsize as a uint64. Bavail (available to non-root) is the
// conservative choice; Bfree (total free including root reserve) would over-
// report. Linux/darwin both have this struct in syscall.
func statfsDiskSpace(dir string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, fmt.Errorf("statfs %q: %w", dir, err)
	}
	// Bsize and Bavail are signed on some platforms; uint64 cast is safe for
	// non-negative values.
	return uint64(stat.Bavail) * uint64(stat.Bsize), nil
}

// KEKDiskSpaceHandler is the server-side handler for StreamKEKDiskSpaceProbe.
// Register it with: quicTransport.Handle(transport.StreamKEKDiskSpaceProbe, h.Handle)
type KEKDiskSpaceHandler struct {
	nodeID      string
	keystoreDir string
	diskSpaceFn diskSpaceFunc // nil → statfsDiskSpace
}

// NewKEKDiskSpaceHandler constructs a handler bound to the given keystore dir.
// nodeID is the responder's identity used by the leader to attribute reject
// reasons in error messages. diskSpaceFn may be nil — defaults to syscall.Statfs.
func NewKEKDiskSpaceHandler(nodeID, keystoreDir string, diskSpaceFn diskSpaceFunc) *KEKDiskSpaceHandler {
	return &KEKDiskSpaceHandler{
		nodeID:      nodeID,
		keystoreDir: keystoreDir,
		diskSpaceFn: diskSpaceFn,
	}
}

// Handle processes a StreamKEKDiskSpaceProbe request and returns a response.
func (h *KEKDiskSpaceHandler) Handle(req *transport.Message) *transport.Message {
	if _, err := decodeKEKDiskSpaceReq(req.Payload); err != nil {
		return transport.NewErrorResponse(req, transport.StatusError,
			fmt.Errorf("kek_diskspace_probe: decode request: %w", err))
	}
	fn := h.diskSpaceFn
	if fn == nil {
		fn = statfsDiskSpace
	}
	free, err := fn(h.keystoreDir)
	if err != nil {
		return transport.NewErrorResponse(req, transport.StatusError,
			fmt.Errorf("kek_diskspace_probe: %w", err))
	}
	resp := KEKDiskSpaceResp{
		FreeBytes:    free,
		KeystorePath: h.keystoreDir,
		NodeID:       h.nodeID,
	}
	return transport.NewResponse(req, encodeKEKDiskSpaceResp(resp))
}

// GetKEKDiskSpace sends a probe to a peer and returns its response. Verifies
// only the wire framing — the leader applies the MinKeystoreFreeBytes policy
// check itself (so the threshold can move without redeploying followers).
func GetKEKDiskSpace(ctx context.Context, peer string, dialer kekDiskSpaceProbeDialer) (KEKDiskSpaceResp, error) {
	reqBytes := encodeKEKDiskSpaceReq(KEKDiskSpaceReq{})
	respBytes, err := dialer(ctx, peer, reqBytes)
	if err != nil {
		return KEKDiskSpaceResp{}, fmt.Errorf("GetKEKDiskSpace: transport: %w", err)
	}
	resp, err := decodeKEKDiskSpaceResp(respBytes)
	if err != nil {
		return KEKDiskSpaceResp{}, fmt.Errorf("GetKEKDiskSpace: decode: %w", err)
	}
	return resp, nil
}
